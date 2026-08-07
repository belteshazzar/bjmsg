/*
 * push.c — the delivery engine: the broker as an HTTP client.
 *
 * Every subscription is a callback URL the broker POSTs batches to. The
 * response is the acknowledgement, which is what makes this a good deal:
 * delivery and ack are one exchange on one kept-alive connection, and
 * nothing in http11c had to change to allow it. A subscriber that cannot
 * keep up simply does not answer yet — the broker holds at most one
 * delivery per subscription, so backpressure needs no policy and there is
 * no queue to bound.
 *
 * The cost is that the broker must make outbound requests without
 * blocking the event loop it also serves on, so transfers run on
 * libcurl's multi interface and bjm_pusher_pump is interleaved with
 * http11c_poll. http11c does not expose its readiness fd, so pump asks
 * for a short poll timeout whenever a transfer is in flight — a local
 * syscall every couple of milliseconds, not a request on a wire. Exposing
 * that fd upstream would remove even the tick; see README.
 */
#include "bjmsg.h"

#include "binjson.h"

#include <curl/curl.h>

#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#define PUSH_DEFAULT_BATCH_BYTES 65536
#define PUSH_MAX_BATCH_BYTES     (4u * 1024 * 1024)

/*
 * A callback that is down is retried, doubling, forever. Giving up would
 * mean deciding on the subscriber's behalf that its messages no longer
 * matter — and there is no need to decide: the receipt is durable, so a
 * subscription that is unreachable for an hour is merely an hour behind.
 */
#define PUSH_BACKOFF_MS      500
#define PUSH_MAX_BACKOFF_MS  30000

#define PUSH_CONNECT_TIMEOUT_MS 5000
#define PUSH_TIMEOUT_MS         30000

/* Poll timeout while a delivery is in flight, and while nothing is. */
#define PUSH_TICK_MS 2
#define PUSH_IDLE_MS 1000

/* ---- one subscription -------------------------------------------------- */

typedef struct {
    char         consumer[BJM_CONSUMER_MAX + 1];
    bjm_push_sub cfg;

    CURL              *easy;
    struct curl_slist *hdrs;
    int   inflight;
    int   gone;                    /* deleted mid-delivery; reap on finish */
    char  url[BJM_CALLBACK_MAX + 1];

    /* The batch being delivered. The bytes are ours: bjm_read's buffer is
     * only valid until the next read of that subject, and a delivery
     * outlives many. */
    char     subject[BJM_SUBJECT_MAX + 1];
    uint64_t first, last, lag;
    uint8_t *body;
    size_t   body_len, body_cap;

    /* X-Bjmsg-Ack from the response: "I took this far", which lets a
     * subscriber accept part of a batch instead of all or nothing. */
    uint64_t ack_hint;
    int      has_ack_hint;
    char     resp[192];
    size_t   resp_len;

    /*
     * A worker delivery instead of a subscriber one: the jobs leased for
     * it, and X-Bjmsg-Done naming those the worker actually finished. A
     * receipt cannot express this — jobs complete out of order — so each
     * index is settled individually when the reply comes back.
     */
    uint64_t jobs[BJM_PUSH_JOBS_MAX];
    int      njobs;
    uint64_t done_list[BJM_PUSH_JOBS_MAX];
    int      ndone;
    int      has_done_list;

    uint64_t due_ms;               /* not before this: backoff */
    /*
     * A queue lease this worker's group holds is due to lapse then, so
     * there will be a job to hand out even if nobody publishes. Set by a
     * take that found nothing; cleared by the next attempt.
     */
    uint64_t wake_ms;
    int      failures;
    char     last_error[128];
    uint64_t delivered;            /* messages acknowledged, for reporting */

    int      rr;                   /* round-robin cursor over the subjects */
} psub;

typedef struct { char name[BJM_SUBJECT_MAX + 1]; } pname;

struct bjm_pusher {
    bjm_store  *st;
    CURLM      *multi;
    psub      **subs;              /* stable: the multi holds psub pointers */
    int         nsubs, cap;
    pname      *names;
    int         nnames, ncap;
    bj_builder *bld;
    int         work;              /* something may be deliverable */
    int         inflight;
    uint64_t    default_batch;
};

uint64_t bjm_now_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000 + (uint64_t)tv.tv_usec / 1000;
}

/* ---- the subject list -------------------------------------------------- */

/*
 * The engine keeps its own list of subject names rather than re-reading
 * the directory: every message that ever appears does so through
 * bjm_publish, which tells us (bjm_pusher_notify), so the list cannot go
 * stale. Only the initial load has to scan.
 */
static int names_add(bjm_pusher *p, const char *name) {
    for (int i = 0; i < p->nnames; i++)
        if (strcmp(p->names[i].name, name) == 0) return 0;

    if (p->nnames == p->ncap) {
        int cap = p->ncap ? p->ncap * 2 : 16;
        pname *q = realloc(p->names, (size_t)cap * sizeof *q);
        if (!q) return -1;
        p->names = q;
        p->ncap = cap;
    }
    snprintf(p->names[p->nnames].name, sizeof p->names[0].name, "%s", name);
    p->nnames++;
    return 1;
}

static void nm_string(void *ctx, const uint8_t *v, uint32_t len) {
    bjm_pusher *p = ctx;
    if (len > BJM_SUBJECT_MAX) return;
    char name[BJM_SUBJECT_MAX + 1];
    memcpy(name, v, len);
    name[len] = '\0';
    names_add(p, name);
}

static int names_load(bjm_pusher *p) {
    const uint8_t *out = NULL;
    size_t out_len = 0;
    int e = bjm_subjects(p->st, NULL, &out, &out_len);
    if (e) return e;
    bj_visitor v = bjm_visitor_noop(p);
    v.on_string = nm_string;
    return bj_decode(out, out_len, &v, NULL);
}

static int matches(const psub *s, const char *name) {
    return bjm_pattern_is(s->cfg.pattern)
         ? bjm_pattern_match(s->cfg.pattern, name)
         : strcmp(s->cfg.pattern, name) == 0;
}

/* ---- curl callbacks ---------------------------------------------------- */

static size_t on_body(char *data, size_t size, size_t nmemb, void *user) {
    psub *s = user;
    size_t n = size * nmemb;
    /* Only kept so a failure can quote what the subscriber said. */
    size_t room = sizeof s->resp - 1 - s->resp_len;
    size_t take = n < room ? n : room;
    memcpy(s->resp + s->resp_len, data, take);
    s->resp_len += take;
    s->resp[s->resp_len] = '\0';
    return n;
}

/* Case-insensitive "starts with", answering where the value begins. */
static size_t hdr_is(const char *data, size_t n, const char *name) {
    size_t wn = strlen(name);
    if (n <= wn) return 0;
    for (size_t i = 0; i < wn; i++) {
        char c = data[i];
        if (c >= 'A' && c <= 'Z') c = (char)(c - 'A' + 'a');
        if (c != name[i]) return 0;
    }
    return wn;
}

static size_t on_hdr(char *data, size_t size, size_t nmemb, void *user) {
    psub *s = user;
    size_t n = size * nmemb, at;
    if ((at = hdr_is(data, n, "x-bjmsg-ack:"))) {
        s->ack_hint = strtoull(data + at, NULL, 10);
        s->has_ack_hint = 1;
    } else if ((at = hdr_is(data, n, "x-bjmsg-done:"))) {
        /* A comma-separated list of the jobs the worker actually
         * finished. Absent means all of them. */
        s->has_done_list = 1;
        s->ndone = 0;
        const char *p = data + at, *end = data + n;
        while (p < end && s->ndone < BJM_PUSH_JOBS_MAX) {
            while (p < end && (*p < '0' || *p > '9')) p++;
            if (p >= end) break;
            uint64_t v = 0;
            while (p < end && *p >= '0' && *p <= '9')
                v = v * 10 + (uint64_t)(*p++ - '0');
            s->done_list[s->ndone++] = v;
        }
    }
    return n;
}

/* ---- lifecycle --------------------------------------------------------- */

static void psub_free(psub *s) {
    if (!s) return;
    if (s->hdrs) curl_slist_free_all(s->hdrs);
    if (s->easy) curl_easy_cleanup(s->easy);
    free(s->body);
    free(s);
}

static psub *psub_new(const char *consumer, const bjm_push_sub *cfg) {
    psub *s = calloc(1, sizeof *s);
    if (!s) return NULL;
    snprintf(s->consumer, sizeof s->consumer, "%s", consumer);
    s->cfg = *cfg;
    s->easy = curl_easy_init();
    if (!s->easy) { free(s); return NULL; }

    curl_easy_setopt(s->easy, CURLOPT_PRIVATE, s);
    curl_easy_setopt(s->easy, CURLOPT_WRITEFUNCTION, on_body);
    curl_easy_setopt(s->easy, CURLOPT_WRITEDATA, s);
    curl_easy_setopt(s->easy, CURLOPT_HEADERFUNCTION, on_hdr);
    curl_easy_setopt(s->easy, CURLOPT_HEADERDATA, s);
    curl_easy_setopt(s->easy, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
    curl_easy_setopt(s->easy, CURLOPT_CONNECTTIMEOUT_MS,
                     (long)PUSH_CONNECT_TIMEOUT_MS);
    curl_easy_setopt(s->easy, CURLOPT_TIMEOUT_MS, (long)PUSH_TIMEOUT_MS);
    /* Redirects would let a callback bounce the broker somewhere it never
     * agreed to send to, which is the one thing a URL from a client must
     * not be able to do. */
    curl_easy_setopt(s->easy, CURLOPT_FOLLOWLOCATION, 0L);
    curl_easy_setopt(s->easy, CURLOPT_POST, 1L);
    return s;
}

static int load_one(void *ctx, const char *consumer, const bjm_push_sub *cfg) {
    bjm_pusher *p = ctx;
    if (p->nsubs == p->cap) {
        int cap = p->cap ? p->cap * 2 : 8;
        psub **q = realloc(p->subs, (size_t)cap * sizeof *q);
        if (!q) return 1;
        p->subs = q;
        p->cap = cap;
    }
    psub *s = psub_new(consumer, cfg);
    if (!s) return 1;
    p->subs[p->nsubs++] = s;
    return 0;
}

bjm_pusher *bjm_pusher_new(bjm_store *st, uint64_t default_batch_bytes) {
    bjm_pusher *p = calloc(1, sizeof *p);
    if (!p) return NULL;
    p->st = st;
    p->default_batch = default_batch_bytes ? default_batch_bytes
                                           : PUSH_DEFAULT_BATCH_BYTES;
    p->multi = curl_multi_init();
    p->bld = bj_builder_new();
    if (!p->multi || !p->bld) { bjm_pusher_free(p); return NULL; }

    if (names_load(p) != BJ_OK) { bjm_pusher_free(p); return NULL; }
    if (bjm_push_each(st, load_one, p) != BJ_OK) { bjm_pusher_free(p); return NULL; }

    p->work = 1;
    return p;
}

void bjm_pusher_free(bjm_pusher *p) {
    if (!p) return;
    for (int i = 0; i < p->nsubs; i++) {
        if (p->subs[i]->inflight)
            curl_multi_remove_handle(p->multi, p->subs[i]->easy);
        psub_free(p->subs[i]);
    }
    free(p->subs);
    free(p->names);
    if (p->multi) curl_multi_cleanup(p->multi);
    bj_builder_free(p->bld);
    free(p);
}

int bjm_pusher_count(const bjm_pusher *p) { return p->nsubs; }

void bjm_pusher_notify(bjm_pusher *p, const char *subject) {
    if (!p) return;
    names_add(p, subject);
    p->work = 1;
}

static psub *find(bjm_pusher *p, const char *consumer) {
    for (int i = 0; i < p->nsubs; i++)
        if (!p->subs[i]->gone && strcmp(p->subs[i]->consumer, consumer) == 0)
            return p->subs[i];
    return NULL;
}

int bjm_pusher_add(bjm_pusher *p, const char *consumer,
                   const bjm_push_sub *cfg) {
    psub *s = find(p, consumer);
    if (s) {
        /*
         * Re-registering an existing consumer moves it, rather than
         * making a second subscription: the consumer name is the
         * identity, and its receipt goes on from where it was. A
         * delivery already in flight is left to finish against the old
         * callback — it is one batch, and it is already acked or not.
         */
        s->cfg = *cfg;
        p->work = 1;
        return 0;
    }
    if (load_one(p, consumer, cfg) != 0) return -1;
    p->work = 1;
    return 0;
}

void bjm_pusher_remove(bjm_pusher *p, const char *consumer) {
    for (int i = 0; i < p->nsubs; i++) {
        psub *s = p->subs[i];
        if (strcmp(s->consumer, consumer) != 0) continue;
        if (s->inflight) { s->gone = 1; return; }  /* reaped on completion */
        psub_free(s);
        p->subs[i] = p->subs[--p->nsubs];
        return;
    }
}

/* ---- seeding a new subscription ---------------------------------------- */

int bjm_pusher_seed(bjm_pusher *p, const char *consumer, const char *pattern,
                    int at_end, uint64_t from) {
    /*
     * Where a subscription starts is settled once, here, by writing the
     * receipts it would otherwise have. Delivery then never needs to know
     * about start positions at all, and — the reason it is done this way —
     * a subject that comes into existence *later* has no receipt, so it
     * starts at its own beginning. Anything else would silently drop
     * whatever was published between that subject's creation and the
     * broker noticing it.
     */
    psub probe;
    memset(&probe, 0, sizeof probe);
    snprintf(probe.cfg.pattern, sizeof probe.cfg.pattern, "%s", pattern);

    for (int i = 0; i < p->nnames; i++) {
        const char *name = p->names[i].name;
        if (!matches(&probe, name)) continue;

        int found = 0;
        uint64_t acked = 0;
        if (bjm_cursor_get(p->st, name, consumer, &found, &acked) != BJ_OK)
            continue;
        if (found) continue;   /* it has a place already; keep it */

        uint64_t at = 0;
        if (at_end) at = bjm_last_index(p->st, name);
        else if (from > 1) at = from - 1;
        bjm_cursor_set(p->st, name, consumer, at);
    }
    bjm_cursor_sync(p->st);
    p->work = 1;
    return 0;
}

/* ---- delivery ---------------------------------------------------------- */

static void hdr_add(struct curl_slist **l, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));

static void hdr_add(struct curl_slist **l, const char *fmt, ...) {
    char line[BJM_CALLBACK_MAX + 128];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(line, sizeof line, fmt, ap);
    va_end(ap);
    struct curl_slist *n = curl_slist_append(*l, line);
    if (n) *l = n;
}

/*
 * Hand the batch already in s->body to libcurl. Everything a subscriber
 * needs to place the batch travels in headers, so the body stays exactly
 * the bytes the log produced.
 */
static int send_batch(bjm_pusher *p, psub *s, int count) {
    snprintf(s->url, sizeof s->url, "%s", s->cfg.callback);

    if (s->hdrs) { curl_slist_free_all(s->hdrs); s->hdrs = NULL; }
    hdr_add(&s->hdrs, "Content-Type: %s", BJMSG_MEDIA_TYPE);
    hdr_add(&s->hdrs, "X-Bjmsg-Subject: %s", s->subject);
    hdr_add(&s->hdrs, "X-Bjmsg-Consumer: %s", s->consumer);
    hdr_add(&s->hdrs, "X-Bjmsg-Count: %d", count);
    hdr_add(&s->hdrs, "X-Bjmsg-First-Index: %llu",
            (unsigned long long)s->first);
    hdr_add(&s->hdrs, "X-Bjmsg-Last-Index: %llu",
            (unsigned long long)s->last);
    if (s->cfg.group[0])
        hdr_add(&s->hdrs, "X-Bjmsg-Group: %s", s->cfg.group);
    else
        /* How much is still waiting after this batch, so a subscriber can
         * tell "keep up" from "catch up" without asking. A queue has no
         * such number to give: what is waiting is whatever no other
         * worker has taken by the time this one asks. */
        hdr_add(&s->hdrs, "X-Bjmsg-Lag: %llu", (unsigned long long)s->lag);
    if (s->cfg.token[0])
        hdr_add(&s->hdrs, "Authorization: Bearer %s", s->cfg.token);
    /* libcurl adds Expect: 100-continue to larger POSTs and then waits a
     * second for a response http11c does not send. */
    hdr_add(&s->hdrs, "Expect:");

    s->resp_len = 0;
    s->resp[0] = '\0';
    s->has_ack_hint = 0;
    s->ack_hint = 0;
    s->has_done_list = 0;
    s->ndone = 0;

    curl_easy_setopt(s->easy, CURLOPT_URL, s->url);
    curl_easy_setopt(s->easy, CURLOPT_HTTPHEADER, s->hdrs);
    curl_easy_setopt(s->easy, CURLOPT_POSTFIELDS, (const char *)s->body);
    curl_easy_setopt(s->easy, CURLOPT_POSTFIELDSIZE_LARGE,
                     (curl_off_t)s->body_len);

    if (curl_multi_add_handle(p->multi, s->easy) != CURLM_OK) return -1;
    s->inflight = 1;
    p->inflight++;
    return 0;
}

/* Collect the `index` of each entry in a take response. */
typedef struct { char key[16]; psub *s; } job_grab;

static void jg_key(void *ctx, const uint8_t *k, uint32_t len) {
    job_grab *g = ctx;
    if (len >= sizeof g->key) len = sizeof g->key - 1;
    memcpy(g->key, k, len);
    g->key[len] = '\0';
}

static void jg_int(void *ctx, double v) {
    job_grab *g = ctx;
    if (strcmp(g->key, "index") != 0) return;
    if (g->s->njobs < BJM_PUSH_JOBS_MAX)
        g->s->jobs[g->s->njobs++] = (uint64_t)v;
}

/*
 * Lease jobs for a pushed worker. A queue subscription reads nothing and
 * acks nothing: what it delivers is the take, and what settles it is
 * done/fail per index once the worker replies.
 */
static int take_batch(bjm_pusher *p, psub *s, const char *name,
                      int *count, const uint8_t **out, size_t *out_len) {
    uint64_t want = s->cfg.max_jobs ? s->cfg.max_jobs : 1;
    if (want > BJM_PUSH_JOBS_MAX) want = BJM_PUSH_JOBS_MAX;

    /* UINT64_MAX: whatever lease the group is configured for. */
    if (bjm_take(p->st, name, s->cfg.group, (int)want, UINT64_MAX,
                 count, out, out_len) != BJ_OK)
        return -1;
    if (*count == 0) return 0;

    s->njobs = 0;
    job_grab g = { {0}, s };
    bj_visitor v = bjm_visitor_noop(&g);
    v.on_key = jg_key;
    v.on_int = jg_int;
    if (bj_decode(*out, *out_len, &v, NULL) != BJ_OK || s->njobs == 0)
        return -1;
    return 1;
}

/*
 * Find this subscription's next batch and start delivering it. Returns 1
 * if a delivery was started, 0 if there was nothing to send.
 */
static int start_delivery(bjm_pusher *p, psub *s) {
    int is_queue = s->cfg.group[0] != '\0';
    s->wake_ms = 0;

    for (int n = 0; n < p->nnames; n++) {
        int i = (s->rr + n) % p->nnames;
        const char *name = p->names[i].name;
        if (!matches(s, name)) continue;

        if (is_queue) {
            int count = 0;
            const uint8_t *out = NULL;
            size_t out_len = 0;
            int got = take_batch(p, s, name, &count, &out, &out_len);
            if (got <= 0) {
                /*
                 * Nothing available now, but a lease this group is
                 * holding may lapse and make its job available again.
                 * Note when, so the worker wakes then rather than
                 * re-asking on a timer.
                 */
                int pending = 0;
                uint64_t due = 0;
                if (bjm_queue_next_due(p->st, name, s->cfg.group,
                                       &pending, &due) == BJ_OK && pending &&
                    (!s->wake_ms || due < s->wake_ms))
                    s->wake_ms = due;
                continue;
            }

            if (out_len > s->body_cap) {
                uint8_t *q = realloc(s->body, out_len);
                if (!q) return 0;
                s->body = q;
                s->body_cap = out_len;
            }
            memcpy(s->body, out, out_len);
            s->body_len = out_len;

            snprintf(s->subject, sizeof s->subject, "%s", name);
            s->first = s->jobs[0];
            s->last  = s->jobs[s->njobs - 1];
            s->lag = 0;
            s->rr = (i + 1) % p->nnames;
            if (send_batch(p, s, count) != 0) return 0;
            return 1;
        }

        uint64_t base = 0, last = 0, bytes = 0;
        if (bjm_subject_info(p->st, name, &base, &last, &bytes) != BJ_OK)
            continue;

        int found = 0;
        uint64_t acked = 0;
        if (bjm_cursor_get(p->st, name, s->consumer, &found, &acked) != BJ_OK)
            continue;
        if (last <= acked) continue;               /* caught up here */

        /*
         * The receipt points below what the log still holds, so those
         * messages are gone — trimmed by a policy that was told to
         * outrank consumers, which is the only way this happens. Skip to
         * what survives and say how much was lost; silently resuming
         * would make the gap invisible.
         */
        if (acked < base) {
            fprintf(stderr, "bjmsg: push %s: %s: %llu message(s) were "
                            "trimmed before delivery; resuming at %llu\n",
                    s->consumer, name, (unsigned long long)(base - acked),
                    (unsigned long long)(base + 1));
            bjm_cursor_set(p->st, name, s->consumer, base);
            acked = base;
            if (last <= acked) continue;
        }

        uint64_t want = s->cfg.batch_bytes ? s->cfg.batch_bytes
                                           : p->default_batch;
        if (want > PUSH_MAX_BATCH_BYTES) want = PUSH_MAX_BATCH_BYTES;

        int count = 0;
        const uint8_t *out = NULL;
        size_t out_len = 0;
        uint64_t log_last = 0;
        if (bjm_read(p->st, name, acked + 1, (size_t)want,
                     &count, &out, &out_len, &log_last) != BJ_OK)
            continue;
        if (count == 0) continue;

        /* Copy: bjm_read's bytes belong to the subject's log and the next
         * read of it — quite possibly for another subscription, on the
         * next pump — would move them under us. */
        if (out_len > s->body_cap) {
            uint8_t *q = realloc(s->body, out_len);
            if (!q) return 0;
            s->body = q;
            s->body_cap = out_len;
        }
        memcpy(s->body, out, out_len);
        s->body_len = out_len;

        snprintf(s->subject, sizeof s->subject, "%s", name);
        s->first = acked + 1;
        /*
         * bjm_read stops on a byte budget, so the batch may end short of
         * the log. The last index actually in it is what may be acked,
         * and counting from `first` is exact because indexes within one
         * subject are contiguous.
         */
        s->last = acked + (uint64_t)count;
        s->lag  = log_last > s->last ? log_last - s->last : 0;
        s->rr = (i + 1) % p->nnames;   /* fairness: next subject goes first */

        if (send_batch(p, s, count) != 0) return 0;
        return 1;
    }
    return 0;
}

static void backoff(psub *s, uint64_t now);

static void backoff(psub *s, uint64_t now) {
    s->failures++;
    uint64_t wait = PUSH_BACKOFF_MS;
    for (int i = 1; i < s->failures && wait < PUSH_MAX_BACKOFF_MS; i++)
        wait *= 2;
    if (wait > PUSH_MAX_BACKOFF_MS) wait = PUSH_MAX_BACKOFF_MS;
    s->due_ms = now + wait;
}

/*
 * Settle the jobs a worker was leased, once it has replied.
 *
 * A transport failure deliberately settles nothing. The worker never got
 * the jobs — or never answered about them — which is exactly the case the
 * lease exists for: it expires and the jobs are handed out again, without
 * a `fail` claiming somebody tried and could not. An HTTP error *is* the
 * worker answering, and that is a real attempt.
 */
static void finish_jobs(bjm_pusher *p, psub *s, CURLcode rc, long status,
                        uint64_t now) {
    int answered = (rc == CURLE_OK);
    int ok = answered && status >= 200 && status < 300;

    if (!answered) {
        snprintf(s->last_error, sizeof s->last_error, "%s",
                 curl_easy_strerror(rc));
        if (s->failures == 0)
            fprintf(stderr, "bjmsg: work %s -> %s: %s (jobs left to their "
                            "leases; retrying)\n",
                    s->consumer, s->url, s->last_error);
        backoff(s, now);
        return;
    }

    for (int i = 0; i < s->njobs; i++) {
        uint64_t idx = s->jobs[i];
        int finished = ok;
        if (ok && s->has_done_list) {
            finished = 0;
            for (int j = 0; j < s->ndone; j++)
                if (s->done_list[j] == idx) { finished = 1; break; }
        }
        int found = 0;
        if (finished) {
            bjm_done(p->st, s->subject, s->cfg.group, idx, &found);
            s->delivered++;
        } else {
            /* UINT64_MAX: the group's own backoff decides when this job
             * becomes due again, which is where that policy belongs. */
            uint64_t retry_in = 0;
            bjm_fail(p->st, s->subject, s->cfg.group, idx, UINT64_MAX,
                     &found, &retry_in);
        }
    }
    s->njobs = 0;

    if (!ok) {
        snprintf(s->last_error, sizeof s->last_error, "HTTP %ld%s%.*s",
                 status, s->resp_len ? ": " : "", (int)s->resp_len, s->resp);
        /*
         * The jobs are already back in the queue with the group's
         * backoff, so this pause is about the *worker*: one that answers
         * with an error for everything should not be handed the whole
         * queue as fast as it can refuse it.
         */
        backoff(s, now);
        return;
    }

    if (s->failures) {
        fprintf(stderr, "bjmsg: work %s -> %s: taking jobs again\n",
                s->consumer, s->url);
        s->failures = 0;
        s->last_error[0] = '\0';
    }
    s->due_ms = 0;
    p->work = 1;
}

static void finish(bjm_pusher *p, psub *s, CURLcode rc, uint64_t now) {
    s->inflight = 0;
    p->inflight--;
    curl_multi_remove_handle(p->multi, s->easy);

    if (s->gone) {                 /* deleted while this was in flight */
        for (int i = 0; i < p->nsubs; i++)
            if (p->subs[i] == s) { p->subs[i] = p->subs[--p->nsubs]; break; }
        psub_free(s);
        return;
    }

    long status = 0;
    if (rc == CURLE_OK)
        curl_easy_getinfo(s->easy, CURLINFO_RESPONSE_CODE, &status);

    if (s->cfg.group[0]) { finish_jobs(p, s, rc, status, now); return; }

    if (rc != CURLE_OK || status < 200 || status >= 300) {
        if (rc != CURLE_OK)
            snprintf(s->last_error, sizeof s->last_error, "%s",
                     curl_easy_strerror(rc));
        else
            snprintf(s->last_error, sizeof s->last_error, "HTTP %ld%s%.*s",
                     status, s->resp_len ? ": " : "",
                     (int)s->resp_len, s->resp);
        backoff(s, now);
        /* Once per outage, not once per attempt. */
        if (s->failures == 1)
            fprintf(stderr, "bjmsg: push %s -> %s: %s (retrying)\n",
                    s->consumer, s->url, s->last_error);
        return;
    }

    /*
     * 2xx acknowledges the batch. X-Bjmsg-Ack narrows that to "I took up
     * to here", which is how a subscriber accepts part of one: the rest
     * is simply redelivered, since the receipt is the only record of what
     * was taken.
     */
    uint64_t ack = s->last;
    if (s->has_ack_hint) {
        ack = s->ack_hint;
        if (ack > s->last) ack = s->last;
    }

    if (ack >= s->first) {
        bjm_cursor_set(p->st, s->subject, s->consumer, ack);
        bjm_cursor_sync(p->st);
        s->delivered += ack - s->first + 1;
        s->due_ms = 0;              /* progress: no reason to wait */
    } else {
        /* A 2xx that took nothing is a soft refusal — "not now". Treat it
         * as a failure for pacing, or it becomes a hot loop. */
        snprintf(s->last_error, sizeof s->last_error,
                 "accepted 0 of %llu message(s)",
                 (unsigned long long)(s->last - s->first + 1));
        backoff(s, now);
        return;
    }

    if (s->failures) {
        fprintf(stderr, "bjmsg: push %s -> %s: delivering again\n",
                s->consumer, s->url);
        s->failures = 0;
        s->last_error[0] = '\0';
    }
    p->work = 1;                    /* there may be more where that came from */
}

int bjm_pusher_pump(bjm_pusher *p, uint64_t now) {
    if (!p) return PUSH_IDLE_MS;

    uint64_t next_due = 0;          /* 0 = nothing waiting on the clock */

    if (p->work) {
        int started = 0, busy = 0;
        for (int i = 0; i < p->nsubs; i++) {
            psub *s = p->subs[i];
            if (s->gone) continue;
            /*
             * Not asked, so not evidence of anything: a subscription
             * mid-delivery may well have more waiting behind this batch,
             * and one in backoff certainly does.
             */
            if (s->inflight) { busy = 1; continue; }
            if (s->due_ms > now) {
                busy = 1;
                if (!next_due || s->due_ms < next_due) next_due = s->due_ms;
                continue;
            }
            started += start_delivery(p, s);
            /* A worker with nothing to do but a lease about to lapse. */
            if (s->wake_ms) {
                busy = 1;
                if (!next_due || s->wake_ms < next_due) next_due = s->wake_ms;
            }
        }
        /*
         * A pass that asked every eligible subscription and found nothing
         * to send means there is nothing to send: stop scanning until a
         * publish or a completion says otherwise.
         */
        if (!started && !busy) p->work = 0;
    }

    int running = 0;
    if (p->inflight) {
        curl_multi_perform(p->multi, &running);

        CURLMsg *m;
        int left = 0;
        while ((m = curl_multi_info_read(p->multi, &left))) {
            if (m->msg != CURLMSG_DONE) continue;
            psub *s = NULL;
            curl_easy_getinfo(m->easy_handle, CURLINFO_PRIVATE, (char **)&s);
            if (s) finish(p, s, m->data.result, now);
        }
    }

    if (p->inflight) return PUSH_TICK_MS;
    if (p->work) return 0;
    if (next_due) {
        uint64_t wait = next_due - now;
        return wait < PUSH_IDLE_MS ? (int)wait : PUSH_IDLE_MS;
    }
    return PUSH_IDLE_MS;
}

/* ---- reporting --------------------------------------------------------- */

int bjm_pusher_list(bjm_pusher *p, const uint8_t **out, size_t *out_len) {
    bj_builder *b = p->bld;
    bj_builder_reset(b);
    bj_begin_array(b);
    for (int i = 0; i < p->nsubs; i++) {
        psub *s = p->subs[i];
        bj_begin_object(b);
        bj_put_key(b, (const uint8_t *)"consumer", 8);
        bj_put_string(b, (const uint8_t *)s->consumer,
                      (uint32_t)strlen(s->consumer));
        bj_put_key(b, (const uint8_t *)"pattern", 7);
        bj_put_string(b, (const uint8_t *)s->cfg.pattern,
                      (uint32_t)strlen(s->cfg.pattern));
        bj_put_key(b, (const uint8_t *)"callback", 8);
        bj_put_string(b, (const uint8_t *)s->cfg.callback,
                      (uint32_t)strlen(s->cfg.callback));
        bj_put_key(b, (const uint8_t *)"group", 5);
        bj_put_string(b, (const uint8_t *)s->cfg.group,
                      (uint32_t)strlen(s->cfg.group));
        bj_put_key(b, (const uint8_t *)"delivered", 9);
        bj_put_int(b, (int64_t)s->delivered);
        bj_put_key(b, (const uint8_t *)"inflight", 8);
        bj_put_bool(b, s->inflight);
        bj_put_key(b, (const uint8_t *)"failures", 8);
        bj_put_int(b, s->failures);
        bj_put_key(b, (const uint8_t *)"error", 5);
        bj_put_string(b, (const uint8_t *)s->last_error,
                      (uint32_t)strlen(s->last_error));
        bj_end_object(b);
    }
    bj_end_array(b);
    int e = bj_builder_error(b);
    if (e) return e;
    *out = bj_builder_data(b, out_len);
    return *out ? BJ_OK : BJ_ERR_STATE;
}
