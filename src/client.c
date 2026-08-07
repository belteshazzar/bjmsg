/*
 * client.c — `bjmsg pub` and `bjmsg sub`, over libcurl.
 *
 * Both reuse a single CURL easy handle for every request, which is what
 * makes the connection persistent: libcurl keeps the socket in the
 * handle's connection cache and the second request onward skips the
 * handshake entirely. That matters most for `sub`, which is a polling
 * loop — the poll costs one small request on an already-open socket.
 */
#include "bjmsg.h"

#include "binjson.h"

#include <curl/curl.h>

#include <ctype.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define DEFAULT_URL "http://127.0.0.1:8080"
#define DEFAULT_POLL_MS 200
#define DEFAULT_RETRY_MS 5000

/* Ctrl-C: stop whatever loop is running, promptly. Installed by
 * client_init, so a retry wait is interruptible in every subcommand. */
static volatile sig_atomic_t g_stop;
static void on_interrupt(int sig) { (void)sig; g_stop = 1; }

/* Interrupted by a signal, which is how Ctrl-C escapes a retry wait. */
static void sleep_ms(long ms) {
    struct timespec ts = { ms / 1000, (ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

/* ---- a growable byte buffer ------------------------------------------ */

typedef struct { uint8_t *p; size_t len, cap; } buf;

static int buf_append(buf *b, const void *data, size_t len) {
    if (b->len + len > b->cap) {
        size_t cap = b->cap ? b->cap : 256;
        while (cap < b->len + len) cap *= 2;
        uint8_t *p = realloc(b->p, cap);
        if (!p) return -1;
        b->p = p;
        b->cap = cap;
    }
    memcpy(b->p + b->len, data, len);
    b->len += len;
    return 0;
}

static void buf_free(buf *b) { free(b->p); b->p = NULL; b->len = b->cap = 0; }

static size_t on_write(char *data, size_t size, size_t nmemb, void *user) {
    size_t n = size * nmemb;
    return buf_append((buf *)user, data, n) == 0 ? n : 0;
}

/* ---- shared plumbing -------------------------------------------------- */

typedef struct {
    CURL *curl;
    struct curl_slist *headers;
    buf body;                 /* response body of the last request */
    uint64_t last_index;      /* X-Bjmsg-Last-Index of the last response */
    uint64_t acked;           /* X-Bjmsg-Acked: the broker's stored receipt */
    uint64_t skipped;         /* X-Bjmsg-Skipped: messages trimmed away    */
    long retry_ms;            /* 0 = give up on the first failure */
    int  waiting;             /* already reported that we are retrying */
} client;

/* Case-insensitive "does this header line start with `name`", answering
 * the offset of its value or 0 for no match. */
static size_t header_is(const char *data, size_t len, const char *name) {
    size_t n = strlen(name);
    if (len <= n) return 0;
    for (size_t i = 0; i < n; i++)
        if ((char)tolower((unsigned char)data[i]) != name[i]) return 0;
    return n;
}

/* Capture the subscribe cursor headers so a client can pace itself, find
 * the end of the log, or see its own receipt, without decoding the body. */
static size_t on_header(char *data, size_t size, size_t nmemb, void *user) {
    client *c = user;
    size_t n = size * nmemb, at;
    if ((at = header_is(data, n, "x-bjmsg-last-index:")))
        c->last_index = strtoull(data + at, NULL, 10);
    else if ((at = header_is(data, n, "x-bjmsg-acked:")))
        c->acked = strtoull(data + at, NULL, 10);
    else if ((at = header_is(data, n, "x-bjmsg-skipped:")))
        c->skipped = strtoull(data + at, NULL, 10);
    return n;
}

static int client_init(client *c, long retry_ms) {
    memset(c, 0, sizeof *c);
    c->retry_ms = retry_ms;
    c->curl = curl_easy_init();
    if (!c->curl) return -1;
    signal(SIGINT, on_interrupt);
    signal(SIGTERM, on_interrupt);
    curl_easy_setopt(c->curl, CURLOPT_WRITEFUNCTION, on_write);
    curl_easy_setopt(c->curl, CURLOPT_WRITEDATA, &c->body);
    curl_easy_setopt(c->curl, CURLOPT_HEADERFUNCTION, on_header);
    curl_easy_setopt(c->curl, CURLOPT_HEADERDATA, c);
    /* HTTP/1.1 with keep-alive is the whole protocol; don't let libcurl
     * negotiate its way to something else. */
    curl_easy_setopt(c->curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
    return 0;
}

static void client_free(client *c) {
    if (c->headers) curl_slist_free_all(c->headers);
    if (c->curl) curl_easy_cleanup(c->curl);
    buf_free(&c->body);
}

/*
 * Failures that prove the request never reached the broker. Retrying one
 * of these cannot repeat an effect, because there was no effect.
 */
static int never_arrived(CURLcode rc) {
    switch (rc) {
    case CURLE_COULDNT_CONNECT:
    case CURLE_COULDNT_RESOLVE_HOST:
    case CURLE_COULDNT_RESOLVE_PROXY:
        return 1;
    default:
        return 0;
    }
}

/*
 * Failures where the exchange broke after connecting. The broker may or
 * may not have acted on the request, so these are only safe to retry when
 * repeating the request is harmless — every GET here, and /ack, whose
 * receipt never moves backwards. A publish is not in that set: retrying a
 * POST /pub that already landed would append the message twice.
 */
static int broke_midway(CURLcode rc) {
    switch (rc) {
    case CURLE_SEND_ERROR:
    case CURLE_RECV_ERROR:
    case CURLE_GOT_NOTHING:
    case CURLE_PARTIAL_FILE:
    case CURLE_OPERATION_TIMEDOUT:
        return 1;
    default:
        return 0;
    }
}

/*
 * Perform the configured request, waiting for the broker to come back if
 * it is not there. Returns the HTTP status, or -1 when the request failed
 * for good (retries disabled, the failure is not retryable, or Ctrl-C).
 *
 * An HTTP response is never retried — a 404 or a 415 is the broker
 * answering, not a broken connection.
 */
static long client_perform_ex(client *c, const char *url, int idempotent) {
    for (;;) {
        if (g_stop) return -1;

        c->body.len = 0;
        curl_easy_setopt(c->curl, CURLOPT_URL, url);
        CURLcode rc = curl_easy_perform(c->curl);

        if (rc == CURLE_OK) {
            if (c->waiting) {
                fprintf(stderr, "bjmsg: reconnected.\n");
                c->waiting = 0;
            }
            long status = 0;
            curl_easy_getinfo(c->curl, CURLINFO_RESPONSE_CODE, &status);
            return status;
        }

        int retryable = never_arrived(rc) || (idempotent && broke_midway(rc));
        if (!retryable || c->retry_ms <= 0) {
            fprintf(stderr, "bjmsg: %s: %s\n", url, curl_easy_strerror(rc));
            if (retryable && c->retry_ms <= 0)
                fprintf(stderr, "bjmsg: (retries are off; --retry MS waits "
                                "and tries again)\n");
            return -1;
        }

        if (!c->waiting) {
            fprintf(stderr, "bjmsg: %s: %s — retrying every %ldms, "
                            "Ctrl-C to give up\n",
                    url, curl_easy_strerror(rc), c->retry_ms);
            c->waiting = 1;
        }
        sleep_ms(c->retry_ms);
    }
}

/* Most requests here are safe to repeat; publish says otherwise. */
static long client_perform(client *c, const char *url) {
    return client_perform_ex(c, url, 1);
}

/*
 * Parse a millisecond count that is allowed to be 0 ("off"). Returns 0 on
 * success, -1 if the text is not a non-negative integer.
 */
static int parse_ms(const char *v, long *out) {
    char *end;
    long n = strtol(v, &end, 10);
    if (end == v || *end || n < 0) return -1;
    *out = n;
    return 0;
}

/* Print a non-200 response, which the server sends as plain text. */
static void report_error(client *c, long status) {
    fprintf(stderr, "bjmsg: HTTP %ld: %.*s", status,
            (int)c->body.len, c->body.p ? (const char *)c->body.p : "");
    if (c->body.len == 0 || c->body.p[c->body.len - 1] != '\n')
        fputc('\n', stderr);
}

static const char *arg_value(int argc, char **argv, int *i, const char *name) {
    if (*i + 1 >= argc) {
        fprintf(stderr, "bjmsg: %s needs a value\n", name);
        return NULL;
    }
    return argv[++(*i)];
}

static int read_all(const char *path, buf *out) {
    FILE *f = (strcmp(path, "-") == 0) ? stdin : fopen(path, "rb");
    if (!f) { fprintf(stderr, "bjmsg: cannot open %s\n", path); return -1; }
    char chunk[8192];
    size_t n;
    int rc = 0;
    while ((n = fread(chunk, 1, sizeof chunk, f)) > 0)
        if (buf_append(out, chunk, n) != 0) { rc = -1; break; }
    if (f != stdin) fclose(f);
    return rc;
}

/* ---- pub -------------------------------------------------------------- */

static void pub_usage(void) {
    fprintf(stderr,
        "usage: bjmsg pub [--url URL] [--retry MS] [--id KEY | --auto-id]\n"
        "                 <subject> (<text> | --int N | --file PATH)\n"
        "\n"
        "  <text>       publish a binjson STRING\n"
        "  --int N      publish a binjson INT\n"
        "  --file PATH  publish PATH's bytes verbatim (already-encoded\n"
        "               binjson); PATH may be - for stdin\n"
        "  --header k=v add a header (repeatable). Headers travel with the\n"
        "               message and are opaque to the broker.\n"
        "  --id KEY     idempotency key. Republishing the same key inside\n"
        "               the broker's dedup window returns the original\n"
        "               index instead of appending again.\n"
        "  --auto-id    generate a key for this invocation, so a retry of\n"
        "               THIS publish cannot duplicate the message\n"
        "  --retry MS   wait MS between attempts when the broker cannot be\n"
        "               reached (default 5000; 0 disables retrying)\n"
        "\n"
        "Without an id a publish that breaks mid-flight is reported rather\n"
        "than retried: it may already be in the log.\n");
}

/*
 * An idempotency key for one invocation of `pub`. Only has to be unique
 * among publishes inside the broker's dedup window, so clock + pid is
 * enough — and it must NOT be derived from the payload, since two
 * identical messages are legitimately two messages.
 */
static void make_auto_id(char *out, size_t cap) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    snprintf(out, cap, "auto-%llx-%llx-%x",
             (unsigned long long)ts.tv_sec,
             (unsigned long long)ts.tv_nsec,
             (unsigned)getpid());
}

int bjm_cmd_pub(int argc, char **argv) {
    const char *url_base = DEFAULT_URL;
    const char *subject = NULL, *text = NULL, *file = NULL, *id = NULL;
    const char *hdr[16];
    int nhdr = 0;
    int have_int = 0, auto_id = 0;
    long long int_value = 0;
    long retry_ms = DEFAULT_RETRY_MS;

    for (int i = 0; i < argc; i++) {
        const char *a = argv[i];
        if (strcmp(a, "--url") == 0) {
            if (!(url_base = arg_value(argc, argv, &i, "--url"))) return 2;
        } else if (strcmp(a, "--retry") == 0) {
            const char *v = arg_value(argc, argv, &i, "--retry");
            if (!v || parse_ms(v, &retry_ms) != 0) {
                fprintf(stderr, "bjmsg: --retry needs a millisecond count "
                                "(0 to disable)\n");
                return 2;
            }
        } else if (strcmp(a, "--header") == 0 || strcmp(a, "-H") == 0) {
            const char *v = arg_value(argc, argv, &i, "--header");
            if (!v) return 2;
            if (!strchr(v, '=')) {
                fprintf(stderr, "bjmsg: --header wants name=value\n");
                return 2;
            }
            if (nhdr == (int)(sizeof hdr / sizeof hdr[0])) {
                fprintf(stderr, "bjmsg: too many headers\n");
                return 2;
            }
            hdr[nhdr++] = v;
        } else if (strcmp(a, "--id") == 0) {
            if (!(id = arg_value(argc, argv, &i, "--id"))) return 2;
        } else if (strcmp(a, "--auto-id") == 0) {
            auto_id = 1;
        } else if (strcmp(a, "--file") == 0) {
            if (!(file = arg_value(argc, argv, &i, "--file"))) return 2;
        } else if (strcmp(a, "--int") == 0) {
            const char *v = arg_value(argc, argv, &i, "--int");
            if (!v) return 2;
            int_value = strtoll(v, NULL, 10);
            have_int = 1;
        } else if (strcmp(a, "-h") == 0 || strcmp(a, "--help") == 0) {
            pub_usage();
            return 0;
        } else if (a[0] == '-' && a[1] != '\0') {
            fprintf(stderr, "bjmsg: unknown option %s\n", a);
            return 2;
        } else if (!subject) {
            subject = a;
        } else if (!text) {
            text = a;
        } else {
            fprintf(stderr, "bjmsg: unexpected argument %s\n", a);
            return 2;
        }
    }

    if (!subject || (!text && !file && !have_int)) { pub_usage(); return 2; }
    if (!bjm_subject_valid(subject)) {
        fprintf(stderr, "bjmsg: invalid subject '%s'\n", subject);
        return 2;
    }

    /* Build the payload: one complete binjson value, which is what the
     * broker checks for before it will accept a publish. */
    buf payload = {0};
    bj_builder *b = NULL;
    if (file) {
        if (read_all(file, &payload) != 0) { buf_free(&payload); return 1; }
        size_t size = 0;
        if (bj_value_size(payload.p, payload.len, 0, &size) != BJ_OK ||
            size != payload.len) {
            fprintf(stderr, "bjmsg: %s is not exactly one binjson value\n", file);
            buf_free(&payload);
            return 1;
        }
    } else {
        b = bj_builder_new();
        if (!b) return 1;
        /* With headers the message becomes [ {headers}, message ]; the
         * broker stores that under the envelope entry type. */
        if (nhdr) {
            bj_begin_array(b);
            bj_begin_object(b);
            for (int i = 0; i < nhdr; i++) {
                const char *eq = strchr(hdr[i], '=');
                bj_put_key(b, (const uint8_t *)hdr[i], (uint32_t)(eq - hdr[i]));
                bj_put_string(b, (const uint8_t *)(eq + 1),
                              (uint32_t)strlen(eq + 1));
            }
            bj_end_object(b);
        }
        if (have_int) bj_put_int(b, int_value);
        else bj_put_string(b, (const uint8_t *)text, (uint32_t)strlen(text));
        if (nhdr) bj_end_array(b);
        size_t len = 0;
        const uint8_t *d = bj_builder_data(b, &len);
        if (!d || buf_append(&payload, d, len) != 0) {
            bj_builder_free(b);
            buf_free(&payload);
            fprintf(stderr, "bjmsg: encode failed\n");
            return 1;
        }
        bj_builder_free(b);
    }

    client c;
    if (client_init(&c, retry_ms) != 0) { buf_free(&payload); return 1; }
    c.headers = curl_slist_append(NULL, "Content-Type: " BJMSG_MEDIA_TYPE);
    curl_easy_setopt(c.curl, CURLOPT_HTTPHEADER, c.headers);
    curl_easy_setopt(c.curl, CURLOPT_POST, 1L);
    curl_easy_setopt(c.curl, CURLOPT_POSTFIELDS, payload.p);
    curl_easy_setopt(c.curl, CURLOPT_POSTFIELDSIZE, (long)payload.len);

    char auto_buf[64];
    if (auto_id && !id) {
        make_auto_id(auto_buf, sizeof auto_buf);
        id = auto_buf;
    }

    char url[1024];
    int n = snprintf(url, sizeof url, "%s/pub/%s?", url_base, subject);
    if (id) n += snprintf(url + n, sizeof url - n, "id=%s&", id);
    if (nhdr) n += snprintf(url + n, sizeof url - n, "headers=1&");
    if (n > 0 && (size_t)n < sizeof url) url[n - 1] = '\0';   /* trailing & or ? */

    int rc = 1;
    /*
     * With an id the broker will collapse a repeat, so a publish that
     * broke mid-flight can be retried like any other request. Without
     * one it cannot: the message may already be in the log, and retrying
     * would append a second copy.
     */
    long status = client_perform_ex(&c, url, id != NULL);
    if (status == 200) {
        bjm_render(stdout, c.body.p, c.body.len);
        fputc('\n', stdout);
        rc = 0;
    } else if (status > 0) {
        report_error(&c, status);
    }

    client_free(&c);
    buf_free(&payload);
    return rc;
}

/* ---- sub -------------------------------------------------------------- */

/*
 * Pull index and payload out of the batch the broker returns: an ARRAY of
 * { index, term, type, payload }, where payload is BINARY holding the
 * message's own encoded bytes. Those inner bytes are opaque to this
 * decode, so the only BINARY the visitor ever sees is a payload — the
 * key check is belt-and-braces.
 */
typedef struct {
    char     key[16];
    long long index;
    uint64_t next;     /* cursor to request next time */
    int      count;
    const char *subject;   /* printed first when following a pattern */
} sub_scan;

static void s_key(void *ctx, const uint8_t *k, uint32_t len) {
    sub_scan *s = ctx;
    if (len >= sizeof s->key) len = sizeof s->key - 1;
    memcpy(s->key, k, len);
    s->key[len] = '\0';
}

static void s_int(void *ctx, double v) {
    sub_scan *s = ctx;
    if (strcmp(s->key, "index") == 0) s->index = (long long)v;
}

static void s_binary(void *ctx, const uint8_t *bytes, uint32_t len) {
    sub_scan *s = ctx;
    if (strcmp(s->key, "payload") != 0) return;
    if (s->subject) printf("%s\t", s->subject);
    printf("%lld\t", s->index);
    if (bjm_render(stdout, bytes, len) != BJ_OK) fputs("<undecodable>", stdout);
    fputc('\n', stdout);
    fflush(stdout);
    s->count++;
    if ((uint64_t)s->index >= s->next) s->next = (uint64_t)s->index + 1;
}

static void sub_usage(void) {
    fprintf(stderr,
        "usage: bjmsg sub [--url URL] <subject|pattern> [--consumer NAME]\n"
        "                 [--from N | --tail] [--follow] [--interval MS]\n"
        "                 [--retry MS] [--max BYTES]\n"
        "\n"
        "  <pattern>     'orders.*' follows one token, 'orders.>' follows\n"
        "                that and everything below. Each matched subject\n"
        "                keeps its own cursor, and output gains a subject\n"
        "                column. New matches are picked up as they appear.\n"
        "  --consumer N  durable subscription: the broker remembers how far\n"
        "                NAME has read, so rejoining delivers only what it\n"
        "                has not acknowledged. Overrides --from.\n"
        "  --from N      start at index N (default 1, the first message)\n"
        "  --tail        skip the backlog: start after the last message that\n"
        "                exists when the subscription starts. With\n"
        "                --consumer this only sets where a new consumer joins.\n"
        "  --follow      keep polling after catching up\n"
        "  --interval MS poll interval once caught up (default 200)\n"
        "  --retry MS    wait MS between attempts when the broker cannot be\n"
        "                reached, so a subscription survives a broker\n"
        "                restart (default 5000; 0 disables retrying)\n");
}

/*
 * One subject a subscription is following. A plain subject makes a list
 * of one; a pattern makes one per match, each with its own cursor —
 * which is the whole trick, since subjects have independent index
 * spaces and there is no order across them.
 */
typedef struct {
    char     subject[BJM_SUBJECT_MAX + 1];
    uint64_t from;         /* ephemeral cursor */
    uint64_t pending_ack;  /* durable: delivered but not yet acknowledged */
    uint64_t acked;        /* the broker's receipt, from X-Bjmsg-Acked */
    int      tail;         /* still owes its "where does this end" probe */
} sub_target;

#define POLL_OK    0
#define POLL_ABSENT -1     /* 404: no such subject (yet) */
#define POLL_FATAL -2

/* Poll one subject once, printing whatever came back. Returns the number
 * of messages, or POLL_ABSENT / POLL_FATAL. */
static int poll_target(client *c, const char *url_base, sub_target *t,
                       const char *consumer, uint64_t max_bytes,
                       int show_subject) {
    char url[1024];
    if (consumer) {
        int n = snprintf(url, sizeof url, "%s/sub/%s?consumer=%s&max=%llu",
                         url_base, t->subject, consumer,
                         (unsigned long long)max_bytes);
        /* Piggyback the receipt for the previous batch: the broker
         * persists it before choosing what to send next, so this costs
         * no extra round trip. */
        if (t->pending_ack && n > 0 && (size_t)n < sizeof url)
            n += snprintf(url + n, sizeof url - n, "&ack=%llu",
                          (unsigned long long)t->pending_ack);
        if (t->tail && n > 0 && (size_t)n < sizeof url)
            snprintf(url + n, sizeof url - n, "&start=last");
    } else {
        snprintf(url, sizeof url, "%s/sub/%s?from=%llu&max=%llu",
                 url_base, t->subject, (unsigned long long)t->from,
                 (unsigned long long)max_bytes);
    }

    long status = client_perform(c, url);
    if (g_stop) return POLL_FATAL;
    if (status < 0) return POLL_FATAL;
    if (status == 404) {
        /* Absent is not an error: with --tail there is no backlog to
         * skip, so the first message published is the first one wanted. */
        if (t->tail) { t->tail = 0; t->from = 1; }
        return POLL_ABSENT;
    }
    if (status != 200) {
        report_error(c, status);
        return POLL_FATAL;
    }

    if (c->skipped) {
        fprintf(stderr, "bjmsg: %s: %llu message(s) had already been "
                        "trimmed; starting at the oldest one kept\n",
                t->subject, (unsigned long long)c->skipped);
        c->skipped = 0;
    }

    sub_scan s = {0};
    s.next = t->from;
    s.subject = show_subject ? t->subject : NULL;
    bj_visitor v = bjm_visitor_noop(&s);
    v.on_int = s_int;
    v.on_binary = s_binary;
    v.on_key = s_key;
    if (bj_decode(c->body.p, c->body.len, &v, NULL) != BJ_OK) {
        fprintf(stderr, "bjmsg: malformed batch from broker\n");
        return POLL_FATAL;
    }

    if (t->tail && !consumer) {
        /* The probe told us where the log ends; start just past it. */
        t->from = c->last_index + 1;
        t->tail = 0;
        return 0;
    }
    t->tail = 0;
    if (s.count > 0) t->pending_ack = s.next - 1;
    t->acked = c->acked;
    t->from = s.next;
    return s.count;
}

/* Collect the strings of a binjson ARRAY. */
typedef struct { sub_target **list; int *n, *cap; uint64_t from; int tail; } resolver;

static void rv_string(void *ctx, const uint8_t *v, uint32_t len) {
    resolver *r = ctx;
    if (len > BJM_SUBJECT_MAX) return;
    char name[BJM_SUBJECT_MAX + 1];
    memcpy(name, v, len);
    name[len] = '\0';

    for (int i = 0; i < *r->n; i++)
        if (strcmp((*r->list)[i].subject, name) == 0) return;   /* known */

    if (*r->n == *r->cap) {
        int cap = *r->cap ? *r->cap * 2 : 8;
        sub_target *p = realloc(*r->list, (size_t)cap * sizeof *p);
        if (!p) return;
        *r->list = p;
        *r->cap = cap;
    }
    sub_target *t = &(*r->list)[(*r->n)++];
    memset(t, 0, sizeof *t);
    snprintf(t->subject, sizeof t->subject, "%s", name);
    t->from = r->from;
    t->tail = r->tail;
}

/*
 * Add any newly matching subjects to the list, keeping the cursors of
 * those already there. Subjects are created at any time, so a pattern
 * has to be re-asked periodically rather than resolved once.
 */
static int resolve_pattern(client *c, const char *url_base, const char *pattern,
                           sub_target **list, int *n, int *cap,
                           uint64_t from, int tail) {
    char *esc = curl_easy_escape(c->curl, pattern, 0);
    if (!esc) return -1;
    char url[1024];
    snprintf(url, sizeof url, "%s/subjects?pattern=%s", url_base, esc);
    curl_free(esc);

    curl_easy_setopt(c->curl, CURLOPT_HTTPGET, 1L);
    curl_easy_setopt(c->curl, CURLOPT_CUSTOMREQUEST, NULL);
    long status = client_perform(c, url);
    if (status != 200) return -1;

    resolver r = { list, n, cap, from, tail };
    bj_visitor v = bjm_visitor_noop(&r);
    v.on_string = rv_string;
    return bj_decode(c->body.p, c->body.len, &v, NULL) == BJ_OK ? 0 : -1;
}

/* How often a pattern is re-asked for newly created subjects. */
#define RESOLVE_EVERY_MS 3000

int bjm_cmd_sub(int argc, char **argv) {
    const char *url_base = DEFAULT_URL;
    const char *subject = NULL;
    const char *consumer = NULL;
    uint64_t from = 1, max_bytes = 65536;
    long interval = DEFAULT_POLL_MS;
    long retry_ms = DEFAULT_RETRY_MS;
    int follow = 0, tail = 0;

    for (int i = 0; i < argc; i++) {
        const char *a = argv[i];
        if (strcmp(a, "--consumer") == 0) {
            if (!(consumer = arg_value(argc, argv, &i, "--consumer"))) return 2;
        } else if (strcmp(a, "--url") == 0) {
            if (!(url_base = arg_value(argc, argv, &i, "--url"))) return 2;
        } else if (strcmp(a, "--from") == 0) {
            const char *v = arg_value(argc, argv, &i, "--from");
            if (!v) return 2;
            from = strtoull(v, NULL, 10);
        } else if (strcmp(a, "--max") == 0) {
            const char *v = arg_value(argc, argv, &i, "--max");
            if (!v) return 2;
            max_bytes = strtoull(v, NULL, 10);
        } else if (strcmp(a, "--interval") == 0) {
            const char *v = arg_value(argc, argv, &i, "--interval");
            if (!v) return 2;
            interval = strtol(v, NULL, 10);
        } else if (strcmp(a, "--retry") == 0) {
            const char *v = arg_value(argc, argv, &i, "--retry");
            if (!v || parse_ms(v, &retry_ms) != 0) {
                fprintf(stderr, "bjmsg: --retry needs a millisecond count "
                                "(0 to disable)\n");
                return 2;
            }
        } else if (strcmp(a, "--follow") == 0 || strcmp(a, "-f") == 0) {
            follow = 1;
        } else if (strcmp(a, "--tail") == 0) {
            tail = 1;
        } else if (strcmp(a, "-h") == 0 || strcmp(a, "--help") == 0) {
            sub_usage();
            return 0;
        } else if (a[0] == '-' && a[1] != '\0') {
            fprintf(stderr, "bjmsg: unknown option %s\n", a);
            return 2;
        } else if (!subject) {
            subject = a;
        } else {
            fprintf(stderr, "bjmsg: unexpected argument %s\n", a);
            return 2;
        }
    }

    if (!subject) { sub_usage(); return 2; }
    int is_pattern = bjm_pattern_is(subject);
    if (is_pattern ? !bjm_pattern_valid(subject) : !bjm_subject_valid(subject)) {
        fprintf(stderr, "bjmsg: invalid %s '%s'\n",
                is_pattern ? "pattern" : "subject", subject);
        return 2;
    }
    if (consumer && !bjm_consumer_valid(consumer)) {
        fprintf(stderr, "bjmsg: invalid consumer '%s'\n", consumer);
        return 2;
    }
    if (from == 0) from = 1;
    /*
     * --tail asks a cursor past the end of the log, which the broker
     * answers with an empty batch plus the real last index — so the
     * probe costs nothing, delivers nothing, and says where to start.
     * With --consumer the broker owns the cursor, so --tail becomes
     * ?start=last and only decides where a new consumer joins.
     */
    uint64_t start_from = (tail && !consumer) ? UINT64_MAX : from;

    client c;
    if (client_init(&c, retry_ms) != 0) return 1;
    curl_easy_setopt(c.curl, CURLOPT_HTTPGET, 1L);

    sub_target *targets = NULL;
    int ntargets = 0, cap = 0;
    if (!is_pattern) {
        targets = calloc(1, sizeof *targets);
        if (!targets) { client_free(&c); return 1; }
        snprintf(targets[0].subject, sizeof targets[0].subject, "%s", subject);
        targets[0].from = start_from;
        targets[0].tail = tail;
        ntargets = cap = 1;
    }

    int rc = 0;
    int first_resolve = 1;
    long since_resolve = RESOLVE_EVERY_MS;   /* resolve on the first pass */
    while (!g_stop) {
        if (is_pattern && since_resolve >= RESOLVE_EVERY_MS) {
            since_resolve = 0;
            /*
             * Only the first resolve honours --tail. A subject that
             * appears later did not exist when the subscription started,
             * so it has no backlog to skip — everything in it is new,
             * and starting at its end would silently drop whatever was
             * published between its creation and this resolve.
             */
            if (resolve_pattern(&c, url_base, subject, &targets,
                                &ntargets, &cap,
                                first_resolve ? start_from : 1,
                                first_resolve ? tail : 0) != 0) {
                rc = 1;
                break;
            }
            /* Nothing matched yet, so nothing has been skipped: the
             * next resolve is still the subscription starting. */
            if (ntargets > 0) first_resolve = 0;
            if (ntargets == 0) {
                if (!follow) break;
                sleep_ms(interval);
                since_resolve += interval;
                continue;
            }
        }

        int delivered = 0, absent = 0, fatal = 0;
        for (int i = 0; i < ntargets && !g_stop; i++) {
            int n = poll_target(&c, url_base, &targets[i], consumer,
                                max_bytes, is_pattern);
            if (n == POLL_FATAL) { fatal = 1; break; }
            if (n == POLL_ABSENT) { absent++; continue; }
            delivered += n;
        }
        if (fatal) { rc = g_stop ? rc : 1; break; }

        /* A named subject that does not exist is an error unless we are
         * waiting for it; under a pattern it just means "not yet". */
        if (!is_pattern && absent == ntargets && !follow && !tail) {
            report_error(&c, 404);
            rc = 1;
            break;
        }

        if (delivered == 0) {
            if (!follow) break;          /* caught up everywhere */
            sleep_ms(interval);
            since_resolve += interval;
        }
    }

    /*
     * The receipt for each subject's last batch has nowhere to piggyback
     * once the loop ends, so send it explicitly — unless a later poll
     * already carried it, which X-Bjmsg-Acked tells us for free. One
     * attempt each regardless of --retry: hanging on the way out is
     * worse than a redelivery that at-least-once already permits.
     */
    if (consumer && rc == 0) {
        g_stop = 0;
        c.retry_ms = 0;
        for (int i = 0; i < ntargets; i++) {
            sub_target *t = &targets[i];
            if (t->pending_ack <= t->acked) continue;
            char url[1024];
            snprintf(url, sizeof url, "%s/ack/%s?consumer=%s&index=%llu",
                     url_base, t->subject, consumer,
                     (unsigned long long)t->pending_ack);
            curl_easy_setopt(c.curl, CURLOPT_POST, 1L);
            curl_easy_setopt(c.curl, CURLOPT_POSTFIELDS, "");
            curl_easy_setopt(c.curl, CURLOPT_POSTFIELDSIZE, 0L);
            c.waiting = 0;
            if (client_perform(&c, url) != 200)
                fprintf(stderr, "bjmsg: final ack of %s index %llu failed"
                                " (that batch will be redelivered)\n",
                        t->subject, (unsigned long long)t->pending_ack);
        }
    }

    free(targets);
    client_free(&c);
    return rc;
}

/* ---- query commands --------------------------------------------------- */

/*
 * These all have the same shape: connect, ask one question, print the
 * answer, exit. Nothing is published, subscribed to, or served.
 */
/* Defined with the policy command, which is what they are for. */
static int parse_duration(const char *v, uint64_t *out);
static int parse_size(const char *v, uint64_t *out);

typedef struct {
    const char *url_base;
    const char *subject;    /* first positional, when the command takes one */
    const char *consumer;
    const char *group;
    const char *exec;
    const char *to;
    uint64_t    before, keep, index;
    uint64_t    max_age, max_messages, max_bytes;
    uint64_t    lease_ms;
    uint64_t    max_attempts;
    uint64_t    backoff_ms, max_backoff_ms, delay_ms;
    int         have_lease, have_attempts;
    int         have_backoff, have_max_backoff, have_delay;
    int         max;
    long        retry_ms, interval_ms;
    int         force, clear, del, ignore_consumers;
    const char *text;      /* second positional, for request */
    uint64_t    timeout_ms;
    int         raw, follow;
} query_opts;

/* Returns 0 on success, or an exit code (2) on a bad argument. */
static int query_parse(int argc, char **argv, query_opts *o, const char *usage) {
    memset(o, 0, sizeof *o);
    o->url_base = DEFAULT_URL;
    o->retry_ms = DEFAULT_RETRY_MS;

    for (int i = 0; i < argc; i++) {
        const char *a = argv[i];
        if (strcmp(a, "--url") == 0) {
            if (!(o->url_base = arg_value(argc, argv, &i, "--url"))) return 2;
        } else if (strcmp(a, "--consumer") == 0) {
            if (!(o->consumer = arg_value(argc, argv, &i, "--consumer"))) return 2;
        } else if (strcmp(a, "--before") == 0) {
            const char *v = arg_value(argc, argv, &i, "--before");
            if (!v) return 2;
            o->before = strtoull(v, NULL, 10);
        } else if (strcmp(a, "--keep") == 0) {
            const char *v = arg_value(argc, argv, &i, "--keep");
            if (!v) return 2;
            o->keep = strtoull(v, NULL, 10);
        } else if (strcmp(a, "--index") == 0) {
            const char *v = arg_value(argc, argv, &i, "--index");
            if (!v) return 2;
            o->index = strtoull(v, NULL, 10);
        } else if (strcmp(a, "--max-age") == 0) {
            const char *v = arg_value(argc, argv, &i, "--max-age");
            if (!v || parse_duration(v, &o->max_age) != 0) {
                fprintf(stderr, "bjmsg: --max-age wants a duration like "
                                "90, 30m, 12h, 7d, 2w\n");
                return 2;
            }
        } else if (strcmp(a, "--max-messages") == 0) {
            const char *v = arg_value(argc, argv, &i, "--max-messages");
            if (!v) return 2;
            o->max_messages = strtoull(v, NULL, 10);
        } else if (strcmp(a, "--max-bytes") == 0) {
            const char *v = arg_value(argc, argv, &i, "--max-bytes");
            if (!v || parse_size(v, &o->max_bytes) != 0) {
                fprintf(stderr, "bjmsg: --max-bytes wants a size like "
                                "4096, 512K, 100M, 2G\n");
                return 2;
            }
        } else if (strcmp(a, "--group") == 0) {
            if (!(o->group = arg_value(argc, argv, &i, "--group"))) return 2;
        } else if (strcmp(a, "--exec") == 0) {
            if (!(o->exec = arg_value(argc, argv, &i, "--exec"))) return 2;
        } else if (strcmp(a, "--to") == 0) {
            if (!(o->to = arg_value(argc, argv, &i, "--to"))) return 2;
        } else if (strcmp(a, "--reply-to") == 0) {
            if (!(o->to = arg_value(argc, argv, &i, "--reply-to"))) return 2;
        } else if (strcmp(a, "--timeout") == 0) {
            const char *v = arg_value(argc, argv, &i, "--timeout");
            uint64_t secs;
            if (!v || parse_duration(v, &secs) != 0) {
                fprintf(stderr, "bjmsg: --timeout wants a duration like 5s\n");
                return 2;
            }
            o->timeout_ms = secs * 1000;
        } else if (strcmp(a, "--raw") == 0) {
            o->raw = 1;
        } else if (strcmp(a, "--follow") == 0 || strcmp(a, "-f") == 0) {
            o->follow = 1;
        } else if (strcmp(a, "--max") == 0) {
            const char *v = arg_value(argc, argv, &i, "--max");
            if (!v) return 2;
            o->max = atoi(v);
        } else if (strcmp(a, "--lease") == 0) {
            const char *v = arg_value(argc, argv, &i, "--lease");
            uint64_t secs;
            if (!v || parse_duration(v, &secs) != 0) {
                fprintf(stderr, "bjmsg: --lease wants a duration like "
                                "30s, 5m, or 0 to disable leasing\n");
                return 2;
            }
            o->lease_ms = secs * 1000;
            o->have_lease = 1;
        } else if (strcmp(a, "--backoff") == 0) {
            const char *v = arg_value(argc, argv, &i, "--backoff");
            uint64_t secs;
            if (!v || parse_duration(v, &secs) != 0) {
                fprintf(stderr, "bjmsg: --backoff wants a duration like "
                                "1s, 30s, or 0 to retry instantly\n");
                return 2;
            }
            o->backoff_ms = secs * 1000;
            o->have_backoff = 1;
        } else if (strcmp(a, "--max-backoff") == 0) {
            const char *v = arg_value(argc, argv, &i, "--max-backoff");
            uint64_t secs;
            if (!v || parse_duration(v, &secs) != 0) {
                fprintf(stderr, "bjmsg: --max-backoff wants a duration\n");
                return 2;
            }
            o->max_backoff_ms = secs * 1000;
            o->have_max_backoff = 1;
        } else if (strcmp(a, "--delay") == 0) {
            const char *v = arg_value(argc, argv, &i, "--delay");
            uint64_t secs;
            if (!v || parse_duration(v, &secs) != 0) {
                fprintf(stderr, "bjmsg: --delay wants a duration like 30s\n");
                return 2;
            }
            o->delay_ms = secs * 1000;
            o->have_delay = 1;
        } else if (strcmp(a, "--max-attempts") == 0) {
            const char *v = arg_value(argc, argv, &i, "--max-attempts");
            if (!v) return 2;
            o->max_attempts = strtoull(v, NULL, 10);
            o->have_attempts = 1;
        } else if (strcmp(a, "--interval") == 0) {
            const char *v = arg_value(argc, argv, &i, "--interval");
            if (!v || parse_ms(v, &o->interval_ms) != 0) return 2;
        } else if (strcmp(a, "--ignore-consumers") == 0) {
            o->ignore_consumers = 1;
        } else if (strcmp(a, "--delete") == 0) {
            o->del = 1;
        } else if (strcmp(a, "--clear") == 0) {
            o->clear = 1;
        } else if (strcmp(a, "--force") == 0) {
            o->force = 1;
        } else if (strcmp(a, "--retry") == 0) {
            const char *v = arg_value(argc, argv, &i, "--retry");
            if (!v || parse_ms(v, &o->retry_ms) != 0) {
                fprintf(stderr, "bjmsg: --retry needs a millisecond count "
                                "(0 to disable)\n");
                return 2;
            }
        } else if (strcmp(a, "-h") == 0 || strcmp(a, "--help") == 0) {
            fputs(usage, stderr);
            return 1;
        } else if (a[0] == '-' && a[1] != '\0') {
            fprintf(stderr, "bjmsg: unknown option %s\n", a);
            return 2;
        } else if (!o->subject) {
            o->subject = a;
        } else if (!o->text) {
            o->text = a;
        } else {
            fprintf(stderr, "bjmsg: unexpected argument %s\n", a);
            return 2;
        }
    }
    return 0;
}

/* Issue one request and print the binjson answer. `method` is NULL for a
 * GET, "POST" or "DELETE" otherwise. */
static int query_run(const query_opts *o, const char *method, const char *url) {
    client c;
    if (client_init(&c, o->retry_ms) != 0) return 1;
    if (!method) {
        curl_easy_setopt(c.curl, CURLOPT_HTTPGET, 1L);
    } else {
        curl_easy_setopt(c.curl, CURLOPT_CUSTOMREQUEST, method);
        if (strcmp(method, "POST") == 0) {
            curl_easy_setopt(c.curl, CURLOPT_POSTFIELDS, "");
            curl_easy_setopt(c.curl, CURLOPT_POSTFIELDSIZE, 0L);
        }
    }

    int rc = 1;
    long status = client_perform(&c, url);
    if (status == 200) {
        bjm_render(stdout, c.body.p, c.body.len);
        fputc('\n', stdout);
        rc = 0;
    } else if (status > 0) {
        report_error(&c, status);
    }

    client_free(&c);
    return rc;
}

#define CONSUMERS_USAGE \
    "usage: bjmsg consumers [--url URL] [--retry MS] <subject>\n"

int bjm_cmd_consumers(int argc, char **argv) {
    query_opts o;
    int rc = query_parse(argc, argv, &o, CONSUMERS_USAGE);
    if (rc) return rc == 1 ? 0 : rc;
    if (!o.subject || !bjm_subject_valid(o.subject)) {
        fputs(CONSUMERS_USAGE, stderr);
        return 2;
    }

    char url[1024];
    snprintf(url, sizeof url, "%s/consumers/%s", o.url_base, o.subject);
    return query_run(&o, NULL, url);
}

#define UNSUB_USAGE \
    "usage: bjmsg unsubscribe [--url URL] <subject> --consumer NAME\n" \
    "\n" \
    "Forget a durable subscription. Its read receipt is deleted, so a\n" \
    "subscriber rejoining under that name starts fresh.\n"

int bjm_cmd_unsubscribe(int argc, char **argv) {
    query_opts o;
    int rc = query_parse(argc, argv, &o, UNSUB_USAGE);
    if (rc) return rc == 1 ? 0 : rc;
    if (!o.subject || !bjm_subject_valid(o.subject) ||
        !o.consumer || !bjm_consumer_valid(o.consumer)) {
        fputs(UNSUB_USAGE, stderr);
        return 2;
    }

    char url[1024];
    snprintf(url, sizeof url, "%s/consumers/%s?consumer=%s",
             o.url_base, o.subject, o.consumer);
    return query_run(&o, "DELETE", url);
}

/*
 * Durations as 90, 45s, 30m, 12h, 7d, 2w — plain digits are seconds.
 * Returns 0 on success.
 */
static int parse_duration(const char *v, uint64_t *out) {
    char *end;
    unsigned long long n = strtoull(v, &end, 10);
    if (end == v) return -1;
    uint64_t mult = 1;
    switch (*end) {
    case '\0':          mult = 1; break;
    case 's': case 'S': mult = 1; end++; break;
    case 'm': case 'M': mult = 60; end++; break;
    case 'h': case 'H': mult = 3600; end++; break;
    case 'd': case 'D': mult = 86400; end++; break;
    case 'w': case 'W': mult = 604800; end++; break;
    default: return -1;
    }
    if (*end) return -1;
    *out = (uint64_t)n * mult;
    return 0;
}

/* Sizes as 4096, 512K, 100M, 2G. Plain digits are bytes. */
static int parse_size(const char *v, uint64_t *out) {
    char *end;
    unsigned long long n = strtoull(v, &end, 10);
    if (end == v) return -1;
    uint64_t mult = 1;
    switch (*end) {
    case '\0':          mult = 1; break;
    case 'k': case 'K': mult = 1024ULL; end++; break;
    case 'm': case 'M': mult = 1024ULL * 1024; end++; break;
    case 'g': case 'G': mult = 1024ULL * 1024 * 1024; end++; break;
    case 't': case 'T': mult = 1024ULL * 1024 * 1024 * 1024; end++; break;
    default: return -1;
    }
    if (*end == 'b' || *end == 'B') end++;
    if (*end) return -1;
    *out = (uint64_t)n * mult;
    return 0;
}

#define POLICY_USAGE \
    "usage: bjmsg policy [--url URL] [<subject> [options]]\n" \
    "\n" \
    "  (no subject)          list every subject that has a policy\n" \
    "  <subject>             show that subject's policy\n" \
    "  --max-age D           discard messages older than D (90, 30m, 12h, 7d, 2w)\n" \
    "  --max-messages N      keep at most N messages\n" \
    "  --max-bytes S         keep the log under S (4096, 512K, 100M, 2G)\n" \
    "  --ignore-consumers    let retention discard unread messages; without\n" \
    "                        this a lagging subscription holds the log\n" \
    "  --clear               remove the policy entirely\n" \
    "\n" \
    "Several limits may be set at once: each implies a trim boundary and\n" \
    "the tightest one wins, so whichever is reached first takes effect.\n" \
    "The broker enforces policies on its own every few seconds.\n"

int bjm_cmd_policy(int argc, char **argv) {
    query_opts o;
    int rc = query_parse(argc, argv, &o, POLICY_USAGE);
    if (rc) return rc == 1 ? 0 : rc;
    if (o.subject && !bjm_subject_valid(o.subject)) {
        fputs(POLICY_USAGE, stderr);
        return 2;
    }

    char url[1024];
    if (!o.subject) {
        snprintf(url, sizeof url, "%s/policies", o.url_base);
        return query_run(&o, NULL, url);
    }
    if (o.clear) {
        snprintf(url, sizeof url, "%s/policy/%s", o.url_base, o.subject);
        return query_run(&o, "DELETE", url);
    }
    if (!o.max_age && !o.max_messages && !o.max_bytes && !o.ignore_consumers) {
        snprintf(url, sizeof url, "%s/policy/%s", o.url_base, o.subject);
        return query_run(&o, NULL, url);
    }

    int n = snprintf(url, sizeof url,
                     "%s/policy/%s?max_age_s=%llu&max_messages=%llu"
                     "&max_bytes=%llu&ignore_consumers=%d",
                     o.url_base, o.subject,
                     (unsigned long long)o.max_age,
                     (unsigned long long)o.max_messages,
                     (unsigned long long)o.max_bytes,
                     o.ignore_consumers);
    if (n < 0 || (size_t)n >= sizeof url) return 2;
    return query_run(&o, "PUT", url);
}

/* ---- queue groups ------------------------------------------------------ */

#define QUEUE_USAGE \
    "usage: bjmsg queue [--url URL] <subject> [--group G [--lease D]\n" \
    "                   [--delete]]\n" \
    "\n" \
    "  (no --group)   show every queue group on the subject\n" \
    "  --lease D      how long a taken job is held before it is handed to\n" \
    "                 somebody else (default 30s). --lease 0 turns leasing\n" \
    "                 off: jobs are taken and forgotten, so a worker that\n" \
    "                 dies loses its job.\n" \
    "  --max-attempts N  give up on a job after N deliveries and count it\n" \
    "                 dead (default 10). 0 retries forever, which lets one\n" \
    "                 always-failing job starve the queue.\n" \
    "  --backoff D    base wait before a failed job is offered again; it\n" \
    "                 doubles with each attempt (default 1s, 0 = instant)\n" \
    "  --max-backoff D  ceiling on that doubling (default 5m)\n" \
    "  --delete       forget the group\n"

int bjm_cmd_queue(int argc, char **argv) {
    query_opts o;
    int rc = query_parse(argc, argv, &o, QUEUE_USAGE);
    if (rc) return rc == 1 ? 0 : rc;
    if (!o.subject || !bjm_subject_valid(o.subject) ||
        (o.group && !bjm_group_valid(o.group))) {
        fputs(QUEUE_USAGE, stderr);
        return 2;
    }

    char url[1024];
    if (!o.group) {
        snprintf(url, sizeof url, "%s/queue/%s", o.url_base, o.subject);
        return query_run(&o, NULL, url);
    }
    if (o.del) {
        snprintf(url, sizeof url, "%s/queue/%s?group=%s",
                 o.url_base, o.subject, o.group);
        return query_run(&o, "DELETE", url);
    }
    snprintf(url, sizeof url,
             "%s/queue/%s?group=%s&lease_ms=%llu&max_attempts=%llu"
             "&backoff_ms=%llu&max_backoff_ms=%llu",
             o.url_base, o.subject, o.group,
             (unsigned long long)(o.have_lease ? o.lease_ms
                                               : BJM_LEASE_DEFAULT_MS),
             (unsigned long long)(o.have_attempts ? o.max_attempts
                                                  : BJM_MAX_ATTEMPTS_DEFAULT),
             (unsigned long long)(o.have_backoff ? o.backoff_ms
                                                 : BJM_BACKOFF_DEFAULT_MS),
             (unsigned long long)(o.have_max_backoff
                                      ? o.max_backoff_ms
                                      : BJM_MAX_BACKOFF_DEFAULT_MS));
    return query_run(&o, "PUT", url);
}

/* Build the /take/ URL shared by `take` and `work`. */
static void take_url(char *url, size_t cap, const query_opts *o, int max) {
    int n = snprintf(url, cap, "%s/take/%s?group=%s&max=%d",
                     o->url_base, o->subject, o->group, max);
    if (o->have_lease && n > 0 && (size_t)n < cap)
        snprintf(url + n, cap - n, "&lease=%llu",
                 (unsigned long long)o->lease_ms);
}

#define TAKE_USAGE \
    "usage: bjmsg take [--url URL] <subject> --group G [--max N] [--lease D]\n" \
    "\n" \
    "Lease jobs and print them as <index><tab><payload>. Each stays\n" \
    "leased until `bjmsg done` finishes it, `bjmsg fail` returns it, or\n" \
    "the lease expires and it goes back to the queue.\n"

int bjm_cmd_take(int argc, char **argv) {
    query_opts o;
    int rc = query_parse(argc, argv, &o, TAKE_USAGE);
    if (rc) return rc == 1 ? 0 : rc;
    if (!o.subject || !bjm_subject_valid(o.subject) ||
        !o.group || !bjm_group_valid(o.group)) {
        fputs(TAKE_USAGE, stderr);
        return 2;
    }

    char url[1024];
    take_url(url, sizeof url, &o, o.max > 0 ? o.max : 1);

    client c;
    if (client_init(&c, o.retry_ms) != 0) return 1;
    curl_easy_setopt(c.curl, CURLOPT_CUSTOMREQUEST, "POST");
    curl_easy_setopt(c.curl, CURLOPT_POSTFIELDS, "");
    curl_easy_setopt(c.curl, CURLOPT_POSTFIELDSIZE, 0L);

    rc = 1;
    long status = client_perform(&c, url);
    if (status == 200) {
        /* Same batch shape as a subscribe, so the same scanner prints it. */
        sub_scan s = {0};
        bj_visitor v = bjm_visitor_noop(&s);
        v.on_int = s_int;
        v.on_binary = s_binary;
        v.on_key = s_key;
        rc = bj_decode(c.body.p, c.body.len, &v, NULL) == BJ_OK ? 0 : 1;
    } else if (status > 0) {
        report_error(&c, status);
    }

    client_free(&c);
    return rc;
}

#define JOBEND_USAGE(verb) \
    "usage: bjmsg " verb " [--url URL] <subject> --group G --index N\n" \
    "                 [--delay D]\n" \
    "\n" \
    "--delay overrides the group's backoff for this one job.\n"

static int job_end(int argc, char **argv, const char *verb, const char *usage) {
    query_opts o;
    int rc = query_parse(argc, argv, &o, usage);
    if (rc) return rc == 1 ? 0 : rc;
    if (!o.subject || !bjm_subject_valid(o.subject) ||
        !o.group || !bjm_group_valid(o.group) || o.index == 0) {
        fputs(usage, stderr);
        return 2;
    }

    char url[1024];
    int n = snprintf(url, sizeof url, "%s/%s/%s?group=%s&index=%llu",
                     o.url_base, verb, o.subject, o.group,
                     (unsigned long long)o.index);
    /* Without --delay the broker applies the group's backoff policy. */
    if (o.have_delay && n > 0 && (size_t)n < sizeof url)
        snprintf(url + n, sizeof url - n, "&delay=%llu",
                 (unsigned long long)o.delay_ms);
    return query_run(&o, "POST", url);
}

int bjm_cmd_done(int argc, char **argv) {
    return job_end(argc, argv, "done", JOBEND_USAGE("done"));
}

int bjm_cmd_fail(int argc, char **argv) {
    return job_end(argc, argv, "fail", JOBEND_USAGE("fail"));
}

/* ---- the worker loop --------------------------------------------------- */

/*
 * One job's index and rendered payload, pulled out of a take response.
 * Only one job is taken at a time here: --exec runs them serially, and a
 * job held but not started is a job whose lease is burning down.
 */
typedef struct {
    char      key[16];
    long long index;
    long long attempts;
    char     *text;
    size_t    text_len;
    int       have;
} job;

static void job_key(void *ctx, const uint8_t *k, uint32_t len) {
    job *j = ctx;
    if (len >= sizeof j->key) len = sizeof j->key - 1;
    memcpy(j->key, k, len);
    j->key[len] = '\0';
}

static void job_int(void *ctx, double v) {
    job *j = ctx;
    if (j->have) return;
    if (strcmp(j->key, "index") == 0)         j->index = (long long)v;
    else if (strcmp(j->key, "attempts") == 0) j->attempts = (long long)v;
}

static void job_binary(void *ctx, const uint8_t *bytes, uint32_t len) {
    job *j = ctx;
    if (j->have || strcmp(j->key, "payload") != 0) return;
    FILE *f = open_memstream(&j->text, &j->text_len);
    if (!f) return;
    if (bjm_render(f, bytes, len) != BJ_OK) fputs("<undecodable>", f);
    fclose(f);
    j->have = 1;
}

#define WORK_USAGE \
    "usage: bjmsg work [--url URL] <subject> --group G --exec CMD\n" \
    "                  [--lease D] [--interval MS]\n" \
    "\n" \
    "Take one job at a time and run CMD for each. The payload is written\n" \
    "to CMD's stdin, and BJMSG_SUBJECT / BJMSG_GROUP / BJMSG_INDEX /\n" \
    "BJMSG_ATTEMPTS are set in its environment.\n" \
    "\n" \
    "CMD exiting 0 finishes the job; anything else returns it to the\n" \
    "queue immediately. A job whose worker dies is redelivered when its\n" \
    "lease expires, so CMD must tolerate running twice.\n"

int bjm_cmd_work(int argc, char **argv) {
    query_opts o;
    int rc = query_parse(argc, argv, &o, WORK_USAGE);
    if (rc) return rc == 1 ? 0 : rc;
    if (!o.subject || !bjm_subject_valid(o.subject) ||
        !o.group || !bjm_group_valid(o.group) || !o.exec) {
        fputs(WORK_USAGE, stderr);
        return 2;
    }
    long idle = o.interval_ms > 0 ? o.interval_ms : DEFAULT_POLL_MS;

    client c;
    if (client_init(&c, o.retry_ms) != 0) return 1;

    char url[1024];
    take_url(url, sizeof url, &o, 1);

    int failures = 0;
    while (!g_stop) {
        curl_easy_setopt(c.curl, CURLOPT_CUSTOMREQUEST, "POST");
        curl_easy_setopt(c.curl, CURLOPT_POSTFIELDS, "");
        curl_easy_setopt(c.curl, CURLOPT_POSTFIELDSIZE, 0L);

        long status = client_perform(&c, url);
        if (g_stop) break;
        if (status < 0) { rc = 1; break; }
        if (status != 200) {
            if (status == 404) { sleep_ms(idle); continue; }  /* no subject yet */
            report_error(&c, status);
            rc = 1;
            break;
        }

        job j = {0};
        bj_visitor v = bjm_visitor_noop(&j);
        v.on_key = job_key;
        v.on_int = job_int;
        v.on_binary = job_binary;
        if (bj_decode(c.body.p, c.body.len, &v, NULL) != BJ_OK) {
            fprintf(stderr, "bjmsg: malformed take response\n");
            rc = 1;
            break;
        }
        if (!j.have) { sleep_ms(idle); continue; }   /* queue is empty */

        char env[32];
        setenv("BJMSG_SUBJECT", o.subject, 1);
        setenv("BJMSG_GROUP", o.group, 1);
        snprintf(env, sizeof env, "%lld", j.index);
        setenv("BJMSG_INDEX", env, 1);
        /* >1 means this job was run before and its lease expired — the
         * signal a handler needs to decide whether to guard itself. */
        snprintf(env, sizeof env, "%lld", j.attempts);
        setenv("BJMSG_ATTEMPTS", env, 1);

        FILE *child = popen(o.exec, "w");
        int ok = 0;
        if (!child) {
            fprintf(stderr, "bjmsg: cannot run '%s'\n", o.exec);
        } else {
            fputs(j.text ? j.text : "", child);
            int st = pclose(child);
            ok = (st == 0);
        }
        free(j.text);

        /* Report the outcome on a second handle's worth of settings: the
         * same connection, a different URL. */
        char end[1024];
        snprintf(end, sizeof end, "%s/%s/%s?group=%s&index=%lld",
                 o.url_base, ok ? "done" : "fail", o.subject, o.group, j.index);
        curl_easy_setopt(c.curl, CURLOPT_CUSTOMREQUEST, "POST");
        curl_easy_setopt(c.curl, CURLOPT_POSTFIELDS, "");
        curl_easy_setopt(c.curl, CURLOPT_POSTFIELDSIZE, 0L);
        if (client_perform(&c, end) != 200 && !g_stop)
            fprintf(stderr, "bjmsg: could not report job %lld as %s\n",
                    j.index, ok ? "done" : "failed");

        printf("%lld\t%s\n", j.index, ok ? "done" : "failed");
        fflush(stdout);
        /* No pause here: the broker's backoff already withholds the job
         * this worker just failed, so there is nothing to spin on, and
         * sleeping would only delay the *other* jobs waiting behind it. */
        if (!ok) failures++;
    }

    client_free(&c);
    return rc ? rc : (failures ? 1 : 0);
}

/* ---- request / reply ---------------------------------------------------- */

/* Both defined with the pipeline machinery below, which is where they
 * carry the most weight; a replier runs a handler the same way. */
static void feed_for(const uint8_t *payload, size_t plen, int raw, buf *out);
static int run_filter(const char *cmd, const uint8_t *in, size_t in_len,
                      buf *out);

#define REPLY_SUBJECT_DEFAULT "_reply"
#define REPLY_GROUP_DEFAULT   "repliers"

/* Read one named header out of an encoded headers object. */
typedef struct {
    const char *want;
    char        key[64];
    char        value[BJM_SUBJECT_MAX + 1];
    int         got;
} hdr_get;

static void hg_key(void *ctx, const uint8_t *k, uint32_t len) {
    hdr_get *h = ctx;
    if (len >= sizeof h->key) len = sizeof h->key - 1;
    memcpy(h->key, k, len);
    h->key[len] = '\0';
}

static void hg_string(void *ctx, const uint8_t *v, uint32_t len) {
    hdr_get *h = ctx;
    if (h->got || strcmp(h->key, h->want) != 0) return;
    if (len >= sizeof h->value) len = sizeof h->value - 1;
    memcpy(h->value, v, len);
    h->value[len] = '\0';
    h->got = 1;
}

static int header_value(const uint8_t *headers, size_t len, const char *name,
                        char *out, size_t out_size) {
    hdr_get h;
    memset(&h, 0, sizeof h);
    h.want = name;
    bj_visitor v = bjm_visitor_noop(&h);
    v.on_key = hg_key;
    v.on_string = hg_string;
    if (bj_decode(headers, len, &v, NULL) != BJ_OK || !h.got) return 0;
    snprintf(out, out_size, "%s", h.value);
    return 1;
}

/* Publish `body` with { correlation: id } attached, to `subject`. */
static long publish_reply(client *c, const query_opts *o, const char *subject,
                          const char *correlation, const uint8_t *msg,
                          size_t msg_len) {
    bj_builder *b = bj_builder_new();
    if (!b) return -1;
    bj_begin_array(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"correlation", 11);
    bj_put_string(b, (const uint8_t *)correlation, (uint32_t)strlen(correlation));
    bj_end_object(b);
    bj_put_raw(b, msg, (uint32_t)msg_len);
    bj_end_array(b);

    size_t len = 0;
    const uint8_t *data = bj_builder_data(b, &len);
    if (!data) { bj_builder_free(b); return -1; }

    char url[1024];
    /* Keyed on the correlation id, so a redelivered request cannot
     * produce a second reply. */
    snprintf(url, sizeof url, "%s/pub/%s?headers=1&id=reply.%s",
             o->url_base, subject, correlation);
    if (!c->headers) {
        c->headers = curl_slist_append(NULL, "Content-Type: " BJMSG_MEDIA_TYPE);
        curl_easy_setopt(c->curl, CURLOPT_HTTPHEADER, c->headers);
    }
    curl_easy_setopt(c->curl, CURLOPT_CUSTOMREQUEST, NULL);
    curl_easy_setopt(c->curl, CURLOPT_POST, 1L);
    curl_easy_setopt(c->curl, CURLOPT_POSTFIELDS, data);
    curl_easy_setopt(c->curl, CURLOPT_POSTFIELDSIZE, (long)len);
    long status = client_perform(c, url);
    bj_builder_free(b);
    return status;
}

/*
 * Walks every message of a batch looking for one correlation id. Each
 * batch entry is a flat object, so on_object_end is one message — which
 * matters when several requesters share a reply subject and another's
 * reply arrives in the same batch as ours.
 */
typedef struct {
    char        key[16];
    long long   index, type;
    const uint8_t *payload;
    size_t      plen;
    const char *want;
    uint64_t    highest;   /* to advance the cursor past what we read */
    int         matched;
} corr_scan;

static void cs_key(void *ctx, const uint8_t *k, uint32_t len) {
    corr_scan *s = ctx;
    if (len >= sizeof s->key) len = sizeof s->key - 1;
    memcpy(s->key, k, len);
    s->key[len] = '\0';
}

static void cs_int(void *ctx, double v) {
    corr_scan *s = ctx;
    if (strcmp(s->key, "index") == 0)     s->index = (long long)v;
    else if (strcmp(s->key, "type") == 0) s->type = (long long)v;
}

/* Valid only for the duration of the decode, which is where it is used. */
static void cs_binary(void *ctx, const uint8_t *b, uint32_t len) {
    corr_scan *s = ctx;
    if (strcmp(s->key, "payload") != 0) return;
    s->payload = b;
    s->plen = len;
}

static void cs_object_end(void *ctx) {
    corr_scan *s = ctx;
    if ((uint64_t)s->index > s->highest) s->highest = (uint64_t)s->index;

    const uint8_t *h, *msg;
    size_t hlen, mlen;
    char got[96];
    if (!s->matched && s->type == BJM_ENTRY_ENVELOPE && s->payload &&
        bjm_envelope_split(s->payload, s->plen, &h, &hlen, &msg, &mlen) &&
        header_value(h, hlen, "correlation", got, sizeof got) &&
        strcmp(got, s->want) == 0) {
        bjm_render(stdout, msg, mlen);
        fputc('\n', stdout);
        s->matched = 1;
    }
    s->payload = NULL;
    s->plen = 0;
    s->type = 0;
}

/* A message from a subscribe batch: index, entry type, payload. */
typedef struct {
    char      key[16];
    long long index, type;
    uint8_t  *payload;
    size_t    plen;
    int       have;
} rr_msg;

static void rr_key(void *ctx, const uint8_t *k, uint32_t len) {
    rr_msg *m = ctx;
    if (len >= sizeof m->key) len = sizeof m->key - 1;
    memcpy(m->key, k, len);
    m->key[len] = '\0';
}

static void rr_int(void *ctx, double v) {
    rr_msg *m = ctx;
    if (m->have) return;
    if (strcmp(m->key, "index") == 0)     m->index = (long long)v;
    else if (strcmp(m->key, "type") == 0) m->type = (long long)v;
}

static void rr_binary(void *ctx, const uint8_t *b, uint32_t len) {
    rr_msg *m = ctx;
    if (m->have || strcmp(m->key, "payload") != 0) return;
    m->payload = malloc(len ? len : 1);
    if (!m->payload) return;
    memcpy(m->payload, b, len);
    m->plen = len;
    m->have = 1;
}

/* The subject's highest index, or 0. Used to start tailing replies from
 * the end BEFORE the request goes out, so a fast reply is not missed. */
static uint64_t subject_last_index(client *c, const query_opts *o,
                                   const char *subject) {
    char url[1024];
    snprintf(url, sizeof url, "%s/sub/%s?from=%llu",
             o->url_base, subject, (unsigned long long)UINT64_MAX);
    curl_easy_setopt(c->curl, CURLOPT_CUSTOMREQUEST, NULL);
    curl_easy_setopt(c->curl, CURLOPT_HTTPGET, 1L);
    c->last_index = 0;
    long status = client_perform(c, url);
    return status == 200 ? c->last_index : 0;
}

#define REQUEST_USAGE \
    "usage: bjmsg request [--url URL] <subject> <text> [--timeout D]\n" \
    "                     [--reply-to SUBJECT]\n" \
    "\n" \
    "Publish a request and wait for its reply. The request carries\n" \
    "reply_to and correlation headers; the reply is matched on the\n" \
    "correlation, so many requesters can share one reply subject.\n" \
    "\n" \
    "  --timeout D    how long to wait (default 5s); exits 1 on timeout\n" \
    "  --reply-to S   reply subject (default " REPLY_SUBJECT_DEFAULT ")\n"

int bjm_cmd_request(int argc, char **argv) {
    query_opts o;
    int rc = query_parse(argc, argv, &o, REQUEST_USAGE);
    if (rc) return rc == 1 ? 0 : rc;
    const char *text = o.text;
    const char *reply_to = o.to ? o.to : REPLY_SUBJECT_DEFAULT;
    if (!o.subject || !bjm_subject_valid(o.subject) || !text ||
        !bjm_subject_valid(reply_to)) {
        fputs(REQUEST_USAGE, stderr);
        return 2;
    }
    uint64_t timeout_ms = o.timeout_ms ? o.timeout_ms : 5000;
    long idle = o.interval_ms > 0 ? o.interval_ms : 100;

    char corr[64];
    make_auto_id(corr, sizeof corr);

    client c;
    if (client_init(&c, o.retry_ms) != 0) return 1;

    /* Where the reply subject ends now, before the request exists. */
    uint64_t from = subject_last_index(&c, &o, reply_to) + 1;

    bj_builder *b = bj_builder_new();
    if (!b) { client_free(&c); return 1; }
    bj_begin_array(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"reply_to", 8);
    bj_put_string(b, (const uint8_t *)reply_to, (uint32_t)strlen(reply_to));
    bj_put_key(b, (const uint8_t *)"correlation", 11);
    bj_put_string(b, (const uint8_t *)corr, (uint32_t)strlen(corr));
    bj_end_object(b);
    bj_put_string(b, (const uint8_t *)text, (uint32_t)strlen(text));
    bj_end_array(b);

    size_t len = 0;
    const uint8_t *data = bj_builder_data(b, &len);
    char url[1024];
    snprintf(url, sizeof url, "%s/pub/%s?headers=1&id=req.%s",
             o.url_base, o.subject, corr);
    c.headers = curl_slist_append(NULL, "Content-Type: " BJMSG_MEDIA_TYPE);
    curl_easy_setopt(c.curl, CURLOPT_HTTPHEADER, c.headers);
    curl_easy_setopt(c.curl, CURLOPT_POST, 1L);
    curl_easy_setopt(c.curl, CURLOPT_POSTFIELDS, data);
    curl_easy_setopt(c.curl, CURLOPT_POSTFIELDSIZE, (long)len);

    rc = 1;
    if (client_perform(&c, url) != 200) {
        fprintf(stderr, "bjmsg: request could not be published\n");
        goto done;
    }

    for (uint64_t waited = 0; waited < timeout_ms && !g_stop;
         waited += (uint64_t)idle) {
        snprintf(url, sizeof url, "%s/sub/%s?from=%llu",
                 o.url_base, reply_to, (unsigned long long)from);
        curl_easy_setopt(c.curl, CURLOPT_HTTPGET, 1L);
        curl_easy_setopt(c.curl, CURLOPT_CUSTOMREQUEST, NULL);

        long status = client_perform(&c, url);
        if (status == 404) { sleep_ms(idle); continue; }
        if (status != 200) { report_error(&c, status); goto done; }

        /* Every message in the batch: on a shared reply subject most of
         * them belong to other requesters. */
        corr_scan sc;
        memset(&sc, 0, sizeof sc);
        sc.want = corr;
        bj_visitor v = bjm_visitor_noop(&sc);
        v.on_key = cs_key;
        v.on_int = cs_int;
        v.on_binary = cs_binary;
        v.on_object_end = cs_object_end;
        if (bj_decode(c.body.p, c.body.len, &v, NULL) != BJ_OK) {
            fprintf(stderr, "bjmsg: malformed reply batch\n");
            goto done;
        }
        if (sc.highest >= from) from = sc.highest + 1;
        if (sc.matched) { rc = 0; goto done; }
        sleep_ms(idle);
    }
    if (rc) fprintf(stderr, "bjmsg: no reply within %llums\n",
                    (unsigned long long)timeout_ms);

done:
    bj_builder_free(b);
    client_free(&c);
    return rc;
}

#define REPLY_USAGE \
    "usage: bjmsg reply [--url URL] <subject> --exec CMD [--group G]\n" \
    "                   [--interval MS]\n" \
    "\n" \
    "Serve requests on <subject>: run CMD for each, publish its stdout to\n" \
    "the request's reply_to with the same correlation. Repliers share a\n" \
    "queue group (default " REPLY_GROUP_DEFAULT "), so each request is\n" \
    "handled once however many are running.\n"

int bjm_cmd_reply(int argc, char **argv) {
    query_opts o;
    int rc = query_parse(argc, argv, &o, REPLY_USAGE);
    if (rc) return rc == 1 ? 0 : rc;
    const char *group = o.group ? o.group : REPLY_GROUP_DEFAULT;
    if (!o.subject || !bjm_subject_valid(o.subject) || !o.exec ||
        !bjm_group_valid(group)) {
        fputs(REPLY_USAGE, stderr);
        return 2;
    }
    long idle = o.interval_ms > 0 ? o.interval_ms : DEFAULT_POLL_MS;

    client c;
    if (client_init(&c, o.retry_ms) != 0) return 1;
    buf out = {0};

    rc = 0;
    while (!g_stop) {
        char url[1024];
        snprintf(url, sizeof url, "%s/take/%s?group=%s&max=1",
                 o.url_base, o.subject, group);
        curl_easy_setopt(c.curl, CURLOPT_CUSTOMREQUEST, "POST");
        curl_easy_setopt(c.curl, CURLOPT_POSTFIELDS, "");
        curl_easy_setopt(c.curl, CURLOPT_POSTFIELDSIZE, 0L);

        long status = client_perform(&c, url);
        if (g_stop) break;
        if (status < 0) { rc = 1; break; }
        if (status == 404) { sleep_ms(idle); continue; }
        if (status != 200) { report_error(&c, status); rc = 1; break; }

        rr_msg m;
        memset(&m, 0, sizeof m);
        bj_visitor v = bjm_visitor_noop(&m);
        v.on_key = rr_key;
        v.on_int = rr_int;
        v.on_binary = rr_binary;
        if (bj_decode(c.body.p, c.body.len, &v, NULL) != BJ_OK) {
            fprintf(stderr, "bjmsg: malformed take response\n");
            rc = 1;
            break;
        }
        if (!m.have) { free(m.payload); sleep_ms(idle); continue; }

        const uint8_t *h = NULL, *msg = m.payload;
        size_t hlen = 0, mlen = m.plen;
        char reply_to[BJM_SUBJECT_MAX + 1] = "", corr[64] = "";
        if (m.type == BJM_ENTRY_ENVELOPE)
            bjm_envelope_split(m.payload, m.plen, &h, &hlen, &msg, &mlen);
        if (h) {
            header_value(h, hlen, "reply_to", reply_to, sizeof reply_to);
            header_value(h, hlen, "correlation", corr, sizeof corr);
        }

        buf feed = {0};
        feed_for(msg, mlen, 0, &feed);
        int st = run_filter(o.exec, feed.p, feed.len, &out);
        buf_free(&feed);

        int ok = (st == 0);
        int replied = 0;
        if (ok && reply_to[0] && corr[0]) {
            size_t n = out.len;
            while (n > 0 && (out.p[n - 1] == '\n' || out.p[n - 1] == '\r')) n--;
            bj_builder *rb = bj_builder_new();
            if (rb) {
                bj_put_string(rb, out.p, (uint32_t)n);
                size_t rlen = 0;
                const uint8_t *rdata = bj_builder_data(rb, &rlen);
                if (rdata &&
                    publish_reply(&c, &o, reply_to, corr, rdata, rlen) != 200) {
                    fprintf(stderr, "bjmsg: could not publish the reply\n");
                    ok = 0;
                } else if (rdata) {
                    replied = 1;
                }
                bj_builder_free(rb);
            }
        }

        snprintf(url, sizeof url, "%s/%s/%s?group=%s&index=%lld",
                 o.url_base, ok ? "done" : "fail", o.subject, group, m.index);
        curl_easy_setopt(c.curl, CURLOPT_CUSTOMREQUEST, "POST");
        curl_easy_setopt(c.curl, CURLOPT_POSTFIELDS, "");
        curl_easy_setopt(c.curl, CURLOPT_POSTFIELDSIZE, 0L);
        client_perform(&c, url);

        /* A request with no reply_to is legitimate — fire and forget —
         * but saying "replied" when nothing was sent would hide the
         * header extraction silently failing. */
        printf("%lld\t%s\n", m.index,
               !ok ? "failed" : replied ? "replied" : "done (no reply_to)");
        fflush(stdout);
        free(m.payload);
    }

    buf_free(&out);
    client_free(&c);
    return rc;
}

/* ---- effectively-once pipelines ---------------------------------------- */

/*
 * Run `cmd` with `in` on its stdin and collect its stdout.
 *
 * The input goes via a temporary file rather than a second pipe. Two
 * pipes to one child deadlock as soon as the child writes more than a
 * pipe buffer before reading all of its input, and avoiding that needs a
 * poll loop for a case a message pipeline does not need. A file also
 * keeps `cmd` a shell string, so --exec 'jq .field' works as written.
 *
 * Returns the child's exit code, or -1 if it could not be run or died on
 * a signal.
 */
static int run_filter(const char *cmd, const uint8_t *in, size_t in_len,
                      buf *out) {
    char path[] = "/tmp/bjmsg-in-XXXXXX";
    int fd = mkstemp(path);
    if (fd < 0) return -1;
    if (in_len && write(fd, in, in_len) != (ssize_t)in_len) {
        close(fd); unlink(path); return -1;
    }
    close(fd);

    /*
     * The subshell is load-bearing. `CMD < file` binds the redirect to
     * the last simple command, so `grep x || true` would leave grep
     * reading the parent's stdin and hang; `( CMD ) < file` redirects
     * the whole thing.
     */
    char line[2048];
    if ((size_t)snprintf(line, sizeof line, "( %s ) < %s", cmd, path)
            >= sizeof line) {
        unlink(path); return -1;
    }

    FILE *child = popen(line, "r");
    if (!child) { unlink(path); return -1; }

    char chunk[8192];
    size_t n;
    out->len = 0;
    while ((n = fread(chunk, 1, sizeof chunk, child)) > 0)
        if (buf_append(out, chunk, n) != 0) break;

    int status = pclose(child);
    unlink(path);
    /* pclose answers a wait status, not an exit code. */
    if (status == -1 || !WIFEXITED(status)) return -1;
    return WEXITSTATUS(status);
}

/* Appends a top-level STRING's value, not its rendering. */
typedef struct { buf *out; int got; } str_peek;

static void peek_string(void *ctx, const uint8_t *sv, uint32_t len) {
    str_peek *p = ctx;
    if (p->got) return;
    buf_append(p->out, sv, len);
    p->got = 1;
}

/*
 * The bytes a handler should see for a message.
 *
 * `raw` hands over the encoded binjson untouched. Otherwise the message
 * is rendered as text — except for a top-level STRING, where the handler
 * gets the string's *value*. Handing it the rendering would pass on the
 * quotes and the escaping too, so `tr a-z A-Z` over "hello" would come
 * back as a string containing quote characters.
 */
static void feed_for(const uint8_t *payload, size_t plen, int raw, buf *out) {
    out->len = 0;
    if (raw) { buf_append(out, payload, plen); return; }

    if (plen > 0 && payload[0] == BJ_TYPE_STRING) {
        str_peek pk = { out, 0 };
        bj_visitor v = bjm_visitor_noop(&pk);
        v.on_string = peek_string;
        if (bj_decode(payload, plen, &v, NULL) == BJ_OK && pk.got) return;
        out->len = 0;
    }

    char *text = NULL;
    size_t tlen = 0;
    FILE *f = open_memstream(&text, &tlen);
    if (f) {
        bjm_render(f, payload, plen);
        fclose(f);
        buf_append(out, text, tlen);
    }
    free(text);
}

/* One input message: its index and its payload bytes, copied because the
 * response buffer is reused by the next request. */
typedef struct {
    char      key[16];
    long long index;
    uint8_t  *payload;
    size_t    plen;
    int       have;
} pipe_msg;

static void pm_key(void *ctx, const uint8_t *k, uint32_t len) {
    pipe_msg *m = ctx;
    if (len >= sizeof m->key) len = sizeof m->key - 1;
    memcpy(m->key, k, len);
    m->key[len] = '\0';
}

static void pm_int(void *ctx, double v) {
    pipe_msg *m = ctx;
    if (!m->have && strcmp(m->key, "index") == 0) m->index = (long long)v;
}

static void pm_binary(void *ctx, const uint8_t *b, uint32_t len) {
    pipe_msg *m = ctx;
    if (m->have || strcmp(m->key, "payload") != 0) return;
    m->payload = malloc(len ? len : 1);
    if (!m->payload) return;
    memcpy(m->payload, b, len);
    m->plen = len;
    m->have = 1;
}

#define PIPE_USAGE \
    "usage: bjmsg pipe [--url URL] <in-subject> --consumer NAME\n" \
    "                  --to <out-subject> --exec CMD [--raw] [--follow]\n" \
    "                  [--interval MS]\n" \
    "\n" \
    "Read a subject, transform each message with CMD, publish the result\n" \
    "to another subject. The publish and the input's acknowledgement\n" \
    "happen in ONE broker call, and the output carries an idempotency key\n" \
    "derived from the input index — so a crash anywhere in the loop\n" \
    "replays the input and the rerun collapses onto the output that is\n" \
    "already there. One input, one output, whatever fails.\n" \
    "\n" \
    "  --exec CMD   payload on stdin, replacement message on stdout;\n" \
    "               empty stdout drops the message, a non-zero exit\n" \
    "               leaves it unacknowledged to be retried\n" \
    "  --raw        stdin and stdout are encoded binjson rather than the\n" \
    "               rendered text form\n" \
    "  --follow     keep polling once the input is drained\n"

int bjm_cmd_pipe(int argc, char **argv) {
    query_opts o;
    int rc = query_parse(argc, argv, &o, PIPE_USAGE);
    if (rc) return rc == 1 ? 0 : rc;
    if (!o.subject || !bjm_subject_valid(o.subject) ||
        !o.consumer || !bjm_consumer_valid(o.consumer) ||
        !o.to || !bjm_subject_valid(o.to) || !o.exec) {
        fputs(PIPE_USAGE, stderr);
        return 2;
    }
    long idle = o.interval_ms > 0 ? o.interval_ms : DEFAULT_POLL_MS;

    client c;
    if (client_init(&c, o.retry_ms) != 0) return 1;

    buf outbuf = {0};
    bj_builder *bld = bj_builder_new();
    if (!bld) { client_free(&c); return 1; }

    rc = 0;
    while (!g_stop) {
        /* One at a time: the ack rides with the output, so a batch would
         * have to be published before any of it could be acknowledged. */
        char url[1024];
        snprintf(url, sizeof url, "%s/sub/%s?consumer=%s&max=1",
                 o.url_base, o.subject, o.consumer);
        curl_easy_setopt(c.curl, CURLOPT_HTTPGET, 1L);
        curl_easy_setopt(c.curl, CURLOPT_CUSTOMREQUEST, NULL);

        long status = client_perform(&c, url);
        if (g_stop) break;
        if (status < 0) { rc = 1; break; }
        if (status != 200) {
            if (status == 404 && o.follow) { sleep_ms(idle); continue; }
            report_error(&c, status);
            rc = 1;
            break;
        }

        pipe_msg m;
        memset(&m, 0, sizeof m);
        bj_visitor v = bjm_visitor_noop(&m);
        v.on_key = pm_key;
        v.on_int = pm_int;
        v.on_binary = pm_binary;
        if (bj_decode(c.body.p, c.body.len, &v, NULL) != BJ_OK) {
            fprintf(stderr, "bjmsg: malformed batch from broker\n");
            rc = 1;
            break;
        }
        if (!m.have) {
            free(m.payload);
            if (!o.follow) break;
            sleep_ms(idle);
            continue;
        }

        /* Hand the handler either the raw encoded message or its text
         * rendering, and take its stdout back the same way. */
        buf feed = {0};
        feed_for(m.payload, m.plen, o.raw, &feed);

        char env[32];
        setenv("BJMSG_SUBJECT", o.subject, 1);
        setenv("BJMSG_CONSUMER", o.consumer, 1);
        snprintf(env, sizeof env, "%lld", m.index);
        setenv("BJMSG_INDEX", env, 1);

        int st = run_filter(o.exec, feed.p, feed.len, &outbuf);
        buf_free(&feed);
        free(m.payload);

        if (st != 0) {
            /* Not acknowledged, so the broker hands it back next poll. */
            fprintf(stderr, "bjmsg: %lld failed (exit %d), not acknowledged\n",
                    m.index, st);
            rc = 1;
            if (!o.follow) break;
            sleep_ms(idle);
            continue;
        }

        /*
         * Deterministic from the input, NOT from the output: rerunning a
         * handler that is not perfectly deterministic must still collapse
         * onto the message its first run produced.
         */
        char id[BJM_DEDUP_ID_MAX + 1];
        snprintf(id, sizeof id, "%s.%s.%lld", o.consumer, o.subject, m.index);

        if (outbuf.len == 0) {
            /* Nothing to publish — the handler dropped it. Acknowledge on
             * its own so the input still makes progress. */
            snprintf(url, sizeof url, "%s/ack/%s?consumer=%s&index=%lld",
                     o.url_base, o.subject, o.consumer, m.index);
            curl_easy_setopt(c.curl, CURLOPT_CUSTOMREQUEST, "POST");
            curl_easy_setopt(c.curl, CURLOPT_POSTFIELDS, "");
            curl_easy_setopt(c.curl, CURLOPT_POSTFIELDSIZE, 0L);
            if (client_perform(&c, url) != 200) { rc = 1; break; }
            printf("%lld\tdropped\n", m.index);
            fflush(stdout);
            continue;
        }

        const uint8_t *body = outbuf.p;
        size_t body_len = outbuf.len;
        if (!o.raw) {
            /* Text out becomes a binjson STRING, trailing newline and all
             * removed — a shell filter almost always adds one. */
            size_t n = outbuf.len;
            while (n > 0 && (outbuf.p[n - 1] == '\n' || outbuf.p[n - 1] == '\r')) n--;
            bj_builder_reset(bld);
            bj_put_string(bld, outbuf.p, (uint32_t)n);
            body = bj_builder_data(bld, &body_len);
            if (!body) { rc = 1; break; }
        }

        snprintf(url, sizeof url,
                 "%s/pub/%s?id=%s&ack_subject=%s&ack_consumer=%s&ack_index=%lld",
                 o.url_base, o.to, id, o.subject, o.consumer, m.index);
        curl_easy_setopt(c.curl, CURLOPT_CUSTOMREQUEST, NULL);
        curl_easy_setopt(c.curl, CURLOPT_POST, 1L);
        curl_easy_setopt(c.curl, CURLOPT_POSTFIELDS, body);
        curl_easy_setopt(c.curl, CURLOPT_POSTFIELDSIZE, (long)body_len);
        if (!c.headers) {
            c.headers = curl_slist_append(NULL, "Content-Type: " BJMSG_MEDIA_TYPE);
            curl_easy_setopt(c.curl, CURLOPT_HTTPHEADER, c.headers);
        }

        status = client_perform(&c, url);
        if (status != 200) {
            if (status > 0) report_error(&c, status);
            rc = 1;
            break;
        }
        printf("%lld\t->\t%s\n", m.index, o.to);
        fflush(stdout);
    }

    bj_builder_free(bld);
    buf_free(&outbuf);
    client_free(&c);
    return rc;
}

/* ---- the dead-letter channel ------------------------------------------- */

/*
 * A dead-letter envelope, rendered readably. The payload arrives as
 * BINARY holding the original message, so it decodes one level further
 * than a plain `sub` of the .dead subject would show.
 */
typedef struct {
    char      key[16];
    char      group[BJM_GROUP_MAX + 1];
    long long index, attempts;
    int       printed;
} dead_scan;

static void d_key(void *ctx, const uint8_t *k, uint32_t len) {
    dead_scan *d = ctx;
    if (len >= sizeof d->key) len = sizeof d->key - 1;
    memcpy(d->key, k, len);
    d->key[len] = '\0';
}

static void d_string(void *ctx, const uint8_t *v, uint32_t len) {
    dead_scan *d = ctx;
    if (strcmp(d->key, "group") != 0) return;
    if (len > BJM_GROUP_MAX) len = BJM_GROUP_MAX;
    memcpy(d->group, v, len);
    d->group[len] = '\0';
}

static void d_int(void *ctx, double v) {
    dead_scan *d = ctx;
    if (strcmp(d->key, "index") == 0)         d->index = (long long)v;
    else if (strcmp(d->key, "attempts") == 0) d->attempts = (long long)v;
}

/* The envelope itself arrives as the outer batch's BINARY payload; the
 * original message is the BINARY inside it. */
static void d_envelope(void *ctx, const uint8_t *bytes, uint32_t len);

typedef struct {
    char      key[16];
    long long dlq_index;
} dead_outer;

static void o_key(void *ctx, const uint8_t *k, uint32_t len) {
    dead_outer *o = ctx;
    if (len >= sizeof o->key) len = sizeof o->key - 1;
    memcpy(o->key, k, len);
    o->key[len] = '\0';
}

static void o_int(void *ctx, double v) {
    dead_outer *o = ctx;
    if (strcmp(o->key, "index") == 0) o->dlq_index = (long long)v;
}

static long long g_dlq_index;   /* the entry being printed */

static void d_payload(void *ctx, const uint8_t *bytes, uint32_t len) {
    dead_scan *d = ctx;
    if (strcmp(d->key, "payload") != 0 || d->printed) return;
    printf("%lld\t%s\torig=%lld\tattempts=%lld\t",
           g_dlq_index, d->group, d->index, d->attempts);
    if (bjm_render(stdout, bytes, len) != BJ_OK) fputs("<undecodable>", stdout);
    fputc('\n', stdout);
    d->printed = 1;
}

static void d_envelope(void *ctx, const uint8_t *bytes, uint32_t len) {
    dead_outer *o = ctx;
    g_dlq_index = o->dlq_index;
    dead_scan d;
    memset(&d, 0, sizeof d);
    bj_visitor v = bjm_visitor_noop(&d);
    v.on_key = d_key;
    v.on_string = d_string;
    v.on_int = d_int;
    v.on_binary = d_payload;
    if (bj_decode(bytes, len, &v, NULL) != BJ_OK || !d.printed)
        printf("%lld\t<not a dead-letter envelope>\n", o->dlq_index);
}

#define DEAD_USAGE \
    "usage: bjmsg dead [--url URL] <subject> [--from N]\n" \
    "\n" \
    "Show <subject>.dead: jobs a queue group gave up on, one per line as\n" \
    "  <dead index> <group> orig=<index> attempts=<n> <payload>\n" \
    "\n" \
    "Put one back with: bjmsg requeue <subject> --index <dead index>\n"

int bjm_cmd_dead(int argc, char **argv) {
    query_opts o;
    int rc = query_parse(argc, argv, &o, DEAD_USAGE);
    if (rc) return rc == 1 ? 0 : rc;
    if (!o.subject || !bjm_subject_valid(o.subject)) {
        fputs(DEAD_USAGE, stderr);
        return 2;
    }

    char url[1024];
    snprintf(url, sizeof url, "%s/sub/%s.dead?from=%llu",
             o.url_base, o.subject,
             (unsigned long long)(o.before ? o.before : 1));

    client c;
    if (client_init(&c, o.retry_ms) != 0) return 1;
    curl_easy_setopt(c.curl, CURLOPT_HTTPGET, 1L);

    rc = 1;
    long status = client_perform(&c, url);
    if (status == 200) {
        dead_outer outer = {{0}, 0};
        bj_visitor v = bjm_visitor_noop(&outer);
        v.on_key = o_key;
        v.on_int = o_int;
        v.on_binary = d_envelope;
        rc = bj_decode(c.body.p, c.body.len, &v, NULL) == BJ_OK ? 0 : 1;
    } else if (status == 404) {
        rc = 0;   /* nothing has ever died here */
    } else if (status > 0) {
        report_error(&c, status);
    }

    client_free(&c);
    return rc;
}

#define REQUEUE_USAGE \
    "usage: bjmsg requeue [--url URL] <subject> --index N\n" \
    "\n" \
    "Publish a dead-lettered message back to its subject. N is the index\n" \
    "in <subject>.dead, the first column of `bjmsg dead`. The message is\n" \
    "appended with a new index; the dead-letter record stays put.\n"

int bjm_cmd_requeue(int argc, char **argv) {
    query_opts o;
    int rc = query_parse(argc, argv, &o, REQUEUE_USAGE);
    if (rc) return rc == 1 ? 0 : rc;
    if (!o.subject || !bjm_subject_valid(o.subject) || o.index == 0) {
        fputs(REQUEUE_USAGE, stderr);
        return 2;
    }

    char url[1024];
    snprintf(url, sizeof url, "%s/requeue/%s?index=%llu",
             o.url_base, o.subject, (unsigned long long)o.index);
    return query_run(&o, "POST", url);
}

#define SEEK_USAGE \
    "usage: bjmsg seek [--url URL] <subject> --consumer NAME --index N\n" \
    "\n" \
    "Move a subscription's read receipt forward to N — how a consumer\n" \
    "left behind by a trim gets going again. Receipts never move\n" \
    "backwards; to replay, delete the subscription and rejoin.\n"

int bjm_cmd_seek(int argc, char **argv) {
    query_opts o;
    int rc = query_parse(argc, argv, &o, SEEK_USAGE);
    if (rc) return rc == 1 ? 0 : rc;
    if (!o.subject || !bjm_subject_valid(o.subject) ||
        !o.consumer || !bjm_consumer_valid(o.consumer) || o.index == 0) {
        fputs(SEEK_USAGE, stderr);
        return 2;
    }

    char url[1024];
    snprintf(url, sizeof url, "%s/ack/%s?consumer=%s&index=%llu",
             o.url_base, o.subject, o.consumer,
             (unsigned long long)o.index);
    return query_run(&o, "POST", url);
}

#define SUBJECTS_USAGE \
    "usage: bjmsg subjects [--url URL] [--retry MS] [<pattern>]\n" \
    "\n" \
    "With no pattern, every subject. A pattern matches token-wise on '.':\n" \
    "'*' is one token, '>' is this one and everything below it.\n"

int bjm_cmd_subjects(int argc, char **argv) {
    query_opts o;
    int rc = query_parse(argc, argv, &o, SUBJECTS_USAGE);
    if (rc) return rc == 1 ? 0 : rc;
    if (o.subject && !bjm_pattern_valid(o.subject)) {
        fputs(SUBJECTS_USAGE, stderr);
        return 2;
    }

    char url[1024];
    if (o.subject) {
        client tmp;
        if (client_init(&tmp, o.retry_ms) != 0) return 1;
        char *esc = curl_easy_escape(tmp.curl, o.subject, 0);
        snprintf(url, sizeof url, "%s/subjects?pattern=%s",
                 o.url_base, esc ? esc : o.subject);
        if (esc) curl_free(esc);
        client_free(&tmp);
    } else {
        snprintf(url, sizeof url, "%s/subjects", o.url_base);
    }
    return query_run(&o, NULL, url);
}

#define INFO_USAGE "usage: bjmsg info [--url URL] [--retry MS] <subject>\n"

int bjm_cmd_info(int argc, char **argv) {
    query_opts o;
    int rc = query_parse(argc, argv, &o, INFO_USAGE);
    if (rc) return rc == 1 ? 0 : rc;
    if (!o.subject || !bjm_subject_valid(o.subject)) {
        fputs(INFO_USAGE, stderr);
        return 2;
    }

    char url[1024];
    snprintf(url, sizeof url, "%s/info/%s", o.url_base, o.subject);
    return query_run(&o, NULL, url);
}

#define HEALTH_USAGE "usage: bjmsg health [--url URL] [--retry MS]\n"

int bjm_cmd_health(int argc, char **argv) {
    query_opts o;
    int rc = query_parse(argc, argv, &o, HEALTH_USAGE);
    if (rc) return rc == 1 ? 0 : rc;

    char url[1024];
    snprintf(url, sizeof url, "%s/health", o.url_base);
    return query_run(&o, NULL, url);
}

#define TRIM_USAGE \
    "usage: bjmsg trim [--url URL] <subject> (--before N | --keep N) [--force]\n" \
    "\n" \
    "  --before N  discard messages with an index below N\n" \
    "  --keep N    keep only the newest N messages\n" \
    "  --force     trim past consumers' read receipts, discarding messages\n" \
    "              they have not read yet (refused without this)\n"

int bjm_cmd_trim(int argc, char **argv) {
    query_opts o;
    int rc = query_parse(argc, argv, &o, TRIM_USAGE);
    if (rc) return rc == 1 ? 0 : rc;
    if (!o.subject || !bjm_subject_valid(o.subject) ||
        (o.before == 0 && o.keep == 0)) {
        fputs(TRIM_USAGE, stderr);
        return 2;
    }

    char url[1024];
    int n = snprintf(url, sizeof url, "%s/trim/%s?", o.url_base, o.subject);
    if (o.keep) n += snprintf(url + n, sizeof url - n, "keep=%llu",
                              (unsigned long long)o.keep);
    else        n += snprintf(url + n, sizeof url - n, "before=%llu",
                              (unsigned long long)o.before);
    if (o.force) snprintf(url + n, sizeof url - n, "&force=1");
    return query_run(&o, "POST", url);
}
