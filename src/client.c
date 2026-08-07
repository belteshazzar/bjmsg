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
        "usage: bjmsg pub [--url URL] [--retry MS] <subject>\n"
        "                 (<text> | --int N | --file PATH)\n"
        "\n"
        "  <text>       publish a binjson STRING\n"
        "  --int N      publish a binjson INT\n"
        "  --file PATH  publish PATH's bytes verbatim (already-encoded\n"
        "               binjson); PATH may be - for stdin\n"
        "  --retry MS   wait MS between attempts when the broker cannot be\n"
        "               reached (default 5000; 0 disables retrying)\n");
}

int bjm_cmd_pub(int argc, char **argv) {
    const char *url_base = DEFAULT_URL;
    const char *subject = NULL, *text = NULL, *file = NULL;
    int have_int = 0;
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
        if (have_int) bj_put_int(b, int_value);
        else bj_put_string(b, (const uint8_t *)text, (uint32_t)strlen(text));
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

    char url[1024];
    snprintf(url, sizeof url, "%s/pub/%s", url_base, subject);

    int rc = 1;
    /*
     * Not idempotent: a publish that broke mid-flight may already be in
     * the log, so only a failure that never reached the broker is retried.
     */
    long status = client_perform_ex(&c, url, 0);
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
    printf("%lld\t", s->index);
    if (bjm_render(stdout, bytes, len) != BJ_OK) fputs("<undecodable>", stdout);
    fputc('\n', stdout);
    fflush(stdout);
    s->count++;
    if ((uint64_t)s->index >= s->next) s->next = (uint64_t)s->index + 1;
}

static void sub_usage(void) {
    fprintf(stderr,
        "usage: bjmsg sub [--url URL] <subject> [--consumer NAME]\n"
        "                 [--from N | --tail] [--follow] [--interval MS]\n"
        "                 [--retry MS] [--max BYTES]\n"
        "\n"
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
    if (!bjm_subject_valid(subject)) {
        fprintf(stderr, "bjmsg: invalid subject '%s'\n", subject);
        return 2;
    }
    if (consumer && !bjm_consumer_valid(consumer)) {
        fprintf(stderr, "bjmsg: invalid consumer '%s'\n", consumer);
        return 2;
    }
    if (from == 0) from = 1;
    /*
     * --tail asks a cursor past the end of the log, which the broker
     * answers with an empty batch plus the real last index. So the first
     * request costs nothing, delivers nothing, and tells us where to
     * start — no extra route needed.
     *
     * With --consumer the broker owns the cursor, so --tail becomes
     * ?start=last: it only decides where a consumer being seen for the
     * first time joins, and is ignored once it has a receipt.
     */
    if (tail && !consumer) from = UINT64_MAX;

    client c;
    if (client_init(&c, retry_ms) != 0) return 1;
    curl_easy_setopt(c.curl, CURLOPT_HTTPGET, 1L);

    /* Highest index handed to the user but not yet acknowledged. */
    uint64_t pending_ack = 0;

    int rc = 0;
    while (!g_stop) {
        char url[1024];
        if (consumer) {
            int n = snprintf(url, sizeof url,
                             "%s/sub/%s?consumer=%s&max=%llu",
                             url_base, subject, consumer,
                             (unsigned long long)max_bytes);
            /* Piggyback the receipt for the previous batch: the broker
             * persists it before choosing what to send next, so this
             * costs no extra round trip. */
            if (pending_ack && n > 0 && (size_t)n < sizeof url)
                n += snprintf(url + n, sizeof url - n, "&ack=%llu",
                              (unsigned long long)pending_ack);
            if (tail && n > 0 && (size_t)n < sizeof url)
                snprintf(url + n, sizeof url - n, "&start=last");
        } else {
            snprintf(url, sizeof url, "%s/sub/%s?from=%llu&max=%llu",
                     url_base, subject, (unsigned long long)from,
                     (unsigned long long)max_bytes);
        }

        long status = client_perform(&c, url);
        if (g_stop) break;
        if (status < 0) { rc = 1; break; }
        if (status != 200) {
            /* A subject that does not exist yet is not an error when
             * following — the publisher may simply not have run. Nor when
             * tailing an absent subject: it has no backlog to skip, so
             * the first message published is the first one we want. */
            if (status == 404 && (follow || tail)) {
                if (tail) { tail = 0; from = 1; }
                if (!follow) { rc = 0; break; }
                sleep_ms(interval);
                continue;
            }
            report_error(&c, status);
            rc = 1;
            break;
        }

        if (c.skipped) {
            fprintf(stderr, "bjmsg: %llu message(s) had already been trimmed; "
                            "starting at the oldest one kept\n",
                    (unsigned long long)c.skipped);
            c.skipped = 0;
        }

        sub_scan s = {0};
        s.next = from;
        bj_visitor v = bjm_visitor_noop(&s);
        v.on_int = s_int;
        v.on_binary = s_binary;
        v.on_key = s_key;
        if (bj_decode(c.body.p, c.body.len, &v, NULL) != BJ_OK) {
            fprintf(stderr, "bjmsg: malformed batch from server\n");
            rc = 1;
            break;
        }
        if (tail && !consumer) {
            /* The probe told us where the log ends; start just past it. */
            from = c.last_index + 1;
            tail = 0;
            continue;
        }
        if (s.count > 0) pending_ack = s.next - 1;
        from = s.next;

        if (s.count == 0) {
            if (!follow) break;      /* caught up */
            sleep_ms(interval);
        }
    }

    /*
     * The receipt for the last batch has nowhere to piggyback once the
     * loop ends, so send it explicitly — unless a later poll already
     * carried it, which X-Bjmsg-Acked tells us for free.
     */
    if (consumer && pending_ack > c.acked && rc == 0) {
        char url[1024];
        snprintf(url, sizeof url, "%s/ack/%s?consumer=%s&index=%llu",
                 url_base, subject, consumer, (unsigned long long)pending_ack);
        curl_easy_setopt(c.curl, CURLOPT_POST, 1L);
        curl_easy_setopt(c.curl, CURLOPT_POSTFIELDS, "");
        curl_easy_setopt(c.curl, CURLOPT_POSTFIELDSIZE, 0L);
        /*
         * We are here *because* of the Ctrl-C, so clear the stop flag or
         * the request would refuse to run — and take one attempt only,
         * since hanging on the way out is worse than a redelivery that
         * at-least-once already permits. A second Ctrl-C still lands.
         */
        g_stop = 0;
        c.retry_ms = 0;
        c.waiting = 0;
        long status = client_perform(&c, url);
        if (status != 200)
            fprintf(stderr, "bjmsg: final ack of index %llu failed"
                            " (that batch will be redelivered)\n",
                    (unsigned long long)pending_ack);
    }

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

#define SUBJECTS_USAGE "usage: bjmsg subjects [--url URL] [--retry MS]\n"

int bjm_cmd_subjects(int argc, char **argv) {
    query_opts o;
    int rc = query_parse(argc, argv, &o, SUBJECTS_USAGE);
    if (rc) return rc == 1 ? 0 : rc;

    char url[1024];
    snprintf(url, sizeof url, "%s/subjects", o.url_base);
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
