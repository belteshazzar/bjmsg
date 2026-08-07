/*
 * server.c — the broker: http11c routes over the subject store.
 *
 * Protocol (every success body is binjson; errors are text/plain so a
 * bare curl is readable):
 *
 *   POST /pub/<subject>              body: one binjson value (the message)
 *                                    -> { subject, index }
 *   GET  /sub/<subject>?from=&max=   -> ARRAY of { index, term, type, payload }
 *   GET  /subjects                   -> ARRAY of subject names
 *   GET  /health                     -> { ok, backend, subjects, conns }
 *
 * Subscribers are polled, not pushed: http11c serializes a response the
 * moment the handler returns, so there is no way to hold one open. The
 * cursor therefore lives in the request (`from`), which also means the
 * broker keeps no per-subscriber state — a subscriber that dies resumes
 * exactly where it left off, and there is no session table to reap.
 *
 * Handlers run on the event loop, so nothing here may block.
 */
#include "bjmsg.h"

#include "binjson.h"
#include "http11c.h"

#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define DEFAULT_MAX_BATCH_BYTES 65536
#define MAX_BODY_BYTES (4u * 1024 * 1024)
#define IDLE_TIMEOUT_SECS 120
#define RETENTION_SWEEP_SECS 10

typedef struct {
    bjm_store      *store;
    bj_builder     *bld;     /* scratch for composing small responses */
    http11c_server *srv;     /* for connection count in /health */
    time_t          started;
} app;

static http11c_server *g_srv;   /* for the signal handler only */

/* ---- response helpers ------------------------------------------------ */

static void res_bj(http11c_response *res, int code, const uint8_t *data, size_t len) {
    http11c_res_status(res, code);
    http11c_res_header(res, "Content-Type", BJMSG_MEDIA_TYPE);
    http11c_res_write(res, data, len);
}

static void res_err(http11c_response *res, int code, const char *msg) {
    http11c_res_header(res, "Content-Type", "text/plain; charset=utf-8");
    http11c_res_text(res, code, msg);
}

/* Map a BJ_ERR_* code onto the status that describes it to a client. */
static int status_for(int err) {
    switch (err) {
    case BJ_ERR_RANGE:  return 416;   /* cursor outside the log's bounds  */
    case BJ_ERR_STATE:  return 404;   /* no such subject / unopenable     */
    case BJ_ERR_OOM:    return 503;
    default:            return 500;
    }
}

/*
 * The subject named by a /pub/ or /sub/ path. Returns NULL and answers the
 * request itself when the path carries no valid subject.
 */
static const char *subject_of(http11c_request *req, http11c_response *res,
                              const char *prefix) {
    const char *subject = http11c_req_path(req) + strlen(prefix);
    if (!bjm_subject_valid(subject)) {
        res_err(res, 400, "invalid subject: expected 1-128 chars of "
                          "[A-Za-z0-9_.-], no leading/trailing dot\n");
        return NULL;
    }
    return subject;
}

static uint64_t query_u64(http11c_request *req, const char *key, uint64_t dflt) {
    char buf[32];
    if (http11c_req_query_get(req, key, buf, sizeof buf) != 1) return dflt;
    char *end;
    unsigned long long v = strtoull(buf, &end, 10);
    return (end == buf || *end) ? dflt : (uint64_t)v;
}

/*
 * Read the `consumer` query parameter. Returns 1 when present and valid,
 * 0 when absent, -1 when present but malformed (having answered the
 * request itself).
 */
static int query_consumer(http11c_request *req, http11c_response *res,
                          char *out, size_t out_size) {
    int rc = http11c_req_query_get(req, "consumer", out, out_size);
    if (rc == 0) return 0;
    if (rc != 1 || !bjm_consumer_valid(out)) {
        res_err(res, 400, "invalid consumer: expected 1-128 chars of "
                          "[A-Za-z0-9_.-], no leading/trailing dot\n");
        return -1;
    }
    return 1;
}

/* ---- handlers -------------------------------------------------------- */

static void h_publish(http11c_request *req, http11c_response *res) {
    app *a = http11c_req_ctx(req);

    const char *subject = subject_of(req, res, "/pub/");
    if (!subject) return;

    char type[64];
    if (http11c_req_content_type(req, type, sizeof type) != 1 ||
        strcmp(type, BJMSG_MEDIA_TYPE) != 0) {
        res_err(res, 415, "expected Content-Type: " BJMSG_MEDIA_TYPE "\n");
        return;
    }

    size_t len = 0;
    const uint8_t *body = (const uint8_t *)http11c_req_body(req, &len);
    if (!body || len == 0) {
        res_err(res, 400, "empty body\n");
        return;
    }
    /*
     * The log stores payloads opaquely, so nothing downstream would catch
     * a malformed message until a subscriber's decoder tripped over it —
     * by which point it is durable and undeliverable. Check the framing
     * here instead: exactly one complete binjson value, no trailing bytes.
     */
    size_t size = 0;
    if (bj_value_size(body, len, 0, &size) != BJ_OK || size != len) {
        res_err(res, 400, "body is not exactly one binjson value\n");
        return;
    }

    /*
     * ?headers=1 says the body is [ headers, message ] rather than the
     * message alone. The shape is checked here — a subscriber trusts the
     * entry type and would otherwise trip over a mislabelled payload —
     * but the contents of the headers object are never looked at.
     */
    int entry_type = BJM_ENTRY_PLAIN;
    if (query_u64(req, "headers", 0)) {
        if (!bjm_envelope_shape_ok(body, len)) {
            res_err(res, 400, "with ?headers=1 the body must be a 2-element "
                              "array: [ headers-object, message ]\n");
            return;
        }
        entry_type = BJM_ENTRY_ENVELOPE;
    }

    /*
     * An idempotency key makes this publish safe to repeat: if the id has
     * been seen inside the dedup window, the original index is returned
     * and nothing is appended. Look it up before the append, so a retry
     * of a request that already landed cannot add a second copy.
     */
    char id[BJM_DEDUP_ID_MAX + 1];
    int has_id = http11c_req_query_get(req, "id", id, sizeof id);
    if (has_id == 1 && !bjm_dedup_id_valid(id)) {
        res_err(res, 400, "invalid id: 1-128 printable bytes, no '/'\n");
        return;
    }
    has_id = has_id == 1;

    uint64_t index = 0;
    int duplicate = 0;
    if (has_id) {
        int e = bjm_dedup_lookup(a->store, subject, id, &duplicate, &index);
        if (e) { res_err(res, status_for(e), "dedup lookup failed\n"); return; }
    }

    if (!duplicate) {
        int e = bjm_publish(a->store, subject, entry_type, body,
                            (uint32_t)len, &index);
        if (e) {
            res_err(res, status_for(e), "publish failed\n");
            return;
        }
        /* After the append, so a failed publish leaves no claim behind. */
        if (has_id) bjm_dedup_record(a->store, subject, id, index);
    }

    /*
     * Read-process-write in one call: advance a subscription's receipt on
     * the INPUT subject as part of publishing the OUTPUT. That is what
     * makes a pipeline effectively-once — the two writes the broker would
     * otherwise do separately, with a crash window between them, now
     * happen before one response.
     *
     * They still are not one atomic write, and cannot be: they are
     * different files. The order is what carries the guarantee. Publish
     * first, ack second, so a crash in between replays the input, the
     * handler reruns, and the republished output collapses onto the
     * existing one by its idempotency key. The alternative order would
     * ack an input whose output never landed, and lose the message.
     */
    char ack_subject[BJM_SUBJECT_MAX + 1];
    char ack_consumer[BJM_CONSUMER_MAX + 1];
    uint64_t ack_index = query_u64(req, "ack_index", 0);
    int acked = 0;
    if (ack_index > 0 &&
        http11c_req_query_get(req, "ack_subject", ack_subject,
                              sizeof ack_subject) == 1 &&
        http11c_req_query_get(req, "ack_consumer", ack_consumer,
                              sizeof ack_consumer) == 1) {
        if (!bjm_subject_valid(ack_subject) || !bjm_consumer_valid(ack_consumer)) {
            res_err(res, 400, "invalid ack_subject or ack_consumer\n");
            return;
        }
        int e = bjm_cursor_set(a->store, ack_subject, ack_consumer, ack_index);
        if (e) { res_err(res, status_for(e), "ack failed\n"); return; }
        /* The durability point of the pair: once this returns, the output
         * is in its log and the input will not be replayed. */
        bjm_cursor_sync(a->store);
        acked = 1;
    }

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"subject", 7);
    bj_put_string(b, (const uint8_t *)subject, (uint32_t)strlen(subject));
    bj_put_key(b, (const uint8_t *)"index", 5);
    bj_put_int(b, (int64_t)index);
    if (acked) {
        bj_put_key(b, (const uint8_t *)"acked", 5);
        bj_put_int(b, (int64_t)ack_index);
    }
    if (has_id) {
        /* True means "this id was already here" — the caller's retry did
         * not create a second message. */
        bj_put_key(b, (const uint8_t *)"duplicate", 9);
        bj_put_bool(b, duplicate);
    }
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_subscribe(http11c_request *req, http11c_response *res) {
    app *a = http11c_req_ctx(req);

    const char *subject = subject_of(req, res, "/sub/");
    if (!subject) return;

    uint64_t from = query_u64(req, "from", 0);
    uint64_t max  = query_u64(req, "max", DEFAULT_MAX_BATCH_BYTES);
    if (max == 0 || max > MAX_BODY_BYTES) max = DEFAULT_MAX_BATCH_BYTES;
    if (from == 0) from = 1;

    uint64_t base = 0, ignored_last = 0, ignored_bytes = 0;
    if (bjm_subject_info(a->store, subject, &base,
                         &ignored_last, &ignored_bytes) != BJ_OK) {
        res_err(res, 404, "no such subject\n");
        return;
    }

    /*
     * With ?consumer=, the cursor is the broker's to remember: the client
     * sends no `from` at all, and instead piggybacks `ack` for the batch
     * it finished processing. One request per cycle still, and the
     * receipt is persisted before the next batch is handed out.
     */
    char consumer[BJM_CONSUMER_MAX + 1];
    int has_consumer = query_consumer(req, res, consumer, sizeof consumer);
    if (has_consumer < 0) return;

    uint64_t acked = 0;
    if (has_consumer) {
        uint64_t ack = query_u64(req, "ack", 0);
        int e = 0;
        if (ack) e = bjm_cursor_set(a->store, subject, consumer, ack);
        if (e) { res_err(res, status_for(e), "ack failed\n"); return; }

        int found = 0;
        e = bjm_cursor_get(a->store, subject, consumer, &found, &acked);
        if (e) { res_err(res, status_for(e), "cursor lookup failed\n"); return; }

        if (!found) {
            /* First sight of this consumer. ?start=last joins at the end
             * of the log, skipping the backlog; the default replays it. */
            char start[8];
            if (http11c_req_query_get(req, "start", start, sizeof start) == 1 &&
                strcmp(start, "last") == 0)
                acked = bjm_last_index(a->store, subject);
            e = bjm_cursor_set(a->store, subject, consumer, acked);
            if (e) { res_err(res, status_for(e), "cursor create failed\n"); return; }
        }
        from = acked + 1;
    }

    /*
     * A plain `from` cursor below the trim boundary is a browsing client
     * asking for messages that no longer exist — start it at the oldest
     * one there is and say how many it missed. A *consumer* cursor gets
     * no such courtesy: its receipt is a claim about what was delivered,
     * and quietly skipping past lost messages would hide the loss. That
     * case falls through to the 416 below.
     */
    uint64_t skipped = 0;
    if (!has_consumer && from <= base) {
        skipped = base + 1 - from;
        from = base + 1;
    }

    int count = 0;
    uint64_t last = 0;
    const uint8_t *out = NULL;
    size_t out_len = 0;
    int e = bjm_read(a->store, subject, from, (size_t)max,
                     &count, &out, &out_len, &last);
    if (e == BJ_ERR_RANGE) {
        /* The cursor points below the log's base: those messages were
         * trimmed away. Say so — "not found" would send a subscriber
         * looking for a subject that is right here. */
        res_err(res, 416, "those messages have been trimmed. Move on with "
                          "POST /ack/<subject>?consumer=&index= "
                          "(bjmsg seek), or start over by deleting the "
                          "subscription\n");
        return;
    }
    if (e) {
        res_err(res, status_for(e), "no such subject\n");
        return;
    }

    /* The cursor to send next time, and how far behind it is — both
     * cheap enough to put in headers so a client can pace itself without
     * decoding the body. */
    char buf[32];
    snprintf(buf, sizeof buf, "%d", count);
    http11c_res_header(res, "X-Bjmsg-Count", buf);
    snprintf(buf, sizeof buf, "%llu", (unsigned long long)last);
    http11c_res_header(res, "X-Bjmsg-Last-Index", buf);
    if (skipped) {
        snprintf(buf, sizeof buf, "%llu", (unsigned long long)skipped);
        http11c_res_header(res, "X-Bjmsg-Skipped", buf);
    }
    if (has_consumer) {
        snprintf(buf, sizeof buf, "%llu", (unsigned long long)acked);
        http11c_res_header(res, "X-Bjmsg-Acked", buf);
    }

    res_bj(res, 200, out, out_len);
}

static void h_ack(http11c_request *req, http11c_response *res) {
    app *a = http11c_req_ctx(req);

    const char *subject = subject_of(req, res, "/ack/");
    if (!subject) return;

    char consumer[BJM_CONSUMER_MAX + 1];
    int has_consumer = query_consumer(req, res, consumer, sizeof consumer);
    if (has_consumer < 0) return;
    if (!has_consumer) {
        res_err(res, 400, "?consumer=<name> is required\n");
        return;
    }

    uint64_t index = query_u64(req, "index", 0);
    if (index == 0) {
        res_err(res, 400, "?index=<n> is required and must be >= 1\n");
        return;
    }

    int e = bjm_cursor_set(a->store, subject, consumer, index);
    if (e) { res_err(res, status_for(e), "ack failed\n"); return; }

    /*
     * An explicit ack is the one a subscriber sends on its way out, so it
     * is also the point where paying for an fsync is worth it — losing it
     * would replay the batch the subscriber just finished.
     */
    bjm_cursor_sync(a->store);

    int found = 0;
    uint64_t acked = 0;
    bjm_cursor_get(a->store, subject, consumer, &found, &acked);

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"subject", 7);
    bj_put_string(b, (const uint8_t *)subject, (uint32_t)strlen(subject));
    bj_put_key(b, (const uint8_t *)"consumer", 8);
    bj_put_string(b, (const uint8_t *)consumer, (uint32_t)strlen(consumer));
    bj_put_key(b, (const uint8_t *)"acked", 5);
    bj_put_int(b, (int64_t)acked);
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_consumers(http11c_request *req, http11c_response *res) {
    app *a = http11c_req_ctx(req);

    const char *subject = subject_of(req, res, "/consumers/");
    if (!subject) return;

    const uint8_t *out = NULL;
    size_t out_len = 0;
    int e = bjm_consumers(a->store, subject, &out, &out_len);
    if (e) { res_err(res, status_for(e), "listing failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_unsubscribe(http11c_request *req, http11c_response *res) {
    app *a = http11c_req_ctx(req);

    const char *subject = subject_of(req, res, "/consumers/");
    if (!subject) return;

    char consumer[BJM_CONSUMER_MAX + 1];
    int has_consumer = query_consumer(req, res, consumer, sizeof consumer);
    if (has_consumer < 0) return;
    if (!has_consumer) {
        res_err(res, 400, "?consumer=<name> is required\n");
        return;
    }

    int deleted = 0;
    int e = bjm_cursor_delete(a->store, subject, consumer, &deleted);
    if (e) { res_err(res, status_for(e), "delete failed\n"); return; }

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"subject", 7);
    bj_put_string(b, (const uint8_t *)subject, (uint32_t)strlen(subject));
    bj_put_key(b, (const uint8_t *)"consumer", 8);
    bj_put_string(b, (const uint8_t *)consumer, (uint32_t)strlen(consumer));
    bj_put_key(b, (const uint8_t *)"deleted", 7);
    bj_put_bool(b, deleted);
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    /* Deleting a subscription that is already gone is a success: DELETE
     * is idempotent, and `deleted` carries whether anything was there.
     * (A 404 here would also have to be a text body, breaking the
     * binjson-on-success rule the clients decode against.) */
    res_bj(res, 200, out, out_len);
}

static void h_info(http11c_request *req, http11c_response *res) {
    app *a = http11c_req_ctx(req);

    const char *subject = subject_of(req, res, "/info/");
    if (!subject) return;

    uint64_t base = 0, last = 0, bytes = 0;
    int e = bjm_subject_info(a->store, subject, &base, &last, &bytes);
    if (e) { res_err(res, status_for(e), "no such subject\n"); return; }

    int nconsumers = 0;
    uint64_t min_acked = 0;
    bjm_consumer_stats(a->store, subject, &nconsumers, &min_acked);

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"subject", 7);
    bj_put_string(b, (const uint8_t *)subject, (uint32_t)strlen(subject));
    /* The log holds (base, last]: `base` is where trimming has cut to, so
     * `first` is the oldest message still readable. */
    bj_put_key(b, (const uint8_t *)"base", 4);
    bj_put_int(b, (int64_t)base);
    bj_put_key(b, (const uint8_t *)"first", 5);
    bj_put_int(b, (int64_t)(last > base ? base + 1 : 0));
    bj_put_key(b, (const uint8_t *)"last", 4);
    bj_put_int(b, (int64_t)last);
    bj_put_key(b, (const uint8_t *)"messages", 8);
    bj_put_int(b, (int64_t)(last - base));
    bj_put_key(b, (const uint8_t *)"bytes", 5);
    bj_put_int(b, (int64_t)bytes);
    bj_put_key(b, (const uint8_t *)"consumers", 9);
    bj_put_int(b, nconsumers);
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_trim(http11c_request *req, http11c_response *res) {
    app *a = http11c_req_ctx(req);

    const char *subject = subject_of(req, res, "/trim/");
    if (!subject) return;

    uint64_t last = bjm_last_index(a->store, subject);
    uint64_t before = query_u64(req, "before", 0);
    uint64_t keep = query_u64(req, "keep", 0);
    if (keep > 0) {
        /* Keep the newest `keep` messages: everything below that goes. */
        before = last > keep ? last - keep + 1 : 1;
    }
    if (before == 0) {
        res_err(res, 400, "?before=<n> or ?keep=<n> is required\n");
        return;
    }
    int force = query_u64(req, "force", 0) != 0;

    uint64_t new_base = 0, removed = 0;
    int e = bjm_trim(a->store, subject, before, force, &new_base, &removed);
    if (e) { res_err(res, status_for(e), "trim failed\n"); return; }

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"subject", 7);
    bj_put_string(b, (const uint8_t *)subject, (uint32_t)strlen(subject));
    bj_put_key(b, (const uint8_t *)"removed", 7);
    bj_put_int(b, (int64_t)removed);
    bj_put_key(b, (const uint8_t *)"base", 4);
    bj_put_int(b, (int64_t)new_base);
    bj_put_key(b, (const uint8_t *)"last", 4);
    bj_put_int(b, (int64_t)bjm_last_index(a->store, subject));
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

/* As query_consumer, for the `group` parameter. */
static int query_group(http11c_request *req, http11c_response *res,
                       char *out, size_t out_size) {
    int rc = http11c_req_query_get(req, "group", out, out_size);
    if (rc == 0) return 0;
    if (rc != 1 || !bjm_group_valid(out)) {
        res_err(res, 400, "invalid group: expected 1-128 chars of "
                          "[A-Za-z0-9_.-], no leading/trailing dot\n");
        return -1;
    }
    return 1;
}

/* Every queue route needs a valid subject and group; answers the request
 * itself and returns 0 when either is missing. */
static int queue_target(http11c_request *req, http11c_response *res,
                        const char *prefix, const char **subject,
                        char *group, size_t group_size) {
    *subject = subject_of(req, res, prefix);
    if (!*subject) return 0;
    int has = query_group(req, res, group, group_size);
    if (has < 0) return 0;
    if (!has) {
        res_err(res, 400, "?group=<name> is required\n");
        return 0;
    }
    return 1;
}

static void h_take(http11c_request *req, http11c_response *res) {
    app *a = http11c_req_ctx(req);

    const char *subject;
    char group[BJM_GROUP_MAX + 1];
    if (!queue_target(req, res, "/take/", &subject, group, sizeof group)) return;

    int max = (int)query_u64(req, "max", 1);
    /* UINT64_MAX means "whatever the group is configured for". */
    uint64_t lease = query_u64(req, "lease", UINT64_MAX);

    int count = 0;
    const uint8_t *out = NULL;
    size_t out_len = 0;
    int e = bjm_take(a->store, subject, group, max, lease,
                     &count, &out, &out_len);
    if (e) { res_err(res, status_for(e), "no such subject\n"); return; }

    char buf[32];
    snprintf(buf, sizeof buf, "%d", count);
    http11c_res_header(res, "X-Bjmsg-Count", buf);
    res_bj(res, 200, out, out_len);
}

static void h_job_end(http11c_request *req, http11c_response *res, int done) {
    app *a = http11c_req_ctx(req);

    const char *subject;
    char group[BJM_GROUP_MAX + 1];
    const char *prefix = done ? "/done/" : "/fail/";
    if (!queue_target(req, res, prefix, &subject, group, sizeof group)) return;

    uint64_t index = query_u64(req, "index", 0);
    if (index == 0) {
        res_err(res, 400, "?index=<n> is required\n");
        return;
    }

    /* Absent ?delay= means "use the group's backoff policy". */
    uint64_t delay = query_u64(req, "delay", UINT64_MAX);

    int found = 0;
    uint64_t retry_in = 0;
    int e = done ? bjm_done(a->store, subject, group, index, &found)
                 : bjm_fail(a->store, subject, group, index, delay,
                            &found, &retry_in);
    if (e) { res_err(res, status_for(e), "queue update failed\n"); return; }

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"subject", 7);
    bj_put_string(b, (const uint8_t *)subject, (uint32_t)strlen(subject));
    bj_put_key(b, (const uint8_t *)"group", 5);
    bj_put_string(b, (const uint8_t *)group, (uint32_t)strlen(group));
    bj_put_key(b, (const uint8_t *)"index", 5);
    bj_put_int(b, (int64_t)index);
    /*
     * 0 means the lease was not held — the job had already expired and
     * been handed to somebody else, or it was never leased at all. Worth
     * reporting rather than swallowing: it is how a worker learns it ran
     * past its lease and its result may be a duplicate.
     */
    bj_put_key(b, (const uint8_t *)"held", 4);
    bj_put_bool(b, found);
    if (!done) {
        bj_put_key(b, (const uint8_t *)"retry_in_ms", 11);
        bj_put_int(b, (int64_t)retry_in);
    }
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_done(http11c_request *req, http11c_response *res) {
    h_job_end(req, res, 1);
}

static void h_fail(http11c_request *req, http11c_response *res) {
    h_job_end(req, res, 0);
}

static void h_requeue(http11c_request *req, http11c_response *res) {
    app *a = http11c_req_ctx(req);

    const char *subject = subject_of(req, res, "/requeue/");
    if (!subject) return;

    uint64_t index = query_u64(req, "index", 0);
    if (index == 0) {
        res_err(res, 400, "?index=<n> is required (the index in "
                          "<subject>.dead, as shown by bjmsg dead)\n");
        return;
    }

    /* A dead-letter channel has no channel of its own, so there is
     * nothing here to requeue from. Say that rather than reporting a
     * missing index. */
    size_t slen = strlen(subject), dlen = sizeof BJM_DEAD_SUFFIX - 1;
    if (slen >= dlen && strcmp(subject + slen - dlen, BJM_DEAD_SUFFIX) == 0) {
        res_err(res, 400, "that is already a dead-letter channel; requeue "
                          "names the original subject\n");
        return;
    }

    uint64_t at = 0;
    int e = bjm_requeue(a->store, subject, index, &at);
    if (e == BJ_ERR_VERIFY) {
        res_err(res, 422, "that message is not a dead-letter envelope\n");
        return;
    }
    if (e == BJ_ERR_RANGE) {
        res_err(res, 416, "no such index in the dead-letter channel; "
                          "bjmsg dead lists them\n");
        return;
    }
    if (e) { res_err(res, status_for(e), "requeue failed\n"); return; }

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"subject", 7);
    bj_put_string(b, (const uint8_t *)subject, (uint32_t)strlen(subject));
    bj_put_key(b, (const uint8_t *)"from_dead_index", 15);
    bj_put_int(b, (int64_t)index);
    /* A new index, not the old one: the original is still in the log and
     * still dead, and reusing its id would be a lie about ordering. */
    bj_put_key(b, (const uint8_t *)"index", 5);
    bj_put_int(b, (int64_t)at);
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_queues(http11c_request *req, http11c_response *res) {
    app *a = http11c_req_ctx(req);

    const char *subject = subject_of(req, res, "/queue/");
    if (!subject) return;

    const uint8_t *out = NULL;
    size_t out_len = 0;
    int e = bjm_queues(a->store, subject, &out, &out_len);
    if (e) { res_err(res, status_for(e), "listing failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_queue_config(http11c_request *req, http11c_response *res) {
    app *a = http11c_req_ctx(req);

    const char *subject;
    char group[BJM_GROUP_MAX + 1];
    if (!queue_target(req, res, "/queue/", &subject, group, sizeof group)) return;

    uint64_t lease = query_u64(req, "lease_ms", BJM_LEASE_DEFAULT_MS);
    uint64_t attempts = query_u64(req, "max_attempts", BJM_MAX_ATTEMPTS_DEFAULT);
    uint64_t backoff = query_u64(req, "backoff_ms", BJM_BACKOFF_DEFAULT_MS);
    uint64_t max_backoff = query_u64(req, "max_backoff_ms",
                                     BJM_MAX_BACKOFF_DEFAULT_MS);
    int e = bjm_queue_config(a->store, subject, group, lease, attempts,
                             backoff, max_backoff);
    if (e) { res_err(res, status_for(e), "queue config failed\n"); return; }
    h_queues(req, res);
}

static void h_queue_delete(http11c_request *req, http11c_response *res) {
    app *a = http11c_req_ctx(req);

    const char *subject;
    char group[BJM_GROUP_MAX + 1];
    if (!queue_target(req, res, "/queue/", &subject, group, sizeof group)) return;

    int deleted = 0;
    int e = bjm_queue_delete(a->store, subject, group, &deleted);
    if (e) { res_err(res, status_for(e), "queue delete failed\n"); return; }

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"subject", 7);
    bj_put_string(b, (const uint8_t *)subject, (uint32_t)strlen(subject));
    bj_put_key(b, (const uint8_t *)"group", 5);
    bj_put_string(b, (const uint8_t *)group, (uint32_t)strlen(group));
    bj_put_key(b, (const uint8_t *)"deleted", 7);
    bj_put_bool(b, deleted);
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_policy_get(http11c_request *req, http11c_response *res) {
    app *a = http11c_req_ctx(req);

    const char *subject = subject_of(req, res, "/policy/");
    if (!subject) return;

    int found = 0;
    bjm_policy p;
    int e = bjm_policy_get(a->store, subject, &found, &p);
    if (e) { res_err(res, status_for(e), "policy lookup failed\n"); return; }

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"subject", 7);
    bj_put_string(b, (const uint8_t *)subject, (uint32_t)strlen(subject));
    bj_put_key(b, (const uint8_t *)"policy", 6);
    bj_put_bool(b, found);
    bj_put_key(b, (const uint8_t *)"max_age_s", 9);
    bj_put_int(b, (int64_t)p.max_age_s);
    bj_put_key(b, (const uint8_t *)"max_messages", 12);
    bj_put_int(b, (int64_t)p.max_messages);
    bj_put_key(b, (const uint8_t *)"max_bytes", 9);
    bj_put_int(b, (int64_t)p.max_bytes);
    bj_put_key(b, (const uint8_t *)"ignore_consumers", 16);
    bj_put_bool(b, p.ignore_consumers);
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_policy_put(http11c_request *req, http11c_response *res) {
    app *a = http11c_req_ctx(req);

    const char *subject = subject_of(req, res, "/policy/");
    if (!subject) return;

    /* Absent dimensions stay at 0, which is "unlimited" — a PUT replaces
     * the whole policy rather than patching it, so what you send is what
     * the subject has. */
    bjm_policy p = {
        .max_age_s        = query_u64(req, "max_age_s", 0),
        .max_messages     = query_u64(req, "max_messages", 0),
        .max_bytes        = query_u64(req, "max_bytes", 0),
        .ignore_consumers = query_u64(req, "ignore_consumers", 0) != 0,
    };
    if (!p.max_age_s && !p.max_messages && !p.max_bytes) {
        res_err(res, 400, "a policy needs at least one of max_age_s, "
                          "max_messages, max_bytes\n");
        return;
    }

    int e = bjm_policy_set(a->store, subject, &p);
    if (e) { res_err(res, status_for(e), "policy write failed\n"); return; }
    h_policy_get(req, res);
}

static void h_policy_delete(http11c_request *req, http11c_response *res) {
    app *a = http11c_req_ctx(req);

    const char *subject = subject_of(req, res, "/policy/");
    if (!subject) return;

    int cleared = 0;
    int e = bjm_policy_clear(a->store, subject, &cleared);
    if (e) { res_err(res, status_for(e), "policy delete failed\n"); return; }

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"subject", 7);
    bj_put_string(b, (const uint8_t *)subject, (uint32_t)strlen(subject));
    bj_put_key(b, (const uint8_t *)"cleared", 7);
    bj_put_bool(b, cleared);
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_policies(http11c_request *req, http11c_response *res) {
    app *a = http11c_req_ctx(req);
    const uint8_t *out = NULL;
    size_t out_len = 0;
    int e = bjm_policy_list(a->store, &out, &out_len);
    if (e) { res_err(res, status_for(e), "listing failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_subjects(http11c_request *req, http11c_response *res) {
    app *a = http11c_req_ctx(req);
    const uint8_t *out = NULL;
    size_t out_len = 0;
    int e = bjm_subjects(a->store, &out, &out_len);
    if (e) { res_err(res, status_for(e), "listing failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_health(http11c_request *req, http11c_response *res) {
    app *a = http11c_req_ctx(req);
    const char *backend = http11c_backend();

    int nsubjects = 0;
    bjm_subject_count(a->store, &nsubjects);

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"ok", 2);
    bj_put_bool(b, 1);
    bj_put_key(b, (const uint8_t *)"backend", 7);
    bj_put_string(b, (const uint8_t *)backend, (uint32_t)strlen(backend));
    bj_put_key(b, (const uint8_t *)"subjects", 8);
    bj_put_int(b, nsubjects);
    bj_put_key(b, (const uint8_t *)"connections", 11);
    bj_put_int(b, http11c_conn_count(a->srv));
    bj_put_key(b, (const uint8_t *)"uptime_s", 8);
    bj_put_int(b, (int64_t)(time(NULL) - a->started));
    bj_put_key(b, (const uint8_t *)"dedup_window_ms", 15);
    bj_put_int(b, (int64_t)bjm_dedup_window(a->store));
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_not_found(http11c_request *req, http11c_response *res) {
    (void)req;
    res_err(res, 404,
        "no such route. available:\n"
        "  POST   /pub/<subject>[?id=][&headers=1][&ack_subject=&ack_consumer=&ack_index=]\n"
        "  GET    /sub/<subject>?from=|consumer=\n"
        "  POST   /ack/<subject>?consumer=&index=\n"
        "  POST   /trim/<subject>?before=|keep=[&force=1]\n"
        "  POST   /take/<subject>?group=&max=&lease=\n"
        "  POST   /done/<subject>?group=&index=\n"
        "  POST   /fail/<subject>?group=&index=[&delay=]\n"
        "  POST   /requeue/<subject>?index=   (from <subject>.dead)\n"
        "  GET    /queue/<subject>\n"
        "  PUT    /queue/<subject>?group=&lease_ms=&max_attempts=&backoff_ms=\n"
        "  DELETE /queue/<subject>?group=\n"
        "  GET    /consumers/<subject>\n"
        "  DELETE /consumers/<subject>?consumer=\n"
        "  GET    /info/<subject>\n"
        "  GET    /subjects\n"
        "  GET    /health\n");
}

/* ---- lifecycle ------------------------------------------------------- */

static volatile sig_atomic_t g_running = 1;

static void on_signal(int sig) {
    (void)sig;
    g_running = 0;
    if (g_srv) http11c_stop(g_srv);
}

int bjm_serve(const char *host, int port, const char *dir,
              uint64_t dedup_window_ms) {
    app a = {0};
    a.store = bjm_store_open(dir);
    if (!a.store) {
        fprintf(stderr, "bjmsg: cannot open store at %s\n", dir);
        return 1;
    }
    if (dedup_window_ms) bjm_dedup_set_window(a.store, dedup_window_ms);
    a.bld = bj_builder_new();
    if (!a.bld) { bjm_store_free(a.store); return 1; }

    a.started = time(NULL);

    http11c_server *s = http11c_server_new();
    if (!s) { bj_builder_free(a.bld); bjm_store_free(a.store); return 1; }
    a.srv = s;

    http11c_set_ctx(s, &a);
    http11c_set_max_body(s, MAX_BODY_BYTES);
    http11c_set_idle_timeout(s, IDLE_TIMEOUT_SECS);

    http11c_route(s, "POST", "/pub/*", h_publish);
    http11c_route(s, "GET",  "/sub/*", h_subscribe);
    http11c_route(s, "POST", "/ack/*", h_ack);
    http11c_route(s, "POST", "/trim/*", h_trim);
    http11c_route(s, "POST", "/take/*", h_take);
    http11c_route(s, "POST", "/done/*", h_done);
    http11c_route(s, "POST", "/fail/*", h_fail);
    http11c_route(s, "POST", "/requeue/*", h_requeue);
    http11c_route(s, "GET",  "/queue/*", h_queues);
    http11c_route(s, "PUT",  "/queue/*", h_queue_config);
    http11c_route(s, "DELETE", "/queue/*", h_queue_delete);
    http11c_route(s, "GET",  "/consumers/*", h_consumers);
    http11c_route(s, "DELETE", "/consumers/*", h_unsubscribe);
    http11c_route(s, "GET",  "/info/*", h_info);
    http11c_route(s, "GET",  "/policy/*", h_policy_get);
    http11c_route(s, "PUT",  "/policy/*", h_policy_put);
    http11c_route(s, "DELETE", "/policy/*", h_policy_delete);
    http11c_route(s, "GET",  "/policies", h_policies);
    http11c_route(s, "GET",  "/subjects", h_subjects);
    http11c_route(s, "GET",  "/health", h_health);
    http11c_set_fallback(s, h_not_found);

    int rc = 0;
    if (http11c_listen(s, host, port) != 0) {
        fprintf(stderr, "bjmsg: cannot listen on %s:%d\n", host, port);
        rc = 1;
        goto done;
    }

    g_srv = s;
    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGPIPE, SIG_IGN);

    fprintf(stderr, "bjmsg: serving %s on http://%s:%d (%s)\n",
            dir, host, http11c_port(s), http11c_backend());

    /*
     * Our own loop rather than http11c_run, so retention has somewhere to
     * happen. http11c_poll returns after at most a second, which bounds
     * how late a sweep can be; the sweep itself is skipped entirely when
     * no subject has a policy.
     */
    time_t last_sweep = time(NULL);
    while (g_running) {
        if (http11c_poll(s, 1000) < 0) { rc = 1; break; }

        time_t now = time(NULL);
        if (now - last_sweep < RETENTION_SWEEP_SECS) continue;
        last_sweep = now;

        uint64_t removed = 0;
        int trimmed = 0;
        int e = bjm_retention_run(a.store, (uint64_t)now, &removed, &trimmed);
        if (e)
            fprintf(stderr, "bjmsg: retention sweep failed (%d)\n", e);
        else if (removed)
            fprintf(stderr, "bjmsg: retention removed %llu message(s) "
                            "from %d subject(s)\n",
                    (unsigned long long)removed, trimmed);
    }
    fprintf(stderr, "bjmsg: stopped\n");

done:
    g_srv = NULL;
    http11c_server_free(s);
    bj_builder_free(a.bld);
    bjm_store_free(a.store);
    return rc;
}
