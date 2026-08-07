/*
 * bjmsg.h — internal interfaces of the bjmsg executable.
 *
 * One binary, three roles: `bjmsg serve` runs the broker on http11c,
 * `bjmsg pub` and `bjmsg sub` are libcurl clients that speak the same
 * HTTP/1.1 + binjson protocol over a kept-alive connection.
 */
#ifndef BJMSG_H
#define BJMSG_H

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>

#include "binjson.h"

/* The media type carried by every request and success-response body. */
#define BJMSG_MEDIA_TYPE "application/binjson"

/*
 * bj_decode calls every visitor callback unconditionally — a NULL member
 * is a segfault, not "skip this type". Start from this and override only
 * the callbacks you care about.
 */
bj_visitor bjm_visitor_noop(void *ctx);

/* ---- subject store (server side) ------------------------------------- */

/*
 * A directory of per-subject entry logs. Each subject is one elog file
 * named "<subject>.elog": message ids are the log's own contiguous
 * indexes, and payloads are opaque bytes the log never interprets.
 */
typedef struct bjm_store bjm_store;

/* Open (creating if needed) the store rooted at `dir`. NULL on failure. */
bjm_store *bjm_store_open(const char *dir);
void bjm_store_free(bjm_store *st);

/*
 * Subject names are file names, so they are restricted to [A-Za-z0-9_.-],
 * 1..BJM_SUBJECT_MAX bytes, with no leading/trailing '.' and no "..".
 * Returns 1 if `s` is a legal subject.
 */
#define BJM_SUBJECT_MAX 128
int bjm_subject_valid(const char *s);

/*
 * Append `payload` to `subject` and make it durable before returning —
 * one fsync per publish. Writes the assigned index through *out_index.
 * Returns BJ_OK or a negative BJ_ERR_* code.
 */
int bjm_publish(bjm_store *st, const char *subject,
                const uint8_t *payload, uint32_t len, uint64_t *out_index);

/*
 * Read messages from `from` (0 and 1 both mean "from the start") until
 * roughly `max_bytes` of payload is gathered. On BJ_OK, out/out_len expose
 * a binjson ARRAY of { index, term, type, payload } — the entry log's own
 * batch encoding, forwarded to the subscriber verbatim. Those bytes are
 * owned by the store and valid until the next call on the same subject.
 */
int bjm_read(bjm_store *st, const char *subject, uint64_t from,
             size_t max_bytes, int *count,
             const uint8_t **out, size_t *out_len, uint64_t *last_index);

/*
 * Encode the store's subjects as a binjson ARRAY of strings, read from the
 * directory so the answer includes subjects this process has never opened.
 * Bytes are owned by the store and valid until the next call.
 */
int bjm_subjects(bjm_store *st, const uint8_t **out, size_t *out_len);

/* ---- durable subscriptions (read receipts) ---------------------------- */

/*
 * A named consumer's receipt for a subject: the highest index it has
 * acknowledged. Held in one B+ tree for the whole store, keyed
 * "<subject>/<consumer>" — neither name may contain '/', so the key is
 * unambiguous and one subject's consumers form a contiguous range.
 *
 * Delivery is at-least-once. An ack commits (CRC-protected, so it
 * survives the process dying) but is not fsynced, because the cost of a
 * second fsync per batch buys only the difference between "redelivered
 * after a power cut" and "not redelivered" — and a subscriber must
 * tolerate redelivery either way. bjm_cursor_sync forces it where that
 * matters.
 */
#define BJM_CONSUMER_MAX 128
int bjm_consumer_valid(const char *s);

/* *found is 0 when the consumer has never acked this subject. */
int bjm_cursor_get(bjm_store *st, const char *subject, const char *consumer,
                   int *found, uint64_t *index);
/* Upsert the receipt. Never moves a cursor backwards. */
int bjm_cursor_set(bjm_store *st, const char *subject, const char *consumer,
                   uint64_t index);
int bjm_cursor_sync(bjm_store *st);

/*
 * Every consumer of `subject` as a binjson ARRAY of
 * { consumer, acked, lag } objects. Bytes are owned by the store and
 * valid until the next call.
 */
int bjm_consumers(bjm_store *st, const char *subject,
                  const uint8_t **out, size_t *out_len);

/* Forget a subscription. *deleted is 0 when there was nothing to remove. */
int bjm_cursor_delete(bjm_store *st, const char *subject,
                      const char *consumer, int *deleted);

/*
 * How many consumers `subject` has and the lowest index any of them has
 * acknowledged — the boundary below which trimming discards messages
 * somebody is still entitled to. *min_acked is UINT64_MAX when there are
 * no consumers at all.
 */
int bjm_consumer_stats(bjm_store *st, const char *subject,
                       int *count, uint64_t *min_acked);

/* Highest index currently in `subject`, or 0 if it does not exist. */
uint64_t bjm_last_index(bjm_store *st, const char *subject);

/* ---- queue groups (competing consumers) -------------------------------- */

/*
 * A queue group turns a subject into a job queue: each message goes to
 * exactly one member of the group rather than to all of them.
 *
 * A read receipt cannot express this, because completion is out of order
 * — worker A can still be on job 5 when worker B finishes job 6, and no
 * single high-water mark says that. The state is therefore two things:
 *
 *   next      the lowest index never yet handed out
 *   inflight  index -> (lease expiry, attempts) for jobs handed out but
 *             not yet finished
 *
 * Which is sufficient, and cheaper than it looks: below `next` a job is
 * either in the inflight table or it is done, so "done" needs no storage
 * at all — it is the absence of an entry. The table is bounded by how
 * many jobs are being worked on at once, never by how many have run.
 *
 * A lease that expires makes its job available again, so a worker that
 * dies loses nothing. That is at-least-once: the broker cannot tell a
 * dead worker from a slow one, so a slow job is run twice and jobs must
 * be idempotent. A group with lease_ms == 0 opts out — take advances
 * `next` and records nothing, which is at-most-once and loses the job if
 * the worker dies.
 */
#define BJM_GROUP_MAX 128
#define BJM_INFLIGHT_MAX 256
#define BJM_LEASE_DEFAULT_MS 30000
#define BJM_DEAD_MAX 64
#define BJM_MAX_ATTEMPTS_DEFAULT 10

int bjm_group_valid(const char *s);

/*
 * Lease up to `max` jobs to a caller. Expired leases are redelivered
 * before untouched messages are handed out. On BJ_OK, out/out_len expose
 * a binjson ARRAY of { index, attempts, expires_ms, payload }; those
 * bytes are owned by the store and valid until the next call.
 */
int bjm_take(bjm_store *st, const char *subject, const char *group,
             int max, uint64_t lease_ms, int *count,
             const uint8_t **out, size_t *out_len);

/* Finish a job: drop its lease permanently. *found is 0 if it was not
 * leased (an expired-and-retaken job, or a bad index). */
int bjm_done(bjm_store *st, const char *subject, const char *group,
             uint64_t index, int *found);
/* Give a job back now rather than waiting for its lease to expire. */
int bjm_fail(bjm_store *st, const char *subject, const char *group,
             uint64_t index, int *found);

/*
 * `max_attempts` bounds redelivery: a job that has been handed out that
 * many times without finishing is dropped from the queue and counted as
 * dead rather than handed out again. 0 retries forever, which lets one
 * permanently-failing job starve the queue.
 */
int bjm_queue_config(bjm_store *st, const char *subject, const char *group,
                     uint64_t lease_ms, uint64_t max_attempts);
int bjm_queue_delete(bjm_store *st, const char *subject, const char *group,
                     int *deleted);
int bjm_queues(bjm_store *st, const char *subject,
               const uint8_t **out, size_t *out_len);

/*
 * The lowest index any group still owes to a worker, minus one — the
 * boundary below which trimming would destroy unrun jobs. *ngroups is 0
 * when the subject has no queue groups.
 */
int bjm_queue_floor(bjm_store *st, const char *subject,
                    uint64_t *floor, int *ngroups);

/* ---- inspection ------------------------------------------------------- */

/*
 * `base` is the trim boundary: the log holds (base, last], so the oldest
 * readable message is base + 1. `bytes` is the backing file's size.
 */
int bjm_subject_info(bjm_store *st, const char *subject,
                     uint64_t *base, uint64_t *last, uint64_t *bytes);

int bjm_subject_count(bjm_store *st, int *count);

/* ---- retention policy -------------------------------------------------- */

/*
 * A subject's retention policy. Any dimension set to 0 is unlimited, and
 * several may be set at once — each yields a trim boundary and the
 * tightest one wins, so whichever limit is reached first is the one that
 * takes effect.
 *
 * `ignore_consumers` decides what happens when retention and a read
 * receipt disagree. By default retention loses: it will not discard a
 * message a subscription has not read, which is safe but lets one
 * forgotten consumer defeat retention entirely. Setting it makes the
 * policy authoritative — the point of retention is a bound that holds.
 */
typedef struct {
    uint64_t max_age_s;      /* discard messages older than this      */
    uint64_t max_messages;   /* keep at most this many                */
    uint64_t max_bytes;      /* keep the log under this size          */
    int      ignore_consumers;
} bjm_policy;

int bjm_policy_get(bjm_store *st, const char *subject, int *found,
                   bjm_policy *out);
int bjm_policy_set(bjm_store *st, const char *subject, const bjm_policy *p);
int bjm_policy_clear(bjm_store *st, const char *subject, int *cleared);

/* Every subject with a policy, as a binjson ARRAY of objects. */
int bjm_policy_list(bjm_store *st, const uint8_t **out, size_t *out_len);

/*
 * Apply every policy. Meant to be called periodically by the broker; safe
 * to call at any time. Reports what it did through *removed / *trimmed.
 */
int bjm_retention_run(bjm_store *st, uint64_t now,
                      uint64_t *removed, int *trimmed);

/* ---- retention -------------------------------------------------------- */

/*
 * Discard every message with an index below `before`, rewriting the log
 * without them. Returns the number dropped through *removed and the new
 * base index through *out_base.
 *
 * Trimming past a consumer's receipt destroys messages it has not read,
 * so `force` is required to go below the lowest receipt; without it the
 * boundary is clamped to what every consumer has already acknowledged.
 */
int bjm_trim(bjm_store *st, const char *subject, uint64_t before, int force,
             uint64_t *out_base, uint64_t *removed);

/* ---- server ---------------------------------------------------------- */

int bjm_serve(const char *host, int port, const char *dir);

/* ---- clients --------------------------------------------------------- */

int bjm_cmd_pub(int argc, char **argv);
int bjm_cmd_sub(int argc, char **argv);

/*
 * Query commands: connect to a broker, ask one question, print the
 * answer, exit. They neither publish, subscribe, nor serve.
 */
int bjm_cmd_consumers(int argc, char **argv);
int bjm_cmd_unsubscribe(int argc, char **argv);
int bjm_cmd_subjects(int argc, char **argv);
int bjm_cmd_info(int argc, char **argv);
int bjm_cmd_health(int argc, char **argv);
int bjm_cmd_trim(int argc, char **argv);
int bjm_cmd_seek(int argc, char **argv);
int bjm_cmd_policy(int argc, char **argv);
int bjm_cmd_queue(int argc, char **argv);
int bjm_cmd_take(int argc, char **argv);
int bjm_cmd_done(int argc, char **argv);
int bjm_cmd_fail(int argc, char **argv);
int bjm_cmd_work(int argc, char **argv);

/* ---- rendering ------------------------------------------------------- */

/*
 * Write the binjson value at data[0..len) to `f` as JSON-ish text. Returns
 * BJ_OK or a negative BJ_ERR_* code from the decoder.
 */
int bjm_render(FILE *f, const uint8_t *data, size_t len);

#endif /* BJMSG_H */
