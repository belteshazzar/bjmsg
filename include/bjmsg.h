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

/*
 * 1 when `data` is shaped like a headers envelope: a two-element array
 * whose first element is an object. Only the shape — the headers
 * themselves are the publisher's business.
 */
int bjm_envelope_shape_ok(const uint8_t *data, size_t len);

/*
 * Split [ headers, message ] into its two encoded parts, which point
 * into `data`. Returns 0 if it is not that shape.
 */
int bjm_envelope_split(const uint8_t *data, size_t len,
                       const uint8_t **headers, size_t *headers_len,
                       const uint8_t **message, size_t *message_len);

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
 * Subject patterns, matched token-wise on '.' — no regular expressions:
 *
 *   orders.*          one token here      → orders.us, not orders.us.new
 *   orders.>          this and everything below → orders.us, orders.us.new
 *   orders.*.created  mixed
 *
 * '*' and '>' are not legal in a subject name, so a pattern can never be
 * mistaken for one and nothing has to flag which it is.
 */
int bjm_pattern_valid(const char *s);
int bjm_pattern_is(const char *s);      /* 1 if it contains a wildcard */
int bjm_pattern_match(const char *pattern, const char *subject);

/*
 * Message shapes, carried in the entry log's own type byte — which is
 * already in every subscribe response, so a subscriber can tell them
 * apart without the broker unwrapping anything.
 *
 * A message with headers is stored as the two-element binjson ARRAY
 * [ headers, message ]. That costs nothing for the headerless case,
 * which stays byte-for-byte what the publisher sent, and it keeps the
 * subscribe path forwarding elog_get_batch's output untouched. Headers
 * are opaque to the broker: it validates the shape and nothing else.
 */
#define BJM_ENTRY_PLAIN    0x01   /* EL_NORMAL: payload is the message   */
#define BJM_ENTRY_ENVELOPE 0x10   /* payload is [ headers, message ]     */

/*
 * Append `payload` to `subject` and make it durable before returning —
 * one fsync per publish. Writes the assigned index through *out_index.
 * Returns BJ_OK or a negative BJ_ERR_* code.
 */
int bjm_publish(bjm_store *st, const char *subject, int entry_type,
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
int bjm_subjects(bjm_store *st, const char *pattern,
                 const uint8_t **out, size_t *out_len);

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
 * Forget every receipt this consumer holds, on any subject, reporting how
 * many through *deleted. A receipt holds back retention, so a throwaway
 * subscription that merely stopped delivering would otherwise pin the log
 * it was reading for good.
 */
int bjm_cursor_delete_consumer(bjm_store *st, const char *consumer,
                               int *deleted);

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

/* ---- push subscriptions ------------------------------------------------ */

/*
 * Messages are pushed, never polled. A subscriber registers a callback —
 * a URL the broker can reach — and the broker POSTs each batch to it; the
 * HTTP response *is* the acknowledgement, so delivery and ack are one
 * round trip on one kept-alive connection, and a subscriber that is busy
 * simply does not answer yet, which is all the backpressure there is to
 * apply.
 *
 * The subscription holds no cursor. How far the consumer has read is its
 * ordinary receipt in the cursor tree, which is why a push subscription
 * shows up in `bjmsg consumers`, survives the broker restarting, and
 * counts against retention exactly like any other subscription.
 */
#define BJM_CALLBACK_MAX 512
#define BJM_TOKEN_MAX    128

typedef struct {
    char     pattern[BJM_SUBJECT_MAX + 1];  /* subject or wildcard pattern */
    char     callback[BJM_CALLBACK_MAX + 1];
    char     token[BJM_TOKEN_MAX + 1];      /* "" for none; sent as Bearer */
    uint64_t batch_bytes;                   /* 0 = the broker's default    */
} bjm_push_sub;

int bjm_callback_valid(const char *url);
int bjm_token_valid(const char *s);

int bjm_push_get(bjm_store *st, const char *consumer, int *found,
                 bjm_push_sub *out);
int bjm_push_set(bjm_store *st, const char *consumer, const bjm_push_sub *s);
int bjm_push_delete(bjm_store *st, const char *consumer, int *deleted);

/*
 * Visit every registered subscription; `fn` returning non-zero stops the
 * walk. It must not call back into the push registry, whose reads share
 * one output buffer with this scan.
 */
int bjm_push_each(bjm_store *st,
                  int (*fn)(void *ctx, const char *consumer,
                            const bjm_push_sub *s),
                  void *ctx);

/*
 * Called after every append is durable, whichever route made it. The
 * delivery engine listens here rather than on the publish handler, so a
 * message that arrives by requeue or by dead-lettering wakes subscribers
 * the same way an ordinary publish does.
 */
void bjm_store_on_publish(bjm_store *st,
                          void (*cb)(void *ctx, const char *subject,
                                     uint64_t index),
                          void *ctx);

/* ---- the delivery engine ----------------------------------------------- */

/*
 * The broker's outbound half: one libcurl multi handle driving at most
 * one delivery per subscription, pumped from the same loop that serves
 * requests. Nothing here blocks.
 */
typedef struct bjm_pusher bjm_pusher;

bjm_pusher *bjm_pusher_new(bjm_store *st, uint64_t default_batch_bytes);
void        bjm_pusher_free(bjm_pusher *p);
int         bjm_pusher_count(const bjm_pusher *p);

int  bjm_pusher_add(bjm_pusher *p, const char *consumer,
                    const bjm_push_sub *cfg);
void bjm_pusher_remove(bjm_pusher *p, const char *consumer);

/*
 * Write the receipts a brand-new subscription implies, so that where it
 * starts is decided once instead of on every delivery: `at_end` skips
 * whatever already exists, `from` starts at a chosen index, and the
 * default replays everything still in the log. Subjects that already have
 * a receipt for this consumer keep it.
 */
int bjm_pusher_seed(bjm_pusher *p, const char *consumer, const char *pattern,
                    int at_end, uint64_t from);

/* A message landed on `subject`: wake anything subscribed to it. */
void bjm_pusher_notify(bjm_pusher *p, const char *subject);

/*
 * Start whatever deliveries are due and service the ones in flight.
 * Returns how many milliseconds the caller may block before pumping
 * again — pass it to http11c_poll.
 */
int bjm_pusher_pump(bjm_pusher *p, uint64_t now_ms);

/* Live state of every subscription, as a binjson ARRAY of objects. */
int bjm_pusher_list(bjm_pusher *p, const uint8_t **out, size_t *out_len);

uint64_t bjm_now_ms(void);

/* ---- producer idempotency ---------------------------------------------- */

/*
 * A publish carrying `?id=` is deduplicated: if that id was published to
 * the subject recently, the original index is returned and nothing is
 * appended. That is what makes POST /pub safe to retry — without it a
 * publish that broke mid-flight might already be in the log, and
 * retrying would append it twice.
 *
 * "Recently" is a bounded window, implemented as two generations of the
 * dedup index. Writes go to the current one, lookups check both, and
 * every `window` the older is cleared in O(1) (bpt_reset truncates an
 * append-only file) and becomes the new current. So an id is remembered
 * for at least `window` and at most twice that — bounded space with no
 * deletes to accumulate and no compaction to schedule.
 */
#define BJM_DEDUP_ID_MAX 128
#define BJM_DEDUP_WINDOW_DEFAULT_MS 120000

/* Ids are opaque to the broker; they only may not contain '/', which
 * separates subject from id in the index key. */
int bjm_dedup_id_valid(const char *s);

void     bjm_dedup_set_window(bjm_store *st, uint64_t ms);
uint64_t bjm_dedup_window(const bjm_store *st);

/* *found is 1 when this id was already published, with its index. */
int bjm_dedup_lookup(bjm_store *st, const char *subject, const char *id,
                     int *found, uint64_t *index);
/* Remember `id` as having produced `index`. */
int bjm_dedup_record(bjm_store *st, const char *subject, const char *id,
                     uint64_t index);

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
#define BJM_BACKOFF_DEFAULT_MS 1000
/*
 * Where a job goes when it runs out of attempts: an ordinary subject
 * named "<subject>.dead", so every tool that works on a subject works on
 * the dead-letter channel too — inspect it, set a retention policy on
 * it, even consume it with its own queue group.
 *
 * The message there is an envelope, { subject, group, index, attempts,
 * failed_ms, payload }, because the original payload alone does not say
 * which group gave up on it or how many times it was tried.
 */
#define BJM_DEAD_SUFFIX ".dead"
#define BJM_MAX_BACKOFF_DEFAULT_MS 300000

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
/*
 * Give a job back rather than waiting for its lease to expire. It becomes
 * available again after a delay: `delay_ms` if given, or UINT64_MAX to
 * use the group's backoff policy, which doubles the wait with each
 * attempt. *retry_in_ms reports what was applied.
 *
 * Some delay is the point. A job failed with no delay is due instantly,
 * and the worker that just failed it is the one most likely to ask next
 * — so it takes the same job straight back, and a permanently-failing
 * job spins as fast as the network allows instead of retrying.
 */
int bjm_fail(bjm_store *st, const char *subject, const char *group,
             uint64_t index, uint64_t delay_ms, int *found,
             uint64_t *retry_in_ms);

/*
 * `max_attempts` bounds redelivery: a job that has been handed out that
 * many times without finishing is dropped from the queue and counted as
 * dead rather than handed out again. 0 retries forever, which lets one
 * permanently-failing job starve the queue.
 */
int bjm_queue_config(bjm_store *st, const char *subject, const char *group,
                     uint64_t lease_ms, uint64_t max_attempts,
                     uint64_t backoff_ms, uint64_t max_backoff_ms);
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

/*
 * Republish a dead-lettered message back to the subject it came from.
 * `dlq_index` indexes "<subject>.dead"; the original subject is read from
 * the envelope rather than assumed. *new_index is where it landed.
 */
int bjm_requeue(bjm_store *st, const char *subject, uint64_t dlq_index,
                uint64_t *new_index);

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

int bjm_serve(const char *host, int port, const char *dir,
              uint64_t dedup_window_ms);

/* ---- clients --------------------------------------------------------- */

int bjm_cmd_pub(int argc, char **argv);
int bjm_cmd_sub(int argc, char **argv);

/*
 * Query commands: connect to a broker, ask one question, print the
 * answer, exit. They neither publish, subscribe, nor serve.
 */
int bjm_cmd_push(int argc, char **argv);
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
int bjm_cmd_dead(int argc, char **argv);
int bjm_cmd_requeue(int argc, char **argv);
int bjm_cmd_pipe(int argc, char **argv);
int bjm_cmd_request(int argc, char **argv);
int bjm_cmd_reply(int argc, char **argv);

/* ---- rendering ------------------------------------------------------- */

/*
 * Write the binjson value at data[0..len) to `f` as JSON-ish text. Returns
 * BJ_OK or a negative BJ_ERR_* code from the decoder.
 */
int bjm_render(FILE *f, const uint8_t *data, size_t len);

#endif /* BJMSG_H */
