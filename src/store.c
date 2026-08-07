/*
 * store.c — the subject store: a directory of per-subject entry logs.
 *
 * A subject is one elog file. That gives us, for free, everything a
 * message log needs and nothing it does not: contiguous auto-assigned
 * indexes (the message ids), opaque payloads the log never interprets,
 * batched commits with a CRC trailer, torn-tail recovery on open, and
 * elog_get_batch's binjson encoding of { index, term, type, payload },
 * which is exactly the wire form a subscriber wants — so the subscribe
 * path forwards those bytes without re-encoding them.
 *
 * Raft's term is unused here: there is no election, so every entry is
 * appended at term 0, which is what elog's monotonicity rule (term >=
 * last_term and <= current_term, both 0) permits.
 */
#include "bjmsg.h"

#include "binjson.h"
#include "bjio.h"
#include "bjio_posix.h"
#include "bjns.h"
#include "bplustree.h"
#include "entrylog.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define SUBJECT_SUFFIX ".elog"

/* Not a legal subject name (subjects cannot start with '_'... they can,
 * but they always end in .elog, and bjm_subjects filters on that). */
#define CURSORS_FILE "_cursors.bpt"
#define POLICY_FILE  "_policy.bpt"
#define QUEUES_FILE  "_queues.bpt"
#define DEDUP_FILE_0 "_dedup0.bpt"
#define DEDUP_FILE_1 "_dedup1.bpt"
#define CURSORS_ORDER 64

/*
 * Age-based retention needs to know when a message was published, and the
 * entry log stores no timestamps — payloads are opaque and the format is
 * not ours to extend. So the store keeps its own sparse index: a bounded
 * ring of (index, time) marks per subject, one written every
 * max_age / BJM_MARKS_PER_WINDOW seconds.
 *
 * Sparse is the point. Exact per-message times would be another
 * write-amplifying index on the publish path, to answer a question
 * ("roughly how old is message N") that tolerates being approximate:
 * with marks this coarse, a trim keeps at most one interval's worth of
 * messages longer than asked. Erring towards keeping is the safe
 * direction, and BJM_MARKS_MAX covers two full windows so a policy that
 * is later lengthened still has history to work from.
 */
#define BJM_MARKS_MAX 256
#define BJM_MARKS_PER_WINDOW 128

typedef struct {
    char  name[BJM_SUBJECT_MAX + 1];
    bj_io io;
    elog *log;
    bjm_policy pol;
    int   pol_loaded;      /* pol is current; cleared when a policy changes */
    uint64_t last_mark_t;
} subject;

struct bjm_store {
    int         dirfd;
    bj_ns       ns;
    subject    *subs;
    size_t      nsubs, cap;
    bj_builder *bld;      /* scratch for bjm_subjects / bjm_consumers */
    bj_builder *cbld;     /* scratch for cursor and policy values     */
    bj_builder *dbld;     /* scratch for dead-letter envelopes        */
    bj_io       cursors_io;
    bpt        *cursors;  /* opened on first use */
    bj_io       policy_io;
    bpt        *policy;   /* opened on first use */
    bj_io       queues_io;
    bpt        *queues;   /* opened on first use */
    struct qrec *qscratch; /* one decoded queue record, reused */
    bj_io       dd_io[2];
    bpt        *dd[2];     /* two generations of the dedup index */
    int         dd_gen;    /* which of dd[] is current           */
    int         dd_open;
    uint64_t    dd_rotated_ms;
    uint64_t    dd_window_ms;
};

/* ---- names ----------------------------------------------------------- */

static int name_valid(const char *s, size_t max) {
    if (!s) return 0;
    size_t n = strlen(s);
    if (n == 0 || n > max) return 0;
    if (s[0] == '.' || s[n - 1] == '.') return 0;
    for (size_t i = 0; i < n; i++) {
        char c = s[i];
        int ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                 (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.';
        if (!ok) return 0;
        if (c == '.' && s[i + 1] == '.') return 0;
    }
    return 1;
}

int bjm_subject_valid(const char *s)  { return name_valid(s, BJM_SUBJECT_MAX); }
int bjm_consumer_valid(const char *s) { return name_valid(s, BJM_CONSUMER_MAX); }

/* Defined with the rest of the retention machinery, used on the publish
 * path above it. */
static void mark_publish(bjm_store *st, subject *s, uint64_t index);
static void marks_prune(bjm_store *st, const char *subject, uint64_t base);

/* Wall-clock milliseconds; defined with the queue machinery that needs it
 * most, but the dedup window is measured in it too. */
static uint64_t now_ms(void);

/* ---- open / close ---------------------------------------------------- */

bjm_store *bjm_store_open(const char *dir) {
    if (mkdir(dir, 0755) != 0 && errno != EEXIST) return NULL;

    int dirfd = open(dir, O_RDONLY);
    if (dirfd < 0) return NULL;

    bjm_store *st = calloc(1, sizeof *st);
    if (!st) { close(dirfd); return NULL; }
    st->dirfd = dirfd;

    if (bjns_posix_open(dirfd, &st->ns) != BJ_OK) {
        free(st); close(dirfd); return NULL;
    }
    st->bld = bj_builder_new();
    st->cbld = bj_builder_new();
    st->dbld = bj_builder_new();
    if (!st->bld || !st->cbld || !st->dbld) {
        bj_builder_free(st->bld); bj_builder_free(st->cbld);
        bj_builder_free(st->dbld);
        bjns_posix_free(&st->ns); free(st); close(dirfd); return NULL;
    }
    return st;
}

void bjm_store_free(bjm_store *st) {
    if (!st) return;
    for (size_t i = 0; i < st->nsubs; i++) {
        elog_free(st->subs[i].log);
        st->ns.close(st->ns.ctx, &st->subs[i].io);
    }
    free(st->subs);
    if (st->cursors) {
        bpt_sync(st->cursors);       /* the one durability point for acks */
        bpt_free(st->cursors);
        st->ns.close(st->ns.ctx, &st->cursors_io);
    }
    if (st->policy) {
        bpt_sync(st->policy);
        bpt_free(st->policy);
        st->ns.close(st->ns.ctx, &st->policy_io);
    }
    if (st->queues) {
        bpt_sync(st->queues);
        bpt_free(st->queues);
        st->ns.close(st->ns.ctx, &st->queues_io);
    }
    for (int i = 0; i < 2; i++) {
        if (!st->dd[i]) continue;
        bpt_free(st->dd[i]);
        st->ns.close(st->ns.ctx, &st->dd_io[i]);
    }
    free(st->qscratch);
    bj_builder_free(st->bld);
    bj_builder_free(st->cbld);
    bj_builder_free(st->dbld);
    bjns_posix_free(&st->ns);
    close(st->dirfd);
    free(st);
}

/*
 * Find `name`'s log, opening or creating it on first use. Subjects are
 * created implicitly by the first publish or subscribe, so `create`
 * distinguishes the two: a subscribe to an unknown subject must not
 * conjure a file.
 */
static int subject_get(bjm_store *st, const char *name, int create, subject **out) {
    for (size_t i = 0; i < st->nsubs; i++)
        if (strcmp(st->subs[i].name, name) == 0) { *out = &st->subs[i]; return BJ_OK; }

    char file[BJM_SUBJECT_MAX + sizeof SUBJECT_SUFFIX];
    int n = snprintf(file, sizeof file, "%s%s", name, SUBJECT_SUFFIX);
    if (n < 0 || (size_t)n >= sizeof file) return BJ_ERR_RANGE;

    bj_io io;
    int e = st->ns.open(st->ns.ctx, file, (uint32_t)n,
                        create ? BJ_NS_CREATE : 0, &io);
    if (e) return e;

    int fresh = io.size(io.ctx) == 0;
    elog *log = fresh ? elog_create(&io) : elog_open(&io);
    if (!log) { st->ns.close(st->ns.ctx, &io); return BJ_ERR_STATE; }
    /* A file whose bytes are fsynced but whose directory entry is not can
     * vanish across a crash, so pay for the directory sync once, here. */
    if (fresh) st->ns.sync(st->ns.ctx);

    if (st->nsubs == st->cap) {
        size_t cap = st->cap ? st->cap * 2 : 8;
        subject *p = realloc(st->subs, cap * sizeof *p);
        if (!p) { elog_free(log); st->ns.close(st->ns.ctx, &io); return BJ_ERR_OOM; }
        st->subs = p;
        st->cap = cap;
    }
    subject *s = &st->subs[st->nsubs++];
    memset(s, 0, sizeof *s);
    snprintf(s->name, sizeof s->name, "%s", name);
    s->io = io;
    s->log = log;
    *out = s;
    return BJ_OK;
}

/* ---- publish / read -------------------------------------------------- */

int bjm_publish(bjm_store *st, const char *subject_name, int entry_type,
                const uint8_t *payload, uint32_t len, uint64_t *out_index) {
    subject *s;
    int e = subject_get(st, subject_name, 1, &s);
    if (e) return e;

    e = elog_append(s->log, 0, entry_type, payload, len, out_index);
    if (e) return e;
    /*
     * One fsync per publish. elog_append only buffers, so a batching
     * broker would append many entries and sync once — but the sync has
     * to happen before the 200 goes out, and http11c serializes the
     * response the moment this handler returns. Batching therefore waits
     * on deferred responses; see README.
     */
    e = elog_sync(s->log);
    if (e) return e;

    /* Only does anything for a subject with an age policy, and then only
     * once per mark interval. */
    mark_publish(st, s, *out_index);
    return BJ_OK;
}

int bjm_read(bjm_store *st, const char *subject_name, uint64_t from,
             size_t max_bytes, int *count,
             const uint8_t **out, size_t *out_len, uint64_t *last_index) {
    subject *s;
    int e = subject_get(st, subject_name, 0, &s);
    if (e) return e;

    if (from == 0) from = 1;             /* indexes start at 1 */
    if (last_index) *last_index = elog_last_index(s->log);
    /* from > last_index yields an empty ARRAY with count 0, which is the
     * "caught up" answer — not an error. */
    return elog_get_batch(s->log, from, max_bytes, count, out, out_len);
}

uint64_t bjm_last_index(bjm_store *st, const char *subject_name) {
    subject *s;
    if (subject_get(st, subject_name, 0, &s) != BJ_OK) return 0;
    return elog_last_index(s->log);
}

/* ---- durable subscriptions (read receipts) --------------------------- */

/*
 * The cursor tree is opened on first use, so a store nobody has acked
 * against never grows a _cursors.bpt at all.
 */
static int cursors_open(bjm_store *st) {
    if (st->cursors) return BJ_OK;

    bj_io io;
    int e = st->ns.open(st->ns.ctx, CURSORS_FILE, sizeof CURSORS_FILE - 1,
                        BJ_NS_CREATE, &io);
    if (e) return e;

    int fresh = io.size(io.ctx) == 0;
    bpt *t = fresh ? bpt_create(&io, CURSORS_ORDER) : bpt_open(&io);
    if (!t) { st->ns.close(st->ns.ctx, &io); return BJ_ERR_STATE; }
    if (fresh) st->ns.sync(st->ns.ctx);

    st->cursors_io = io;
    st->cursors = t;
    return BJ_OK;
}

/* "<subject>/<consumer>" — '/' is legal in neither name, so this parses
 * back unambiguously and one subject's consumers sort together. */
static int cursor_key(char *buf, size_t cap, const char *subject,
                      const char *consumer, bpt_key *key) {
    int n = snprintf(buf, cap, "%s/%s", subject, consumer ? consumer : "");
    if (n < 0 || (size_t)n >= cap) return BJ_ERR_RANGE;
    key->is_string = 1;
    key->num = 0;
    key->str = (const uint8_t *)buf;
    key->str_len = (uint32_t)n;
    return BJ_OK;
}

/* Pull the first INT out of an encoded value. */
typedef struct { uint64_t v; int seen; } int_grab;

static void grab_int(void *ctx, double v) {
    int_grab *g = ctx;
    if (!g->seen) { g->v = (uint64_t)v; g->seen = 1; }
}

static int decode_index(const uint8_t *data, size_t len, uint64_t *out) {
    int_grab g = {0, 0};
    bj_visitor v = bjm_visitor_noop(&g);
    v.on_int = grab_int;
    int e = bj_decode(data, len, &v, NULL);
    if (e) return e;
    if (!g.seen) return BJ_ERR_VERIFY;
    *out = g.v;
    return BJ_OK;
}

int bjm_cursor_get(bjm_store *st, const char *subject, const char *consumer,
                   int *found, uint64_t *index) {
    *found = 0;
    *index = 0;
    int e = cursors_open(st);
    if (e) return e;

    char buf[BJM_SUBJECT_MAX + BJM_CONSUMER_MAX + 2];
    bpt_key key;
    e = cursor_key(buf, sizeof buf, subject, consumer, &key);
    if (e) return e;

    const uint8_t *val = NULL;
    size_t val_len = 0;
    e = bpt_search(st->cursors, &key, found, &val, &val_len);
    if (e || !*found) return e;
    return decode_index(val, val_len, index);
}

int bjm_cursor_set(bjm_store *st, const char *subject, const char *consumer,
                   uint64_t index) {
    int found = 0;
    uint64_t current = 0;
    int e = bjm_cursor_get(st, subject, consumer, &found, &current);
    if (e) return e;
    /* A receipt only ever moves forward: a late or duplicated ack from a
     * subscriber that already resumed must not rewind it into replay. */
    if (found && index <= current) return BJ_OK;

    char buf[BJM_SUBJECT_MAX + BJM_CONSUMER_MAX + 2];
    bpt_key key;
    e = cursor_key(buf, sizeof buf, subject, consumer, &key);
    if (e) return e;

    bj_builder *b = st->cbld;
    bj_builder_reset(b);
    bj_put_int(b, (int64_t)index);
    size_t len = 0;
    const uint8_t *val = bj_builder_data(b, &len);
    if (!val) return BJ_ERR_STATE;

    return bpt_add(st->cursors, &key, val, (uint32_t)len);
}

int bjm_cursor_sync(bjm_store *st) {
    if (!st->cursors) return BJ_OK;
    return bpt_sync(st->cursors);
}

int bjm_cursor_delete(bjm_store *st, const char *subject, const char *consumer,
                      int *deleted) {
    *deleted = 0;
    int e = cursors_open(st);
    if (e) return e;

    char buf[BJM_SUBJECT_MAX + BJM_CONSUMER_MAX + 2];
    bpt_key key;
    e = cursor_key(buf, sizeof buf, subject, consumer, &key);
    if (e) return e;

    /* bpt_delete is a silent no-op on a missing key, so look first —
     * "there was no such subscription" is worth telling the caller. */
    int found = 0;
    const uint8_t *val = NULL;
    size_t val_len = 0;
    e = bpt_search(st->cursors, &key, &found, &val, &val_len);
    if (e) return e;
    if (!found) return BJ_OK;

    e = bpt_delete(st->cursors, &key);
    if (e) return e;
    *deleted = 1;
    /* Removing a subscription raises the trim boundary, so make it
     * durable now rather than leaving retention to act on a receipt that
     * a crash could bring back. */
    return bpt_sync(st->cursors);
}

/* Bounds of one subject's consumer keys: "<subject>/" .. "<subject>0",
 * '0' being the byte after '/'. */
static void consumer_range(const char *subject, char *lo, size_t lo_cap,
                           char *hi, size_t hi_cap,
                           bpt_key *min, bpt_key *max, int *prefix_len) {
    int nlo = snprintf(lo, lo_cap, "%s/", subject);
    int nhi = snprintf(hi, hi_cap, "%s0", subject);
    min->is_string = 1; min->num = 0;
    min->str = (const uint8_t *)lo; min->str_len = (uint32_t)nlo;
    max->is_string = 1; max->num = 0;
    max->str = (const uint8_t *)hi; max->str_len = (uint32_t)nhi;
    *prefix_len = nlo;
}

int bjm_consumer_stats(bjm_store *st, const char *subject,
                       int *count, uint64_t *min_acked) {
    *count = 0;
    *min_acked = UINT64_MAX;
    int e = cursors_open(st);
    if (e) return e;

    char lo[BJM_SUBJECT_MAX + 2], hi[BJM_SUBJECT_MAX + 2];
    bpt_key min, max;
    int prefix_len;
    consumer_range(subject, lo, sizeof lo, hi, sizeof hi, &min, &max, &prefix_len);

    bpt_cursor *c = bpt_cursor_open(st->cursors, &min, &max);
    if (!c) return BJ_ERR_STATE;

    bpt_key k;
    const uint8_t *val;
    size_t val_len;
    int rc;
    while ((rc = bpt_cursor_next(c, &k, &val, &val_len)) == 1) {
        uint64_t acked = 0;
        if (decode_index(val, val_len, &acked) != BJ_OK) continue;
        (*count)++;
        if (acked < *min_acked) *min_acked = acked;
    }
    bpt_cursor_close(c);
    return rc < 0 ? rc : BJ_OK;
}

int bjm_consumers(bjm_store *st, const char *subject,
                  const uint8_t **out, size_t *out_len) {
    int e = cursors_open(st);
    if (e) return e;

    char lo[BJM_SUBJECT_MAX + 2], hi[BJM_SUBJECT_MAX + 2];
    bpt_key min, max;
    int nlo;
    consumer_range(subject, lo, sizeof lo, hi, sizeof hi, &min, &max, &nlo);

    uint64_t last = bjm_last_index(st, subject);

    bpt_cursor *c = bpt_cursor_open(st->cursors, &min, &max);
    if (!c) return BJ_ERR_STATE;

    /* The cursor writes into the tree's output buffer, and so would
     * bpt_search — but we only read through the cursor here, and build
     * into the store's own builder, so the two never collide. */
    bj_builder *b = st->bld;
    bj_builder_reset(b);
    bj_begin_array(b);

    bpt_key k;
    const uint8_t *val;
    size_t val_len;
    int rc;
    while ((rc = bpt_cursor_next(c, &k, &val, &val_len)) == 1) {
        uint64_t acked = 0;
        if (decode_index(val, val_len, &acked) != BJ_OK) continue;
        /* Key is "<subject>/<consumer>"; the consumer is what follows. */
        if (k.str_len <= (uint32_t)nlo) continue;
        const uint8_t *name = k.str + nlo;
        uint32_t name_len = k.str_len - (uint32_t)nlo;

        bj_begin_object(b);
        bj_put_key(b, (const uint8_t *)"consumer", 8);
        bj_put_string(b, name, name_len);
        bj_put_key(b, (const uint8_t *)"acked", 5);
        bj_put_int(b, (int64_t)acked);
        bj_put_key(b, (const uint8_t *)"lag", 3);
        bj_put_int(b, (int64_t)(last > acked ? last - acked : 0));
        bj_end_object(b);
    }
    bpt_cursor_close(c);
    if (rc < 0) return rc;

    bj_end_array(b);
    e = bj_builder_error(b);
    if (e) return e;
    *out = bj_builder_data(b, out_len);
    return *out ? BJ_OK : BJ_ERR_STATE;
}

/* ---- retention policy ------------------------------------------------- */

/*
 * Policies and marks share one tree, in two key spaces distinguished by a
 * leading byte: "p/<subject>" is the policy, "m/<subject>" its marks.
 * They are kept out of _cursors.bpt so that a consumer range scan cannot
 * see them by accident.
 */
static int policy_open(bjm_store *st) {
    if (st->policy) return BJ_OK;

    bj_io io;
    int e = st->ns.open(st->ns.ctx, POLICY_FILE, sizeof POLICY_FILE - 1,
                        BJ_NS_CREATE, &io);
    if (e) return e;

    int fresh = io.size(io.ctx) == 0;
    bpt *t = fresh ? bpt_create(&io, CURSORS_ORDER) : bpt_open(&io);
    if (!t) { st->ns.close(st->ns.ctx, &io); return BJ_ERR_STATE; }
    if (fresh) st->ns.sync(st->ns.ctx);

    st->policy_io = io;
    st->policy = t;
    return BJ_OK;
}

static int policy_key(char *buf, size_t cap, char kind, const char *subject,
                      bpt_key *key) {
    int n = snprintf(buf, cap, "%c/%s", kind, subject);
    if (n < 0 || (size_t)n >= cap) return BJ_ERR_RANGE;
    key->is_string = 1;
    key->num = 0;
    key->str = (const uint8_t *)buf;
    key->str_len = (uint32_t)n;
    return BJ_OK;
}

/* Collect the named integer fields of a flat binjson object. The key
 * buffer must outsize the longest key it compares against, NUL included —
 * "ignore_consumers" is 16 bytes, and a buffer of exactly 16 truncates it
 * to something that matches nothing. */
typedef struct {
    char key[32];
    bjm_policy *p;
} pol_grab;

static void pol_key(void *ctx, const uint8_t *k, uint32_t len) {
    pol_grab *g = ctx;
    if (len >= sizeof g->key) len = sizeof g->key - 1;
    memcpy(g->key, k, len);
    g->key[len] = '\0';
}

static void pol_int(void *ctx, double v) {
    pol_grab *g = ctx;
    if      (strcmp(g->key, "max_age_s") == 0)    g->p->max_age_s = (uint64_t)v;
    else if (strcmp(g->key, "max_messages") == 0) g->p->max_messages = (uint64_t)v;
    else if (strcmp(g->key, "max_bytes") == 0)    g->p->max_bytes = (uint64_t)v;
}

static void pol_bool(void *ctx, int v) {
    pol_grab *g = ctx;
    if (strcmp(g->key, "ignore_consumers") == 0) g->p->ignore_consumers = v;
}

static int policy_decode(const uint8_t *data, size_t len, bjm_policy *out) {
    memset(out, 0, sizeof *out);
    pol_grab g = { .p = out };
    bj_visitor v = bjm_visitor_noop(&g);
    v.on_key = pol_key;
    v.on_int = pol_int;
    v.on_bool = pol_bool;
    return bj_decode(data, len, &v, NULL);
}

static void policy_encode(bj_builder *b, const bjm_policy *p) {
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"max_age_s", 9);
    bj_put_int(b, (int64_t)p->max_age_s);
    bj_put_key(b, (const uint8_t *)"max_messages", 12);
    bj_put_int(b, (int64_t)p->max_messages);
    bj_put_key(b, (const uint8_t *)"max_bytes", 9);
    bj_put_int(b, (int64_t)p->max_bytes);
    bj_put_key(b, (const uint8_t *)"ignore_consumers", 16);
    bj_put_bool(b, p->ignore_consumers);
    bj_end_object(b);
}

int bjm_policy_get(bjm_store *st, const char *subject, int *found,
                   bjm_policy *out) {
    *found = 0;
    memset(out, 0, sizeof *out);
    int e = policy_open(st);
    if (e) return e;

    char buf[BJM_SUBJECT_MAX + 4];
    bpt_key key;
    e = policy_key(buf, sizeof buf, 'p', subject, &key);
    if (e) return e;

    const uint8_t *val = NULL;
    size_t val_len = 0;
    e = bpt_search(st->policy, &key, found, &val, &val_len);
    if (e || !*found) return e;
    return policy_decode(val, val_len, out);
}

/* Drop any cached copy so the next publish reloads it. */
static void policy_uncache(bjm_store *st, const char *subject) {
    for (size_t i = 0; i < st->nsubs; i++)
        if (strcmp(st->subs[i].name, subject) == 0)
            st->subs[i].pol_loaded = 0;
}

int bjm_policy_set(bjm_store *st, const char *subject, const bjm_policy *p) {
    int e = policy_open(st);
    if (e) return e;

    char buf[BJM_SUBJECT_MAX + 4];
    bpt_key key;
    e = policy_key(buf, sizeof buf, 'p', subject, &key);
    if (e) return e;

    policy_encode(st->cbld, p);
    size_t len = 0;
    const uint8_t *val = bj_builder_data(st->cbld, &len);
    if (!val) return BJ_ERR_STATE;

    e = bpt_add(st->policy, &key, val, (uint32_t)len);
    if (e) return e;
    policy_uncache(st, subject);
    /* Config, not a hot path: make it durable now so a policy survives a
     * power cut rather than silently reverting. */
    return bpt_sync(st->policy);
}

int bjm_policy_clear(bjm_store *st, const char *subject, int *cleared) {
    *cleared = 0;
    int e = policy_open(st);
    if (e) return e;

    char buf[BJM_SUBJECT_MAX + 4];
    bpt_key key;
    e = policy_key(buf, sizeof buf, 'p', subject, &key);
    if (e) return e;

    int found = 0;
    const uint8_t *val = NULL;
    size_t val_len = 0;
    e = bpt_search(st->policy, &key, &found, &val, &val_len);
    if (e) return e;
    if (!found) return BJ_OK;

    e = bpt_delete(st->policy, &key);
    if (e) return e;

    /* The marks only exist to serve an age policy. */
    bpt_key mkey;
    if (policy_key(buf, sizeof buf, 'm', subject, &mkey) == BJ_OK)
        bpt_delete(st->policy, &mkey);

    *cleared = 1;
    policy_uncache(st, subject);
    return bpt_sync(st->policy);
}

/* ---- publish-time marks ----------------------------------------------- */

typedef struct {
    uint64_t v[BJM_MARKS_MAX * 2];   /* index, time, index, time, ... */
    int      n;                      /* number of uint64s used        */
} marks;

static void mark_int(void *ctx, double v) {
    marks *m = ctx;
    if (m->n < (int)(sizeof m->v / sizeof m->v[0])) m->v[m->n++] = (uint64_t)v;
}

static int marks_load(bjm_store *st, const char *subject, marks *m) {
    m->n = 0;
    char buf[BJM_SUBJECT_MAX + 4];
    bpt_key key;
    int e = policy_key(buf, sizeof buf, 'm', subject, &key);
    if (e) return e;

    int found = 0;
    const uint8_t *val = NULL;
    size_t val_len = 0;
    e = bpt_search(st->policy, &key, &found, &val, &val_len);
    if (e || !found) return e;

    bj_visitor v = bjm_visitor_noop(m);
    v.on_int = mark_int;
    return bj_decode(val, val_len, &v, NULL);
}

static int marks_store(bjm_store *st, const char *subject, const marks *m) {
    char buf[BJM_SUBJECT_MAX + 4];
    bpt_key key;
    int e = policy_key(buf, sizeof buf, 'm', subject, &key);
    if (e) return e;

    bj_builder *b = st->cbld;
    bj_builder_reset(b);
    bj_begin_array(b);
    for (int i = 0; i < m->n; i++) bj_put_int(b, (int64_t)m->v[i]);
    bj_end_array(b);

    size_t len = 0;
    const uint8_t *val = bj_builder_data(b, &len);
    if (!val) return BJ_ERR_STATE;
    return bpt_add(st->policy, &key, val, (uint32_t)len);
}

/*
 * Record that `index` was published at `now`, if the subject has an age
 * policy and enough time has passed since the last mark. Oldest marks are
 * dropped once the ring is full.
 */
static void mark_publish(bjm_store *st, subject *s, uint64_t index) {
    if (!s->pol_loaded) {
        int found = 0;
        if (bjm_policy_get(st, s->name, &found, &s->pol) != BJ_OK) return;
        s->pol_loaded = 1;
    }
    if (!s->pol.max_age_s) return;           /* no age policy, no marks */

    uint64_t now = (uint64_t)time(NULL);
    uint64_t interval = s->pol.max_age_s / BJM_MARKS_PER_WINDOW;
    if (interval < 1) interval = 1;
    if (s->last_mark_t && now - s->last_mark_t < interval) return;

    marks m;
    if (marks_load(st, s->name, &m) != BJ_OK) return;
    if (m.n + 2 > (int)(sizeof m.v / sizeof m.v[0])) {
        memmove(m.v, m.v + 2, (size_t)(m.n - 2) * sizeof m.v[0]);
        m.n -= 2;
    }
    m.v[m.n++] = index;
    m.v[m.n++] = now;
    if (marks_store(st, s->name, &m) == BJ_OK) s->last_mark_t = now;
}

/* Forget marks below `base`, which no longer name a live message. */
static void marks_prune(bjm_store *st, const char *subject, uint64_t base) {
    marks m;
    if (marks_load(st, subject, &m) != BJ_OK || m.n == 0) return;
    int keep = 0;
    for (int i = 0; i + 1 < m.n; i += 2) {
        if (m.v[i] <= base) continue;
        m.v[keep++] = m.v[i];
        m.v[keep++] = m.v[i + 1];
    }
    if (keep == m.n) return;
    m.n = keep;
    marks_store(st, subject, &m);
}

/* ---- producer idempotency ---------------------------------------------- */

int bjm_dedup_id_valid(const char *s) {
    if (!s) return 0;
    size_t n = strlen(s);
    if (n == 0 || n > BJM_DEDUP_ID_MAX) return 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        /* Opaque apart from the two things that would break the index key
         * or the log: the separator, and anything unprintable. */
        if (c == '/' || c < 0x20 || c == 0x7f) return 0;
    }
    return 1;
}

void bjm_dedup_set_window(bjm_store *st, uint64_t ms) { st->dd_window_ms = ms; }
uint64_t bjm_dedup_window(const bjm_store *st) {
    return st->dd_window_ms ? st->dd_window_ms : BJM_DEDUP_WINDOW_DEFAULT_MS;
}

/*
 * Which generation is current, and when it took over. Kept in the policy
 * tree because it must outlive a restart and the dedup files themselves
 * are the thing being cleared. Getting it wrong after a crash would only
 * clear the newer generation early — correctness is unaffected, since a
 * lookup checks both, but the window would be shorter than promised.
 */
typedef struct { char key[16]; uint64_t gen, rotated_ms; } dd_state;

static void dd_key(void *ctx, const uint8_t *k, uint32_t len) {
    dd_state *d = ctx;
    if (len >= sizeof d->key) len = sizeof d->key - 1;
    memcpy(d->key, k, len);
    d->key[len] = '\0';
}

static void dd_int(void *ctx, double v) {
    dd_state *d = ctx;
    if (strcmp(d->key, "gen") == 0)             d->gen = (uint64_t)v;
    else if (strcmp(d->key, "rotated_ms") == 0) d->rotated_ms = (uint64_t)v;
}

static void dd_state_key(bpt_key *key) {
    key->is_string = 1;
    key->num = 0;
    key->str = (const uint8_t *)"d/state";
    key->str_len = 7;
}

static void dd_state_save(bjm_store *st) {
    if (policy_open(st) != BJ_OK) return;
    bpt_key key;
    dd_state_key(&key);

    bj_builder *b = st->cbld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"gen", 3);
    bj_put_int(b, st->dd_gen);
    bj_put_key(b, (const uint8_t *)"rotated_ms", 10);
    bj_put_int(b, (int64_t)st->dd_rotated_ms);
    bj_end_object(b);

    size_t len = 0;
    const uint8_t *val = bj_builder_data(b, &len);
    if (val && bpt_add(st->policy, &key, val, (uint32_t)len) == BJ_OK)
        bpt_sync(st->policy);
}

static int dedup_open(bjm_store *st) {
    if (st->dd_open) return BJ_OK;

    static const char *names[2] = { DEDUP_FILE_0, DEDUP_FILE_1 };
    for (int i = 0; i < 2; i++) {
        bj_io io;
        int e = st->ns.open(st->ns.ctx, names[i], (uint32_t)strlen(names[i]),
                            BJ_NS_CREATE, &io);
        if (e) return e;
        int fresh = io.size(io.ctx) == 0;
        bpt *t = fresh ? bpt_create(&io, CURSORS_ORDER) : bpt_open(&io);
        if (!t) { st->ns.close(st->ns.ctx, &io); return BJ_ERR_STATE; }
        st->dd_io[i] = io;
        st->dd[i] = t;
    }

    /* Recover which generation was current. */
    if (policy_open(st) == BJ_OK) {
        bpt_key key;
        dd_state_key(&key);
        int found = 0;
        const uint8_t *val = NULL;
        size_t val_len = 0;
        if (bpt_search(st->policy, &key, &found, &val, &val_len) == BJ_OK && found) {
            dd_state d;
            memset(&d, 0, sizeof d);
            bj_visitor v = bjm_visitor_noop(&d);
            v.on_key = dd_key;
            v.on_int = dd_int;
            if (bj_decode(val, val_len, &v, NULL) == BJ_OK) {
                st->dd_gen = d.gen ? 1 : 0;
                st->dd_rotated_ms = d.rotated_ms;
            }
        }
    }
    if (st->dd_rotated_ms == 0) {
        st->dd_rotated_ms = now_ms();
        st->dd_open = 1;
        dd_state_save(st);
        return BJ_OK;
    }
    st->dd_open = 1;
    return BJ_OK;
}

/*
 * Retire the older generation once the window has elapsed. bpt_reset
 * truncates the file and writes a fresh empty tree, so eviction costs
 * nothing per entry and leaves no deletions to compact away.
 */
static void dedup_rotate(bjm_store *st) {
    uint64_t now = now_ms();
    uint64_t window = bjm_dedup_window(st);
    uint64_t elapsed = now - st->dd_rotated_ms;
    if (elapsed < window) return;

    if (elapsed >= 2 * window) {
        /*
         * Rotation is lazy — it only happens when something asks — so a
         * quiet period can leave both generations older than the window.
         * Stepping one at a time here would let an id outlive the bound
         * this promises, by however long the broker was idle.
         */
        if (bpt_reset(st->dd[0]) != BJ_OK) return;
        if (bpt_reset(st->dd[1]) != BJ_OK) return;
        st->dd_gen = 0;
    } else {
        int old = 1 - st->dd_gen;
        if (bpt_reset(st->dd[old]) != BJ_OK) return;
        st->dd_gen = old;
    }
    st->dd_rotated_ms = now;
    dd_state_save(st);
}

static int dedup_key(char *buf, size_t cap, const char *subject,
                     const char *id, bpt_key *key) {
    int n = snprintf(buf, cap, "%s/%s", subject, id);
    if (n < 0 || (size_t)n >= cap) return BJ_ERR_RANGE;
    key->is_string = 1;
    key->num = 0;
    key->str = (const uint8_t *)buf;
    key->str_len = (uint32_t)n;
    return BJ_OK;
}

int bjm_dedup_lookup(bjm_store *st, const char *subject, const char *id,
                     int *found, uint64_t *index) {
    *found = 0;
    *index = 0;
    int e = dedup_open(st);
    if (e) return e;
    dedup_rotate(st);

    char buf[BJM_SUBJECT_MAX + BJM_DEDUP_ID_MAX + 2];
    bpt_key key;
    e = dedup_key(buf, sizeof buf, subject, id, &key);
    if (e) return e;

    /* Current generation first, then the one waiting to be retired. */
    for (int i = 0; i < 2; i++) {
        bpt *t = st->dd[i == 0 ? st->dd_gen : 1 - st->dd_gen];
        const uint8_t *val = NULL;
        size_t val_len = 0;
        int hit = 0;
        e = bpt_search(t, &key, &hit, &val, &val_len);
        if (e) return e;
        if (!hit) continue;
        e = decode_index(val, val_len, index);
        if (e) return e;
        *found = 1;
        /* Seen in the older generation: copy it forward so an id still in
         * active use is not dropped by the next rotation. */
        if (i == 1) {
            bj_builder *b = st->cbld;
            bj_builder_reset(b);
            bj_put_int(b, (int64_t)*index);
            size_t len = 0;
            const uint8_t *v = bj_builder_data(b, &len);
            if (v) bpt_add(st->dd[st->dd_gen], &key, v, (uint32_t)len);
        }
        return BJ_OK;
    }
    return BJ_OK;
}

int bjm_dedup_record(bjm_store *st, const char *subject, const char *id,
                     uint64_t index) {
    int e = dedup_open(st);
    if (e) return e;

    char buf[BJM_SUBJECT_MAX + BJM_DEDUP_ID_MAX + 2];
    bpt_key key;
    e = dedup_key(buf, sizeof buf, subject, id, &key);
    if (e) return e;

    bj_builder *b = st->cbld;
    bj_builder_reset(b);
    bj_put_int(b, (int64_t)index);
    size_t len = 0;
    const uint8_t *val = bj_builder_data(b, &len);
    if (!val) return BJ_ERR_STATE;

    /*
     * Not fsynced. A crash between the publish and this record leaves the
     * message durable but the id forgotten, so a retry would duplicate —
     * the same narrow gap that exists between any append and its index,
     * and much smaller than the failure this is protecting against.
     */
    return bpt_add(st->dd[st->dd_gen], &key, val, (uint32_t)len);
}

/* ---- queue groups ------------------------------------------------------ */

int bjm_group_valid(const char *s) { return name_valid(s, BJM_GROUP_MAX); }

/*
 * One group's persisted state. `fl` is the inflight table flattened as
 * (index, expires_ms, attempts) triples — the same shape as the retention
 * marks, and for the same reason: it decodes with one integer callback
 * and the table is small enough that rewriting it whole is cheaper than
 * maintaining a record per entry.
 */
typedef struct qrec {
    uint64_t next;
    uint64_t lease_ms;
    uint64_t max_attempts;      /* 0 = retry forever */
    uint64_t backoff_ms;        /* base retry delay; 0 = retry instantly */
    uint64_t max_backoff_ms;    /* ceiling on the doubling */
    uint64_t fl[BJM_INFLIGHT_MAX * 3];
    int      nfl;               /* uint64s used, so entries = nfl/3 */
    uint64_t dead_total;        /* jobs that exhausted their attempts */
    uint64_t dead[BJM_DEAD_MAX];/* the most recent of them */
    int      ndead;
    /* decode scratch */
    char     key[32];
    int      in_array;          /* 0 none, 1 inflight, 2 dead */
} qrec;

#define FL_INDEX(q, i)   ((q)->fl[(i) * 3])
#define FL_EXPIRES(q, i) ((q)->fl[(i) * 3 + 1])
#define FL_ATTEMPTS(q, i) ((q)->fl[(i) * 3 + 2])
#define FL_COUNT(q)      ((q)->nfl / 3)

/*
 * The dead-letter channel for `subject`. Returns 0 when there cannot be
 * one: either the name would not fit, or the subject is already a
 * dead-letter channel, which stops a chain of ".dead.dead.dead" when a
 * queue group is consuming a DLQ and its jobs also fail.
 */
static int dead_subject_of(const char *subject, char *out, size_t cap) {
    size_t n = strlen(subject);
    const size_t suffix = sizeof BJM_DEAD_SUFFIX - 1;
    if (n >= suffix && strcmp(subject + n - suffix, BJM_DEAD_SUFFIX) == 0)
        return 0;
    int w = snprintf(out, cap, "%s%s", subject, BJM_DEAD_SUFFIX);
    if (w < 0 || (size_t)w >= cap || !bjm_subject_valid(out)) return 0;
    return 1;
}

static uint64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)(ts.tv_nsec / 1000000);
}

static int queues_open(bjm_store *st) {
    if (st->queues) return BJ_OK;

    bj_io io;
    int e = st->ns.open(st->ns.ctx, QUEUES_FILE, sizeof QUEUES_FILE - 1,
                        BJ_NS_CREATE, &io);
    if (e) return e;

    int fresh = io.size(io.ctx) == 0;
    bpt *t = fresh ? bpt_create(&io, CURSORS_ORDER) : bpt_open(&io);
    if (!t) { st->ns.close(st->ns.ctx, &io); return BJ_ERR_STATE; }
    if (fresh) st->ns.sync(st->ns.ctx);

    if (!st->qscratch) {
        st->qscratch = calloc(1, sizeof *st->qscratch);
        if (!st->qscratch) {
            bpt_free(t);
            st->ns.close(st->ns.ctx, &io);
            return BJ_ERR_OOM;
        }
    }
    st->queues_io = io;
    st->queues = t;
    return BJ_OK;
}

/* "<subject>/<group>", same unambiguous shape as a consumer key. */
static int queue_key(char *buf, size_t cap, const char *subject,
                     const char *group, bpt_key *key) {
    int n = snprintf(buf, cap, "%s/%s", subject, group ? group : "");
    if (n < 0 || (size_t)n >= cap) return BJ_ERR_RANGE;
    key->is_string = 1;
    key->num = 0;
    key->str = (const uint8_t *)buf;
    key->str_len = (uint32_t)n;
    return BJ_OK;
}

static void q_key(void *ctx, const uint8_t *k, uint32_t len) {
    qrec *q = ctx;
    if (len >= sizeof q->key) len = sizeof q->key - 1;
    memcpy(q->key, k, len);
    q->key[len] = '\0';
    q->in_array = strcmp(q->key, "inflight") == 0 ? 1
                : strcmp(q->key, "dead") == 0     ? 2 : 0;
}

static void q_int(void *ctx, double v) {
    qrec *q = ctx;
    if (q->in_array == 1) {
        if (q->nfl < (int)(sizeof q->fl / sizeof q->fl[0]))
            q->fl[q->nfl++] = (uint64_t)v;
    } else if (q->in_array == 2) {
        if (q->ndead < BJM_DEAD_MAX) q->dead[q->ndead++] = (uint64_t)v;
    } else if (strcmp(q->key, "next") == 0) {
        q->next = (uint64_t)v;
    } else if (strcmp(q->key, "lease_ms") == 0) {
        q->lease_ms = (uint64_t)v;
    } else if (strcmp(q->key, "max_attempts") == 0) {
        q->max_attempts = (uint64_t)v;
    } else if (strcmp(q->key, "dead_total") == 0) {
        q->dead_total = (uint64_t)v;
    } else if (strcmp(q->key, "backoff_ms") == 0) {
        q->backoff_ms = (uint64_t)v;
    } else if (strcmp(q->key, "max_backoff_ms") == 0) {
        q->max_backoff_ms = (uint64_t)v;
    }
}

static void q_array_end(void *ctx) { ((qrec *)ctx)->in_array = 0; }

/*
 * Load a group's record, or invent a fresh one. A new group starts at the
 * oldest surviving message: a job queue exists to run the backlog.
 */
static int queue_load(bjm_store *st, const char *subject_name, const char *group,
                      qrec *q, int *existed) {
    memset(q, 0, sizeof *q);
    *existed = 0;

    char buf[BJM_SUBJECT_MAX + BJM_GROUP_MAX + 2];
    bpt_key key;
    int e = queue_key(buf, sizeof buf, subject_name, group, &key);
    if (e) return e;

    int found = 0;
    const uint8_t *val = NULL;
    size_t val_len = 0;
    e = bpt_search(st->queues, &key, &found, &val, &val_len);
    if (e) return e;

    if (found) {
        bj_visitor v = bjm_visitor_noop(q);
        v.on_key = q_key;
        v.on_int = q_int;
        v.on_array_end = q_array_end;
        e = bj_decode(val, val_len, &v, NULL);
        if (e) return e;
        *existed = 1;
        /* nfl must stay a whole number of triples even if the record was
         * truncated by a smaller BJM_INFLIGHT_MAX in an older binary. */
        q->nfl -= q->nfl % 3;
        return BJ_OK;
    }

    subject *s;
    e = subject_get(st, subject_name, 0, &s);
    q->next = (e == BJ_OK) ? elog_base_index(s->log) + 1 : 1;
    q->lease_ms = BJM_LEASE_DEFAULT_MS;
    q->max_attempts = BJM_MAX_ATTEMPTS_DEFAULT;
    q->backoff_ms = BJM_BACKOFF_DEFAULT_MS;
    q->max_backoff_ms = BJM_MAX_BACKOFF_DEFAULT_MS;
    return BJ_OK;
}

static int queue_store(bjm_store *st, const char *subject, const char *group,
                       const qrec *q) {
    char buf[BJM_SUBJECT_MAX + BJM_GROUP_MAX + 2];
    bpt_key key;
    int e = queue_key(buf, sizeof buf, subject, group, &key);
    if (e) return e;

    bj_builder *b = st->cbld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"next", 4);
    bj_put_int(b, (int64_t)q->next);
    bj_put_key(b, (const uint8_t *)"lease_ms", 8);
    bj_put_int(b, (int64_t)q->lease_ms);
    bj_put_key(b, (const uint8_t *)"max_attempts", 12);
    bj_put_int(b, (int64_t)q->max_attempts);
    bj_put_key(b, (const uint8_t *)"dead_total", 10);
    bj_put_int(b, (int64_t)q->dead_total);
    bj_put_key(b, (const uint8_t *)"backoff_ms", 10);
    bj_put_int(b, (int64_t)q->backoff_ms);
    bj_put_key(b, (const uint8_t *)"max_backoff_ms", 14);
    bj_put_int(b, (int64_t)q->max_backoff_ms);
    bj_put_key(b, (const uint8_t *)"inflight", 8);
    bj_begin_array(b);
    for (int i = 0; i < q->nfl; i++) bj_put_int(b, (int64_t)q->fl[i]);
    bj_end_array(b);
    bj_put_key(b, (const uint8_t *)"dead", 4);
    bj_begin_array(b);
    for (int i = 0; i < q->ndead; i++) bj_put_int(b, (int64_t)q->dead[i]);
    bj_end_array(b);
    bj_end_object(b);

    size_t len = 0;
    const uint8_t *val = bj_builder_data(b, &len);
    if (!val) return BJ_ERR_STATE;
    return bpt_add(st->queues, &key, val, (uint32_t)len);
}

static void fl_remove(qrec *q, int entry) {
    int last = FL_COUNT(q) - 1;
    if (entry != last)
        memcpy(&q->fl[entry * 3], &q->fl[last * 3], 3 * sizeof q->fl[0]);
    q->nfl -= 3;
}

static int fl_find(const qrec *q, uint64_t index) {
    for (int i = 0; i < FL_COUNT(q); i++)
        if (FL_INDEX(q, i) == index) return i;
    return -1;
}

int bjm_take(bjm_store *st, const char *subject_name, const char *group,
             int max, uint64_t lease_ms, int *count,
             const uint8_t **out, size_t *out_len) {
    *count = 0;
    int e = queues_open(st);
    if (e) return e;

    subject *s;
    e = subject_get(st, subject_name, 0, &s);
    if (e) return e;
    uint64_t last = elog_last_index(s->log);

    qrec *q = st->qscratch;
    int existed = 0;
    e = queue_load(st, subject_name, group, q, &existed);
    if (e) return e;
    if (lease_ms == UINT64_MAX) lease_ms = q->lease_ms;   /* caller said "default" */
    else q->lease_ms = lease_ms;

    if (max < 1) max = 1;
    if (max > BJM_INFLIGHT_MAX) max = BJM_INFLIGHT_MAX;

    uint64_t now = now_ms();
    uint64_t chosen[BJM_INFLIGHT_MAX];
    uint64_t attempts[BJM_INFLIGHT_MAX];
    int n = 0;
    /* Jobs that ran out of attempts during this call, routed to the
     * dead-letter channel once the take itself is finished. */
    uint64_t dead_idx[BJM_INFLIGHT_MAX];
    uint64_t dead_att[BJM_INFLIGHT_MAX];
    int ndead_now = 0;

    /*
     * Expired leases first. A job whose worker died is older work than
     * anything still untouched, and redelivering it before handing out
     * new jobs keeps a failing job from being starved behind the queue.
     */
    for (int i = 0; i < FL_COUNT(q) && n < max; i++) {
        if (FL_EXPIRES(q, i) > now) continue;
        /*
         * A job that has used up its attempts stops coming back. Without
         * this a job that always fails is redelivered as fast as workers
         * can ask for it, starving every other job in the queue — the
         * failure mode is a hot loop, not a slow leak.
         */
        if (q->max_attempts && FL_ATTEMPTS(q, i) >= q->max_attempts) {
            uint64_t dead_index = FL_INDEX(q, i);
            q->dead_total++;
            if (q->ndead == BJM_DEAD_MAX) {
                memmove(q->dead, q->dead + 1,
                        (BJM_DEAD_MAX - 1) * sizeof q->dead[0]);
                q->ndead--;
            }
            q->dead[q->ndead++] = dead_index;
            if (ndead_now < (int)(sizeof dead_idx / sizeof dead_idx[0])) {
                dead_idx[ndead_now] = dead_index;
                dead_att[ndead_now] = FL_ATTEMPTS(q, i);
                ndead_now++;
            }
            fl_remove(q, i);
            i--;                     /* fl_remove moved another entry here */
            continue;
        }
        FL_ATTEMPTS(q, i)++;
        FL_EXPIRES(q, i) = now + lease_ms;
        chosen[n] = FL_INDEX(q, i);
        attempts[n] = FL_ATTEMPTS(q, i);
        n++;
    }

    /* Then messages never handed out. */
    while (n < max && q->next <= last) {
        if (lease_ms > 0) {
            if (FL_COUNT(q) >= BJM_INFLIGHT_MAX) break;   /* backpressure */
            q->fl[q->nfl++] = q->next;
            q->fl[q->nfl++] = now + lease_ms;
            q->fl[q->nfl++] = 1;
        }
        chosen[n] = q->next;
        attempts[n] = 1;
        n++;
        q->next++;
    }

    /*
     * Persist the leases before handing the jobs out. The other order
     * could give a job to a worker and forget it had, which is the one
     * failure this table exists to prevent.
     */
    e = queue_store(st, subject_name, group, q);
    if (e) return e;

    bj_builder *b = st->bld;
    bj_builder_reset(b);
    bj_begin_array(b);
    for (int i = 0; i < n; i++) {
        uint64_t term;
        int type;
        const uint8_t *payload;
        size_t plen;
        if (elog_get(s->log, chosen[i], &term, &type, &payload, &plen) != BJ_OK)
            continue;   /* trimmed out from under us; skip it */
        bj_begin_object(b);
        bj_put_key(b, (const uint8_t *)"index", 5);
        bj_put_int(b, (int64_t)chosen[i]);
        bj_put_key(b, (const uint8_t *)"attempts", 8);
        bj_put_int(b, (int64_t)attempts[i]);
        bj_put_key(b, (const uint8_t *)"expires_ms", 10);
        bj_put_int(b, (int64_t)(lease_ms ? now + lease_ms : 0));
        /* As elog_get_batch does for a subscribe: without it a worker
         * cannot tell a headers envelope from a plain message. */
        bj_put_key(b, (const uint8_t *)"type", 4);
        bj_put_int(b, type);
        bj_put_key(b, (const uint8_t *)"payload", 7);
        bj_put_binary(b, payload, (uint32_t)plen);
        bj_end_object(b);
        (*count)++;
    }
    bj_end_array(b);
    e = bj_builder_error(b);
    if (e) return e;

    /*
     * Dead-letter last, and never earlier: publishing opens another
     * subject, which can reallocate the store's subject table and
     * invalidate `s`. Re-acquire it each time round rather than carrying
     * the old pointer across. The response is already built in st->bld,
     * and publishing does not touch that builder.
     */
    char dead[BJM_SUBJECT_MAX + 1];
    if (ndead_now > 0 && dead_subject_of(subject_name, dead, sizeof dead)) {
        for (int i = 0; i < ndead_now; i++) {
            subject *src;
            if (subject_get(st, subject_name, 0, &src) != BJ_OK) break;

            uint64_t dterm;
            int dtype;
            const uint8_t *payload;
            size_t plen;
            if (elog_get(src->log, dead_idx[i], &dterm, &dtype,
                         &payload, &plen) != BJ_OK)
                continue;

            bj_builder *d = st->dbld;
            bj_builder_reset(d);
            bj_begin_object(d);
            bj_put_key(d, (const uint8_t *)"subject", 7);
            bj_put_string(d, (const uint8_t *)subject_name,
                          (uint32_t)strlen(subject_name));
            bj_put_key(d, (const uint8_t *)"group", 5);
            bj_put_string(d, (const uint8_t *)group, (uint32_t)strlen(group));
            bj_put_key(d, (const uint8_t *)"index", 5);
            bj_put_int(d, (int64_t)dead_idx[i]);
            bj_put_key(d, (const uint8_t *)"attempts", 8);
            bj_put_int(d, (int64_t)dead_att[i]);
            bj_put_key(d, (const uint8_t *)"failed_ms", 9);
            bj_put_int(d, (int64_t)now);
            /* So a requeue restores the message's shape, not just its
             * bytes: a headers envelope must come back as one. */
            bj_put_key(d, (const uint8_t *)"type", 4);
            bj_put_int(d, dtype);
            /* BINARY rather than spliced raw: requeue has to hand these
             * exact bytes back to the log, and a binary field gives the
             * decoder their extent for free. */
            bj_put_key(d, (const uint8_t *)"payload", 7);
            bj_put_binary(d, payload, (uint32_t)plen);
            bj_end_object(d);

            size_t dlen = 0;
            const uint8_t *dv = bj_builder_data(d, &dlen);
            uint64_t at = 0;
            if (dv) bjm_publish(st, dead, BJM_ENTRY_PLAIN, dv, (uint32_t)dlen, &at);
        }
    }

    *out = bj_builder_data(b, out_len);
    return *out ? BJ_OK : BJ_ERR_STATE;
}

/* ---- requeue ----------------------------------------------------------- */

/* Pull `subject` and `payload` back out of a dead-letter envelope. */
typedef struct {
    char     key[16];
    char     subject[BJM_SUBJECT_MAX + 1];
    uint8_t *payload;
    size_t   plen;
    int      type;
} envelope;

static void env_key(void *ctx, const uint8_t *k, uint32_t len) {
    envelope *v = ctx;
    if (len >= sizeof v->key) len = sizeof v->key - 1;
    memcpy(v->key, k, len);
    v->key[len] = '\0';
}

static void env_string(void *ctx, const uint8_t *sv, uint32_t len) {
    envelope *v = ctx;
    if (strcmp(v->key, "subject") != 0) return;
    if (len > BJM_SUBJECT_MAX) len = BJM_SUBJECT_MAX;
    memcpy(v->subject, sv, len);
    v->subject[len] = '\0';
}

static void env_int(void *ctx, double v) {
    envelope *e = ctx;
    if (strcmp(e->key, "type") == 0) e->type = (int)v;
}

static void env_binary(void *ctx, const uint8_t *b, uint32_t len) {
    envelope *v = ctx;
    if (strcmp(v->key, "payload") != 0 || v->payload) return;
    /* Copied: publishing reuses the log buffer this points into. */
    v->payload = malloc(len ? len : 1);
    if (!v->payload) return;
    memcpy(v->payload, b, len);
    v->plen = len;
}

int bjm_requeue(bjm_store *st, const char *subject_name, uint64_t dlq_index,
                uint64_t *new_index) {
    char dead[BJM_SUBJECT_MAX + 1];
    if (!dead_subject_of(subject_name, dead, sizeof dead)) return BJ_ERR_RANGE;

    subject *s;
    int e = subject_get(st, dead, 0, &s);
    if (e) return e;

    uint64_t term;
    int type;
    const uint8_t *rec;
    size_t rlen;
    e = elog_get(s->log, dlq_index, &term, &type, &rec, &rlen);
    if (e) return e;

    envelope v;
    memset(&v, 0, sizeof v);
    v.type = BJM_ENTRY_PLAIN;      /* records written before types existed */
    bj_visitor vis = bjm_visitor_noop(&v);
    vis.on_key = env_key;
    vis.on_string = env_string;
    vis.on_int = env_int;
    vis.on_binary = env_binary;
    e = bj_decode(rec, rlen, &vis, NULL);
    if (e || !v.payload || !bjm_subject_valid(v.subject)) {
        free(v.payload);
        return e ? e : BJ_ERR_VERIFY;
    }

    /* Back to the subject the envelope names, not the one asked for: a
     * requeue must not be able to move a message between subjects. */
    e = bjm_publish(st, v.subject, v.type, v.payload, (uint32_t)v.plen, new_index);
    free(v.payload);
    return e;
}

/*
 * The wait before a failed job is offered again: the group's base delay
 * doubled once per attempt so far, capped. The shift is bounded because a
 * group with max_attempts == 0 retries forever and would otherwise shift
 * past the width of the type.
 */
static uint64_t backoff_for(const qrec *q, uint64_t attempts) {
    if (q->backoff_ms == 0) return 0;
    unsigned shift = attempts > 0 ? (unsigned)(attempts - 1) : 0;
    if (shift > 30) shift = 30;
    uint64_t d = q->backoff_ms << shift;
    if (d < q->backoff_ms) d = q->max_backoff_ms;       /* overflowed */
    if (q->max_backoff_ms && d > q->max_backoff_ms) d = q->max_backoff_ms;
    return d;
}

static int queue_release(bjm_store *st, const char *subject, const char *group,
                         uint64_t index, int drop, uint64_t delay_ms,
                         int *found, uint64_t *retry_in_ms) {
    *found = 0;
    if (retry_in_ms) *retry_in_ms = 0;
    int e = queues_open(st);
    if (e) return e;

    qrec *q = st->qscratch;
    int existed = 0;
    e = queue_load(st, subject, group, q, &existed);
    if (e || !existed) return e;

    int at = fl_find(q, index);
    if (at < 0) return BJ_OK;      /* never leased, or already finished */

    if (drop) {
        fl_remove(q, at);
    } else {
        uint64_t d = delay_ms == UINT64_MAX
            ? backoff_for(q, FL_ATTEMPTS(q, at))
            : delay_ms;
        FL_EXPIRES(q, at) = now_ms() + d;
        if (retry_in_ms) *retry_in_ms = d;
    }

    *found = 1;
    return queue_store(st, subject, group, q);
}

int bjm_done(bjm_store *st, const char *subject, const char *group,
             uint64_t index, int *found) {
    return queue_release(st, subject, group, index, 1, 0, found, NULL);
}

int bjm_fail(bjm_store *st, const char *subject, const char *group,
             uint64_t index, uint64_t delay_ms, int *found,
             uint64_t *retry_in_ms) {
    return queue_release(st, subject, group, index, 0, delay_ms,
                         found, retry_in_ms);
}

int bjm_queue_config(bjm_store *st, const char *subject, const char *group,
                     uint64_t lease_ms, uint64_t max_attempts,
                     uint64_t backoff_ms, uint64_t max_backoff_ms) {
    int e = queues_open(st);
    if (e) return e;

    qrec *q = st->qscratch;
    int existed = 0;
    e = queue_load(st, subject, group, q, &existed);
    if (e) return e;
    q->lease_ms = lease_ms;
    q->max_attempts = max_attempts;
    q->backoff_ms = backoff_ms;
    q->max_backoff_ms = max_backoff_ms;

    e = queue_store(st, subject, group, q);
    if (e) return e;
    return bpt_sync(st->queues);   /* config, not a hot path */
}

int bjm_queue_delete(bjm_store *st, const char *subject, const char *group,
                     int *deleted) {
    *deleted = 0;
    int e = queues_open(st);
    if (e) return e;

    char buf[BJM_SUBJECT_MAX + BJM_GROUP_MAX + 2];
    bpt_key key;
    e = queue_key(buf, sizeof buf, subject, group, &key);
    if (e) return e;

    int found = 0;
    const uint8_t *val = NULL;
    size_t val_len = 0;
    e = bpt_search(st->queues, &key, &found, &val, &val_len);
    if (e || !found) return e;

    e = bpt_delete(st->queues, &key);
    if (e) return e;
    *deleted = 1;
    return bpt_sync(st->queues);
}

/* Bounds of one subject's group keys, as for consumers. */
static void queue_range(const char *subject, char *lo, size_t lo_cap,
                        char *hi, size_t hi_cap,
                        bpt_key *min, bpt_key *max, int *prefix_len) {
    int nlo = snprintf(lo, lo_cap, "%s/", subject);
    int nhi = snprintf(hi, hi_cap, "%s0", subject);
    min->is_string = 1; min->num = 0;
    min->str = (const uint8_t *)lo; min->str_len = (uint32_t)nlo;
    max->is_string = 1; max->num = 0;
    max->str = (const uint8_t *)hi; max->str_len = (uint32_t)nhi;
    *prefix_len = nlo;
}

int bjm_queues(bjm_store *st, const char *subject,
               const uint8_t **out, size_t *out_len) {
    int e = queues_open(st);
    if (e) return e;

    uint64_t last = bjm_last_index(st, subject);
    uint64_t now = now_ms();

    char lo[BJM_SUBJECT_MAX + 2], hi[BJM_SUBJECT_MAX + 2];
    bpt_key min, max;
    int nlo;
    queue_range(subject, lo, sizeof lo, hi, sizeof hi, &min, &max, &nlo);

    bpt_cursor *c = bpt_cursor_open(st->queues, &min, &max);
    if (!c) return BJ_ERR_STATE;

    char dead_name[BJM_SUBJECT_MAX + 1];
    bj_builder *b = st->bld;
    bj_builder_reset(b);
    bj_begin_array(b);

    bpt_key k;
    const uint8_t *val;
    size_t val_len;
    int rc;
    while ((rc = bpt_cursor_next(c, &k, &val, &val_len)) == 1) {
        if (k.str_len <= (uint32_t)nlo) continue;
        qrec q;
        memset(&q, 0, sizeof q);
        bj_visitor v = bjm_visitor_noop(&q);
        v.on_key = q_key;
        v.on_int = q_int;
        v.on_array_end = q_array_end;
        if (bj_decode(val, val_len, &v, NULL) != BJ_OK) continue;
        q.nfl -= q.nfl % 3;

        int expired = 0;
        for (int i = 0; i < FL_COUNT(&q); i++)
            if (FL_EXPIRES(&q, i) <= now) expired++;

        bj_begin_object(b);
        bj_put_key(b, (const uint8_t *)"group", 5);
        bj_put_string(b, k.str + nlo, k.str_len - (uint32_t)nlo);
        bj_put_key(b, (const uint8_t *)"next", 4);
        bj_put_int(b, (int64_t)q.next);
        /* Not yet handed to anyone. */
        bj_put_key(b, (const uint8_t *)"pending", 7);
        bj_put_int(b, (int64_t)(last >= q.next ? last - q.next + 1 : 0));
        /* Handed out and not finished; `expired` of them are due back. */
        bj_put_key(b, (const uint8_t *)"inflight", 8);
        bj_put_int(b, FL_COUNT(&q));
        bj_put_key(b, (const uint8_t *)"expired", 7);
        bj_put_int(b, expired);
        bj_put_key(b, (const uint8_t *)"lease_ms", 8);
        bj_put_int(b, (int64_t)q.lease_ms);
        bj_put_key(b, (const uint8_t *)"max_attempts", 12);
        bj_put_int(b, (int64_t)q.max_attempts);
        bj_put_key(b, (const uint8_t *)"backoff_ms", 10);
        bj_put_int(b, (int64_t)q.backoff_ms);
        bj_put_key(b, (const uint8_t *)"max_backoff_ms", 14);
        bj_put_int(b, (int64_t)q.max_backoff_ms);
        bj_put_key(b, (const uint8_t *)"dead", 4);
        bj_put_int(b, (int64_t)q.dead_total);
        bj_put_key(b, (const uint8_t *)"dead_subject", 12);
        if (dead_subject_of(subject, dead_name, sizeof dead_name))
            bj_put_string(b, (const uint8_t *)dead_name,
                          (uint32_t)strlen(dead_name));
        else
            bj_put_null(b);
        bj_put_key(b, (const uint8_t *)"dead_indexes", 12);
        bj_begin_array(b);
        for (int i = 0; i < q.ndead; i++) bj_put_int(b, (int64_t)q.dead[i]);
        bj_end_array(b);
        bj_end_object(b);
    }
    bpt_cursor_close(c);
    if (rc < 0) return rc;

    bj_end_array(b);
    e = bj_builder_error(b);
    if (e) return e;
    *out = bj_builder_data(b, out_len);
    return *out ? BJ_OK : BJ_ERR_STATE;
}

int bjm_queue_floor(bjm_store *st, const char *subject,
                    uint64_t *floor, int *ngroups) {
    *floor = UINT64_MAX;
    *ngroups = 0;
    int e = queues_open(st);
    if (e) return e;

    char lo[BJM_SUBJECT_MAX + 2], hi[BJM_SUBJECT_MAX + 2];
    bpt_key min, max;
    int nlo;
    queue_range(subject, lo, sizeof lo, hi, sizeof hi, &min, &max, &nlo);

    bpt_cursor *c = bpt_cursor_open(st->queues, &min, &max);
    if (!c) return BJ_ERR_STATE;

    bpt_key k;
    const uint8_t *val;
    size_t val_len;
    int rc;
    while ((rc = bpt_cursor_next(c, &k, &val, &val_len)) == 1) {
        if (k.str_len <= (uint32_t)nlo) continue;
        qrec q;
        memset(&q, 0, sizeof q);
        bj_visitor v = bjm_visitor_noop(&q);
        v.on_key = q_key;
        v.on_int = q_int;
        v.on_array_end = q_array_end;
        if (bj_decode(val, val_len, &v, NULL) != BJ_OK) continue;
        q.nfl -= q.nfl % 3;

        /* Everything from here up is still owed to somebody: either not
         * handed out yet, or leased and possibly coming back. */
        uint64_t owed = q.next;
        for (int i = 0; i < FL_COUNT(&q); i++)
            if (FL_INDEX(&q, i) < owed) owed = FL_INDEX(&q, i);

        uint64_t f = owed > 0 ? owed - 1 : 0;
        if (f < *floor) *floor = f;
        (*ngroups)++;
    }
    bpt_cursor_close(c);
    return rc < 0 ? rc : BJ_OK;
}

/* ---- inspection ------------------------------------------------------ */

int bjm_subject_info(bjm_store *st, const char *subject_name,
                     uint64_t *base, uint64_t *last, uint64_t *bytes) {
    subject *s;
    int e = subject_get(st, subject_name, 0, &s);
    if (e) return e;
    *base  = elog_base_index(s->log);
    *last  = elog_last_index(s->log);
    *bytes = elog_file_len(s->log);
    return BJ_OK;
}

int bjm_subject_count(bjm_store *st, int *count) {
    *count = 0;
    DIR *d = fdopendir(dup(st->dirfd));
    if (!d) return BJ_ERR_STATE;
    const size_t suffix_len = sizeof SUBJECT_SUFFIX - 1;
    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        size_t n = strlen(ent->d_name);
        if (n > suffix_len &&
            strcmp(ent->d_name + n - suffix_len, SUBJECT_SUFFIX) == 0)
            (*count)++;
    }
    closedir(d);
    return BJ_OK;
}

/* ---- retention ------------------------------------------------------- */

/*
 * elog_compact writes the surviving entries into a second file, which the
 * host then adopts in place of the original. "Adopt" here is renameat
 * over the live name: the replacement is complete and fsynced before the
 * rename, and the rename itself is atomic, so a crash at any point leaves
 * either the whole old log or the whole new one — never a half-trimmed
 * file. bjns has no rename, but the store owns its directory fd, so this
 * is the store's business to do directly.
 */
int bjm_trim(bjm_store *st, const char *subject_name, uint64_t before,
             int force, uint64_t *out_base, uint64_t *removed) {
    subject *s;
    int e = subject_get(st, subject_name, 0, &s);
    if (e) return e;

    uint64_t base = elog_base_index(s->log);
    uint64_t last = elog_last_index(s->log);

    /* Messages below `before` go, so the new base is the one before it. */
    uint64_t new_base = before > 0 ? before - 1 : 0;
    if (new_base > last) new_base = last;

    if (!force) {
        /*
         * Two things can still be owed a message: a subscription that has
         * not acknowledged it, and a queue group that has not run it. The
         * lower of the two bounds wins.
         */
        uint64_t protect = UINT64_MAX;

        int nconsumers = 0;
        uint64_t min_acked = UINT64_MAX;
        e = bjm_consumer_stats(st, subject_name, &nconsumers, &min_acked);
        if (e) return e;
        if (nconsumers > 0) protect = min_acked;

        uint64_t qfloor = UINT64_MAX;
        int ngroups = 0;
        e = bjm_queue_floor(st, subject_name, &qfloor, &ngroups);
        if (e) return e;
        if (ngroups > 0 && qfloor < protect) protect = qfloor;

        if (protect != UINT64_MAX && new_base > protect) new_base = protect;
    }

    *out_base = base;
    *removed = 0;
    if (new_base <= base) return BJ_OK;      /* nothing to do */

    uint64_t new_base_term = 0;
    e = elog_term_at(s->log, new_base, &new_base_term);
    if (e) return e;

    char live[BJM_SUBJECT_MAX + sizeof SUBJECT_SUFFIX];
    char tmp[BJM_SUBJECT_MAX + sizeof SUBJECT_SUFFIX + 8];
    int nlive = snprintf(live, sizeof live, "%s%s", subject_name, SUBJECT_SUFFIX);
    int ntmp  = snprintf(tmp, sizeof tmp, "%s%s.tmp", subject_name, SUBJECT_SUFFIX);
    if (nlive < 0 || (size_t)nlive >= sizeof live ||
        ntmp < 0 || (size_t)ntmp >= sizeof tmp) return BJ_ERR_RANGE;

    bj_io dst;
    e = st->ns.open(st->ns.ctx, tmp, (uint32_t)ntmp,
                    BJ_NS_CREATE | BJ_NS_TRUNC, &dst);
    if (e) return e;

    e = elog_compact(s->log, &dst, new_base, new_base_term);
    if (e == BJ_OK && dst.sync) e = dst.sync(dst.ctx);
    st->ns.close(st->ns.ctx, &dst);
    if (e) {
        st->ns.remove(st->ns.ctx, tmp, (uint32_t)ntmp);
        return e;
    }

    /* Release the old file before it is replaced under us. */
    elog_free(s->log);
    s->log = NULL;
    st->ns.close(st->ns.ctx, &s->io);

    if (renameat(st->dirfd, tmp, st->dirfd, live) != 0) {
        st->ns.remove(st->ns.ctx, tmp, (uint32_t)ntmp);
        e = BJ_ERR_STATE;
        goto reopen;
    }
    st->ns.sync(st->ns.ctx);   /* the directory entry now points at the new file */
    e = BJ_OK;

reopen:
    if (st->ns.open(st->ns.ctx, live, (uint32_t)nlive, 0, &s->io) == BJ_OK) {
        s->log = elog_open(&s->io);
        if (!s->log) st->ns.close(st->ns.ctx, &s->io);
    }
    if (!s->log) {
        /* The subject is unusable in this process now. Drop it from the
         * table so the next request reopens from scratch instead of
         * dereferencing a dead handle. */
        memmove(s, s + 1, (size_t)((st->subs + st->nsubs) - (s + 1)) * sizeof *s);
        st->nsubs--;
        return e ? e : BJ_ERR_STATE;
    }

    *out_base = elog_base_index(s->log);
    *removed = *out_base > base ? *out_base - base : 0;
    if (*removed && st->policy) marks_prune(st, subject_name, *out_base);
    return e;
}

/* ---- applying a policy ------------------------------------------------ */

/*
 * The trim boundary this policy implies: the lowest index worth keeping.
 * Each dimension proposes one and the highest — the tightest limit —
 * wins, which is what "whichever comes first" means. 0 means no trim.
 */
static uint64_t policy_boundary(bjm_store *st, subject *s,
                                const bjm_policy *p, uint64_t now) {
    uint64_t base = elog_base_index(s->log);
    uint64_t last = elog_last_index(s->log);
    uint64_t live = last - base;
    if (live == 0) return 0;

    uint64_t before = 0;

    if (p->max_messages && live > p->max_messages) {
        uint64_t b = last - p->max_messages + 1;
        if (b > before) before = b;
    }

    if (p->max_bytes) {
        uint64_t bytes = elog_file_len(s->log);
        if (bytes > p->max_bytes) {
            /*
             * Bytes have to be converted to a count, and only the average
             * message size is known without walking the log. That makes
             * this approximate — but it always errs towards removing too
             * few, and retention runs again, so it converges rather than
             * overshooting.
             */
            uint64_t avg = bytes / live;
            if (avg == 0) avg = 1;
            uint64_t drop = (bytes - p->max_bytes + avg - 1) / avg;
            if (drop > live) drop = live;
            uint64_t b = base + drop + 1;
            if (b > before) before = b;
        }
    }

    if (p->max_age_s && now > p->max_age_s) {
        uint64_t cutoff = now - p->max_age_s;
        marks m;
        if (marks_load(st, s->name, &m) == BJ_OK) {
            /* The newest mark older than the cutoff: everything at or
             * below its index was published before the window opened. */
            uint64_t b = 0;
            for (int i = 0; i + 1 < m.n; i += 2)
                if (m.v[i + 1] < cutoff && m.v[i] + 1 > b) b = m.v[i] + 1;
            if (b > before) before = b;
        }
    }

    return before;
}

int bjm_retention_run(bjm_store *st, uint64_t now,
                      uint64_t *removed, int *trimmed) {
    *removed = 0;
    *trimmed = 0;
    if (!st->policy) {
        /* Never opened means no policy was ever set — but a store on disk
         * may still have one, so open it before concluding that. */
        int e = policy_open(st);
        if (e) return e;
    }

    /*
     * Collect the subject names first. Trimming calls back into this same
     * tree (bjm_policy_get, marks_prune), and those searches reuse the
     * tree's output buffer that an open cursor's entry points into.
     */
    char (*names)[BJM_SUBJECT_MAX + 1] = NULL;
    size_t n = 0, cap = 0;

    bpt_key min = { 1, 0, (const uint8_t *)"p/", 2 };
    bpt_key max = { 1, 0, (const uint8_t *)"p0", 2 };
    bpt_cursor *c = bpt_cursor_open(st->policy, &min, &max);
    if (!c) return BJ_ERR_STATE;

    bpt_key k;
    const uint8_t *val;
    size_t val_len;
    int rc;
    while ((rc = bpt_cursor_next(c, &k, &val, &val_len)) == 1) {
        if (k.str_len <= 2 || k.str_len - 2 > BJM_SUBJECT_MAX) continue;
        if (n == cap) {
            size_t ncap = cap ? cap * 2 : 8;
            void *p = realloc(names, ncap * sizeof *names);
            if (!p) { rc = BJ_ERR_OOM; break; }
            names = p;
            cap = ncap;
        }
        memcpy(names[n], k.str + 2, k.str_len - 2);
        names[n][k.str_len - 2] = '\0';
        n++;
    }
    bpt_cursor_close(c);
    if (rc < 0) { free(names); return rc; }

    for (size_t i = 0; i < n; i++) {
        int found = 0;
        bjm_policy p;
        if (bjm_policy_get(st, names[i], &found, &p) != BJ_OK || !found) continue;

        subject *s;
        if (subject_get(st, names[i], 0, &s) != BJ_OK) continue;  /* no log yet */

        uint64_t before = policy_boundary(st, s, &p, now);
        if (before == 0) continue;

        uint64_t new_base = 0, got = 0;
        if (bjm_trim(st, names[i], before, p.ignore_consumers,
                     &new_base, &got) != BJ_OK)
            continue;
        if (got) {
            *removed += got;
            (*trimmed)++;
        }
    }

    free(names);
    return BJ_OK;
}

int bjm_policy_list(bjm_store *st, const uint8_t **out, size_t *out_len) {
    int e = policy_open(st);
    if (e) return e;

    bpt_key min = { 1, 0, (const uint8_t *)"p/", 2 };
    bpt_key max = { 1, 0, (const uint8_t *)"p0", 2 };
    bpt_cursor *c = bpt_cursor_open(st->policy, &min, &max);
    if (!c) return BJ_ERR_STATE;

    bj_builder *b = st->bld;
    bj_builder_reset(b);
    bj_begin_array(b);

    bpt_key k;
    const uint8_t *val;
    size_t val_len;
    int rc;
    while ((rc = bpt_cursor_next(c, &k, &val, &val_len)) == 1) {
        if (k.str_len <= 2) continue;
        bjm_policy p;
        if (policy_decode(val, val_len, &p) != BJ_OK) continue;

        bj_begin_object(b);
        bj_put_key(b, (const uint8_t *)"subject", 7);
        bj_put_string(b, k.str + 2, k.str_len - 2);
        bj_put_key(b, (const uint8_t *)"max_age_s", 9);
        bj_put_int(b, (int64_t)p.max_age_s);
        bj_put_key(b, (const uint8_t *)"max_messages", 12);
        bj_put_int(b, (int64_t)p.max_messages);
        bj_put_key(b, (const uint8_t *)"max_bytes", 9);
        bj_put_int(b, (int64_t)p.max_bytes);
        bj_put_key(b, (const uint8_t *)"ignore_consumers", 16);
        bj_put_bool(b, p.ignore_consumers);
        bj_end_object(b);
    }
    bpt_cursor_close(c);
    if (rc < 0) return rc;

    bj_end_array(b);
    e = bj_builder_error(b);
    if (e) return e;
    *out = bj_builder_data(b, out_len);
    return *out ? BJ_OK : BJ_ERR_STATE;
}

/* ---- discovery ------------------------------------------------------- */

int bjm_subjects(bjm_store *st, const uint8_t **out, size_t *out_len) {
    DIR *d = fdopendir(dup(st->dirfd));
    if (!d) return BJ_ERR_STATE;
    rewinddir(d);

    bj_builder *b = st->bld;
    bj_builder_reset(b);
    bj_begin_array(b);

    const size_t suffix_len = sizeof SUBJECT_SUFFIX - 1;
    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        size_t n = strlen(ent->d_name);
        if (n <= suffix_len) continue;
        if (strcmp(ent->d_name + n - suffix_len, SUBJECT_SUFFIX) != 0) continue;
        bj_put_string(b, (const uint8_t *)ent->d_name, (uint32_t)(n - suffix_len));
    }
    closedir(d);

    bj_end_array(b);
    int e = bj_builder_error(b);
    if (e) return e;
    *out = bj_builder_data(b, out_len);
    return *out ? BJ_OK : BJ_ERR_STATE;
}
