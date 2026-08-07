/*
 * bjtext.c — render a binjson value as JSON-ish text.
 *
 * Debug/CLI output only: it is not a binjson-to-JSON converter, because
 * the types JSON has no spelling for (BINARY, OID, DATE, POINTER) are
 * printed in an unambiguous but non-JSON form rather than lossily coerced.
 */
#include "bjmsg.h"

#include "binjson.h"

#include <string.h>

#define RENDER_MAX_DEPTH 64

typedef struct {
    FILE *f;
    int depth;                       /* 0 == top level                  */
    char kind[RENDER_MAX_DEPTH + 1]; /* 'a' array, 'o' object           */
    char first[RENDER_MAX_DEPTH + 1];/* nothing emitted at this depth yet */
    int after_key;                   /* next value belongs to a key     */
} rctx;

/* Emit the separator owed before a value at the current depth. */
static void sep(rctx *c) {
    if (c->depth == 0) return;
    if (c->after_key) { c->after_key = 0; return; }  /* the key wrote it */
    if (!c->first[c->depth]) fputc(',', c->f);
    c->first[c->depth] = 0;
}

static void push(rctx *c, char kind) {
    if (c->depth < RENDER_MAX_DEPTH) {
        c->depth++;
        c->kind[c->depth] = kind;
        c->first[c->depth] = 1;
    }
}

static void pop(rctx *c) {
    if (c->depth > 0) c->depth--;
}

static void put_quoted(FILE *f, const uint8_t *s, uint32_t len) {
    fputc('"', f);
    for (uint32_t i = 0; i < len; i++) {
        unsigned char ch = s[i];
        switch (ch) {
        case '"':  fputs("\\\"", f); break;
        case '\\': fputs("\\\\", f); break;
        case '\n': fputs("\\n", f); break;
        case '\r': fputs("\\r", f); break;
        case '\t': fputs("\\t", f); break;
        default:
            if (ch < 0x20) fprintf(f, "\\u%04x", ch);
            else fputc(ch, f);
        }
    }
    fputc('"', f);
}

static void put_hex(FILE *f, const uint8_t *b, uint32_t len) {
    static const char hex[] = "0123456789abcdef";
    for (uint32_t i = 0; i < len; i++) {
        fputc(hex[b[i] >> 4], f);
        fputc(hex[b[i] & 0xf], f);
    }
}

/* A double that is integral prints without a decimal point; INT and DATE
 * arrive as doubles already range-checked into the safe-integer range. */
static void put_num(FILE *f, double v) {
    if (v == (double)(long long)v) fprintf(f, "%lld", (long long)v);
    else fprintf(f, "%.17g", v);
}

static void on_null(void *ctx)              { rctx *c = ctx; sep(c); fputs("null", c->f); }
static void on_bool(void *ctx, int t)       { rctx *c = ctx; sep(c); fputs(t ? "true" : "false", c->f); }
static void on_int(void *ctx, double v)     { rctx *c = ctx; sep(c); put_num(c->f, v); }
static void on_float(void *ctx, double v)   { rctx *c = ctx; sep(c); put_num(c->f, v); }

static void on_string(void *ctx, const uint8_t *s, uint32_t len) {
    rctx *c = ctx; sep(c); put_quoted(c->f, s, len);
}

static void on_binary(void *ctx, const uint8_t *b, uint32_t len) {
    rctx *c = ctx; sep(c);
    fprintf(c->f, "bin(%u)0x", len);
    put_hex(c->f, b, len > 32 ? 32 : len);
    if (len > 32) fputs("...", c->f);
}

static void on_oid(void *ctx, const uint8_t *b) {
    rctx *c = ctx; sep(c); fputs("oid(", c->f); put_hex(c->f, b, 12); fputc(')', c->f);
}

static void on_date(void *ctx, double ms) {
    rctx *c = ctx; sep(c); fputs("date(", c->f); put_num(c->f, ms); fputc(')', c->f);
}

static void on_pointer(void *ctx, double off) {
    rctx *c = ctx; sep(c); fputs("ptr(", c->f); put_num(c->f, off); fputc(')', c->f);
}

static void on_array_begin(void *ctx, uint32_t n) {
    rctx *c = ctx; (void)n; sep(c); fputc('[', c->f); push(c, 'a');
}
static void on_array_end(void *ctx)  { rctx *c = ctx; fputc(']', c->f); pop(c); }

static void on_object_begin(void *ctx, uint32_t n) {
    rctx *c = ctx; (void)n; sep(c); fputc('{', c->f); push(c, 'o');
}
static void on_object_end(void *ctx) { rctx *c = ctx; fputc('}', c->f); pop(c); }

static void on_key(void *ctx, const uint8_t *k, uint32_t len) {
    rctx *c = ctx;
    if (!c->first[c->depth]) fputc(',', c->f);
    c->first[c->depth] = 0;
    put_quoted(c->f, k, len);
    fputc(':', c->f);
    c->after_key = 1;
}

/* ---- the no-op visitor ------------------------------------------------ */

static void n_void(void *ctx)                                { (void)ctx; }
static void n_int(void *ctx, int v)                          { (void)ctx; (void)v; }
static void n_dbl(void *ctx, double v)                       { (void)ctx; (void)v; }
static void n_u32(void *ctx, uint32_t v)                     { (void)ctx; (void)v; }
static void n_bytes(void *ctx, const uint8_t *p, uint32_t n) { (void)ctx; (void)p; (void)n; }
static void n_ptr(void *ctx, const uint8_t *p)               { (void)ctx; (void)p; }

bj_visitor bjm_visitor_noop(void *ctx) {
    bj_visitor v = {
        .on_null = n_void,          .on_bool = n_int,
        .on_int = n_dbl,            .on_float = n_dbl,
        .on_string = n_bytes,       .on_binary = n_bytes,
        .on_oid = n_ptr,            .on_date = n_dbl,
        .on_pointer = n_dbl,        .on_array_begin = n_u32,
        .on_array_end = n_void,     .on_object_begin = n_u32,
        .on_key = n_bytes,          .on_object_end = n_void,
        .ctx = ctx,
    };
    return v;
}

/* ---- envelope shape ---------------------------------------------------- */

/*
 * Only the first two callbacks matter: a top-level ARRAY of exactly two,
 * opening with an OBJECT. Whatever is nested below is the publisher's.
 */
typedef struct { int seen; int ok; } shape;

static void shape_array(void *ctx, uint32_t count) {
    shape *s = ctx;
    if (s->seen++ == 0 && count == 2) s->ok = 1;
}

static void shape_object(void *ctx, uint32_t count) {
    shape *s = ctx;
    (void)count;
    if (s->seen++ == 1 && s->ok) s->ok = 2;
}

static void shape_other(void *ctx) {
    shape *s = ctx;
    if (s->seen++ < 2) s->ok = 0;
}

static void shape_i(void *ctx, int v)                       { (void)v; shape_other(ctx); }
static void shape_d(void *ctx, double v)                    { (void)v; shape_other(ctx); }
static void shape_b(void *ctx, const uint8_t *p, uint32_t n){ (void)p; (void)n; shape_other(ctx); }
static void shape_p(void *ctx, const uint8_t *p)            { (void)p; shape_other(ctx); }

int bjm_envelope_shape_ok(const uint8_t *data, size_t len) {
    shape s = { 0, 0 };
    bj_visitor v = bjm_visitor_noop(&s);
    v.on_array_begin = shape_array;
    v.on_object_begin = shape_object;
    v.on_null = shape_other;
    v.on_bool = shape_i;
    v.on_int = shape_d;
    v.on_float = shape_d;
    v.on_string = shape_b;
    v.on_binary = shape_b;
    v.on_oid = shape_p;
    v.on_date = shape_d;
    v.on_pointer = shape_d;
    if (bj_decode(data, len, &v, NULL) != BJ_OK) return 0;
    return s.ok == 2;
}

/*
 * FORMAT.md: an ARRAY is the type byte, a uint32 content size, a uint32
 * element count, then the elements — so element 0 starts at offset 9.
 * The arithmetic is checked rather than trusted: the two element sizes
 * must account for exactly the rest of the buffer, which also proves
 * there are exactly two.
 */
int bjm_envelope_split(const uint8_t *data, size_t len,
                       const uint8_t **headers, size_t *headers_len,
                       const uint8_t **message, size_t *message_len) {
    const size_t hdr = 1 + 4 + 4;
    if (len <= hdr || data[0] != BJ_TYPE_ARRAY) return 0;

    size_t s0 = 0, s1 = 0;
    if (bj_value_size(data, len, hdr, &s0) != BJ_OK) return 0;
    if (hdr + s0 >= len) return 0;
    if (bj_value_size(data, len, hdr + s0, &s1) != BJ_OK) return 0;
    if (hdr + s0 + s1 != len) return 0;

    *headers = data + hdr;      *headers_len = s0;
    *message = data + hdr + s0; *message_len = s1;
    return 1;
}

int bjm_render(FILE *f, const uint8_t *data, size_t len) {
    rctx c;
    memset(&c, 0, sizeof c);
    c.f = f;
    c.first[0] = 1;

    const bj_visitor v = {
        on_null, on_bool, on_int, on_float, on_string, on_binary, on_oid,
        on_date, on_pointer, on_array_begin, on_array_end, on_object_begin,
        on_key, on_object_end, &c,
    };
    return bj_decode(data, len, &v, NULL);
}
