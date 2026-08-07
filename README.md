# bjmsg

Publish/subscribe over HTTP/1.1 with [binjson](https://github.com/mdy-docs/binjson)
payloads, as one executable that is either the broker or a client.

```sh
make
./bin/bjmsg serve &
./bin/bjmsg sub orders.new &
./bin/bjmsg pub orders.new "first message"
```

Nothing polls. `sub` starts a small HTTP server of its own and tells the
broker to POST matching messages to it, so a publish reaches a subscriber
in well under a millisecond and an idle broker costs nothing.

For a broker, three subscribers and a publisher across five terminals —
the [NATS hello-nats tutorial](https://docs.nats.io/tutorials/hello-nats)
shape — see [demo/](demo/).

## How it is built

Three vendored pieces, one copy of each:

| submodule | role |
| --- | --- |
| [`third_party/binjson`](third_party/binjson) | the wire format: `bj_builder` encoder, visitor-driven decoder |
| [`third_party/binjson-structures`](third_party/binjson-structures) | `entrylog` — the durable per-subject message log — over `bjio_posix` |
| [`third_party/http11c`](third_party/http11c) | the HTTP/1.1 server: single-threaded, kqueue/epoll, keep-alive |

binjson-structures carries its own nested `third_party/binjson` submodule
for its standalone build. We leave it uninitialised and point both builds
at the top-level copy instead, which is what its README asks a project
depending on both to do — two binjson checkouts in one binary is the
failure mode being avoided.

The client half is libcurl. http11c is a server library only, and writing
a second HTTP implementation to talk to the first one is not a good use of
anybody's time.

## A subject is an entry log

Each subject is one `<subject>.elog` file, and that single decision
supplies most of the broker:

- **Message ids** are the log's own indexes — contiguous, assigned by the
  log, starting at 1.
- **Payloads** are opaque bytes the log never interprets, so binjson
  messages pass through unexamined.
- **Durability** is `elog_sync`: one write plus a real fsync, with a CRC
  trailer. A torn tail is truncated back to the last good commit when the
  log is reopened, so a killed broker loses only unacknowledged publishes.
- **The subscribe response body** is `elog_get_batch`'s output verbatim —
  a binjson ARRAY of `{ index, term, type, payload }`. Nothing is decoded
  and re-encoded on the way out.

Raft's `term` is along for the ride at 0: there is no election here, and
elog's monotonicity rule permits it.

## Protocol

Success bodies are `application/binjson`; errors are `text/plain`, so a
bare `curl` against a broken request is readable.

| route | body / query | response |
| --- | --- | --- |
| `POST /pub/<subject>` | one binjson value, `?id=&ack_subject=&ack_consumer=&ack_index=` | `{ subject, index, acked, duplicate }` |
| `PUT /push/<subject\|pattern>` | `?consumer=&callback=[&token=&batch=&start=last\|&from=]` | `{ consumer, pattern, callback, created }` |
| `DELETE /push` | `?consumer=[&purge=1]` | `{ consumer, deleted, purged }` |
| `GET /push` | | ARRAY of `{ consumer, pattern, callback, delivered, inflight, failures, error }` |
| `POST <callback>` | ARRAY of `{ index, term, type, payload }`, plus `X-Bjmsg-*` | `2xx` acks; `X-Bjmsg-Ack: N` acks part |
| `GET /sub/<subject>` | `?from=&max=` | ARRAY of `{ index, term, type, payload }` |
| `GET /sub/<subject>` | `?consumer=&ack=&start=` | same, from the consumer's receipt |
| `POST /ack/<subject>` | `?consumer=&index=` | `{ subject, consumer, acked }` |
| `POST /trim/<subject>` | `?before=` or `?keep=` `[&force=1]` | `{ subject, removed, base, last }` |
| `POST /take/<subject>` | `?group=&max=&lease=` | ARRAY of `{ index, attempts, expires_ms, payload }` |
| `POST /done/<subject>` | `?group=&index=` | `{ subject, group, index, held }` |
| `POST /fail/<subject>` | `?group=&index=[&delay=]` | `{ …, held, retry_in_ms }` |
| `POST /requeue/<subject>` | `?index=` (into `<subject>.dead`) | `{ subject, from_dead_index, index }` |
| `GET /queue/<subject>` | | ARRAY of group states |
| `PUT /queue/<subject>` | `?group=&lease_ms=&max_attempts=&backoff_ms=&max_backoff_ms=` | the stored groups |
| `DELETE /queue/<subject>` | `?group=` | `{ subject, group, deleted }` |
| `GET /policy/<subject>` | | the subject's retention policy |
| `PUT /policy/<subject>` | `?max_age_s=&max_messages=&max_bytes=&ignore_consumers=` | the stored policy |
| `DELETE /policy/<subject>` | | `{ subject, cleared }` |
| `GET /policies` | | ARRAY of every policy |
| `GET /consumers/<subject>` | | ARRAY of `{ consumer, acked, lag }` |
| `DELETE /consumers/<subject>` | `?consumer=` | `{ subject, consumer, deleted }` |
| `GET /info/<subject>` | | `{ subject, base, first, last, messages, bytes, consumers }` |
| `GET /subjects` | `?pattern=` | ARRAY of subject names |
| `GET /health` | | `{ ok, backend, subjects, connections, uptime_s }` |

Subscribe also answers `X-Bjmsg-Count` and `X-Bjmsg-Last-Index`, so a
client can tell how far behind it is without decoding the body, plus
`X-Bjmsg-Acked` when a consumer is named.

Subject names are file names: 1–128 bytes of `[A-Za-z0-9_.-]`, no leading
or trailing dot, no `..`. A publish creates its subject; a subscribe to an
unknown subject is a 404 rather than an implicit create.

A publish body is checked to be **exactly one complete binjson value**
before it is accepted. The log would happily store anything, and the
malformed message would only surface as an undecodable payload in a
subscriber, after it was durable.

## Messages are pushed

A subscription is a **callback**: a URL the broker can reach. The broker
POSTs each batch to it, and **the HTTP response is the acknowledgement**.

```sh
bjmsg sub orders.new                     # starts a receiver, registers it
bjmsg sub orders.new --consumer billing  # ...durable
bjmsg push                               # what the broker is delivering to
```

That one decision buys three things that a streaming design would have to
work for:

- **Nothing in http11c had to change.** It cannot hold a response open —
  no chunked encoding, no SSE — which is what forced polling before. But
  a webhook needs none of that: the broker is an ordinary HTTP client and
  the subscriber an ordinary HTTP server, both of which already existed
  in this binary.
- **The ack needs no channel of its own.** Delivery and acknowledgement
  are one exchange on one kept-alive connection. `2xx` accepts the batch;
  `X-Bjmsg-Ack: N` accepts it as far as N and leaves the rest to be sent
  again.
- **Backpressure needs no policy.** The broker holds **at most one
  delivery in flight per subscription**, so a subscriber that is busy
  simply does not answer yet. Nothing queues up in the broker, nothing has
  to be bounded, and no slow consumer ever gets disconnected — it just
  lags, and the log is where the backlog was going to live anyway.

Each delivery carries what a subscriber needs without decoding anything:

| header | |
| --- | --- |
| `X-Bjmsg-Subject` | which subject this batch is from |
| `X-Bjmsg-Consumer` | which subscription it is for |
| `X-Bjmsg-First-Index` / `X-Bjmsg-Last-Index` | the range in the batch |
| `X-Bjmsg-Count` | how many messages |
| `X-Bjmsg-Lag` | how many are still waiting behind it |
| `Authorization: Bearer …` | the token the subscription registered with |

The body is byte-for-byte what `GET /sub` returns, so both paths decode
the same way.

### The costs, honestly

**The subscriber must be reachable.** The connection direction is
reversed: the broker dials out. That is right for a topology of instances
— and it is what makes a broker subscribing to another broker just an
ordinary subscription whose callback happens to be a broker — but a client
the broker cannot reach cannot be pushed to. `bjmsg sub` works around the
common case by binding to the local address it reaches the broker from
(libcurl's `CURLINFO_LOCAL_IP`, learned from the registration it had to
make anyway), so nothing needs configuring on one machine or one network.
`--callback` covers a port forward or a proxy.

**The broker ticks while delivering.** Outbound transfers run on libcurl's
multi interface, interleaved with `http11c_poll` from `bjm_serve`'s loop.
http11c does not expose its readiness fd, so with a delivery in flight the
loop asks for a 2 ms poll timeout instead of blocking — a local syscall,
not a request on a wire, and only while something is actually being
delivered. Idle, it blocks for a full second and burns nothing measurable.
Exposing that fd upstream would remove even the tick.

### Where a subscription starts, and where it keeps its place

A push subscription **holds no cursor of its own**. How far it has read is
its ordinary read receipt, which is why it shows in `bjmsg consumers`,
survives a broker restart, and counts against retention exactly like a
pull subscription would.

Where it *starts* is settled once, when it is registered — `?start=last`
skips whatever exists, `?from=N` picks an index, and the default replays
the log. Doing it at registration rather than at delivery is what makes
wildcards behave: a subject created *later* has no receipt, so it is
delivered from its first message. Deciding at delivery time would silently
drop whatever was published between that subject appearing and the broker
noticing it.

### Failure

A callback that does not answer is retried, doubling from 500 ms to a
30 s ceiling, **forever**. Giving up would mean deciding on the
subscriber's behalf that its messages no longer matter, and there is no
need to decide: the receipt is durable, so an unreachable subscription is
merely behind. `bjmsg push` shows the failure count and the last error.

A `2xx` with `X-Bjmsg-Ack: 0` is a soft refusal — "not now" — and takes
the same backoff, so a subscriber whose handler is failing does not spin.
`bjmsg sub --exec CMD` uses exactly that: a non-zero exit refuses that
message and everything after it in the batch, which are then redelivered
in order.

### Registration, and the one thing that is not push

`bjmsg sub` re-asserts its subscription every 30 s (`--heartbeat MS`,
`0` to disable). This is liveness, not polling: it carries no cursor and
asks for no messages. What it buys is self-healing — a broker whose store
was rebuilt has no record of the subscription, and silence is
indistinguishable from "nothing to send". Re-registering is a `PUT`, which
moves the callback and leaves the receipt alone, so it is also how a
subscriber that restarted on a different port says where it moved to.

On exit, `sub` unregisters. A named subscription keeps its receipt, so it
resumes; a throwaway one purges it, because a receipt holds retention off
everything below it and an abandoned one would pin the log for good.
`--keep` leaves the subscription registered so the broker keeps queueing.

### Security

The broker connects to a URL a client supplied, which deserves stating
plainly. Callbacks must be `http://` or `https://` and printable and
space-free (so nothing can smuggle a header into the delivery), and
redirects are not followed. That is syntax, not authorisation: **a broker
open to untrusted clients can be pointed at things on its own network**,
and should be behind something that decides which callbacks are allowed.

In the other direction, each subscription registers a bearer token that
the broker sends on every delivery — `bjmsg sub` generates a random one
unless given `--token` — so a subscriber refuses a POST that did not come
from the broker it registered with. This is the subscriber authenticating
the broker; consumer names are still asserted rather than authenticated,
so anything that can reach the broker can claim one.

### `GET /sub` is still there

Pull still exists, as the way to *read* a subject rather than subscribe to
one: browsing, one-shot tooling, `bjmsg dead`. Nothing subscribes with it.

## Reconnecting

Every client waits and retries when the broker cannot be reached, every
5 s by default. `--retry MS` changes the wait; `--retry 0` turns it off
and fails on the first refusal.

```sh
bjmsg sub greet                          # survives the broker restarting
bjmsg sub greet --retry 500              # ...more eagerly
bjmsg pub greet "hi" --retry 0           # fail now rather than wait
```

So a subscriber can be started before the broker exists, and keeps its
place across a broker restart. A subscription that is already registered
needs no reconnecting at all — the broker restarts, reads it back out of
`_push.bpt`, and carries on delivering.

Only connection failures are retried — an HTTP status is the broker
answering, and a 404 or 415 is not going to become a 200. Which failures
qualify depends on the request:

- **Never reached the broker** (connection refused, DNS failure): always
  retried, since there was no effect to repeat.
- **Broke partway** (send/receive error, timeout): retried only for
  requests that are safe to repeat — every `GET`, and `POST /ack`, whose
  receipt cannot move backwards. A bare `POST /pub` is **not**: the
  message may already be in the log, and retrying would append it twice,
  so it is reported rather than silently duplicated. Give it an
  idempotency key and it becomes retryable like everything else.

## Producer idempotency

A publish carrying an id is deduplicated, so repeating it is free:

```sh
bjmsg pub orders order-42 --id order-42
{"subject":"orders","index":7,"duplicate":false}
bjmsg pub orders order-42 --id order-42
{"subject":"orders","index":7,"duplicate":true}     # nothing appended
```

`--auto-id` generates a key for one invocation instead, which makes *that
publish* safe to retry without you having to invent a key. It is
deliberately not derived from the payload: two identical messages are
legitimately two messages.

That is what closes the producer half of effectively-once processing. The
consumer half you already have for free — indexes are stable and never
shift, so `(subject, index)` is a permanent dedup key a consumer can
record atomically with its own output:

```
BEGIN; insert result; update last_index = 42; COMMIT;   -- skip anything <= 42
```

Exactly-once *delivery* is not a thing anyone can offer; this is the pair
of mechanisms that gets you exactly-once *effect*.

## Headers

A message can carry headers. They live in the entry log's **type byte** —
which is already in every subscribe response — rather than in a wrapper
every message has to pay for:

| entry type | payload |
| --- | --- |
| `0x01` plain | the message, byte-for-byte as published |
| `0x10` envelope | `[ headers, message ]` |

So a headerless message is untouched and the subscribe path still
forwards `elog_get_batch`'s bytes verbatim; only messages that actually
have headers carry the array. A subscriber tells them apart by the `type`
field it already receives.

```sh
bjmsg pub orders "an order" --header source=web --header trace=abc123
bjmsg sub orders
2  [{"source":"web","trace":"abc123"},"an order"]
```

Headers are **opaque to the broker** — it checks the shape on publish, so
a subscriber trusting the type byte cannot trip over a mislabelled
payload, and looks at nothing else. They are not stored as HTTP headers
because http11c has no way to enumerate request headers, only to look one
up by name.

## Request-reply

```sh
bjmsg reply echo --exec 'tr a-z A-Z'      # a responder (run as many as you like)
bjmsg request echo "hello world"          # → "HELLO WORLD"
```

The request carries `reply_to` and `correlation` headers. The reply goes
to `reply_to` with the same correlation, and the requester matches on it —
so many requesters share one reply subject (`_reply` by default) without
needing one each. `--timeout` bounds the wait and exits 1 if it passes.

Two details that make it hold up:

- The requester reads the reply subject's **current end before publishing
  the request**, so a reply that arrives before it starts polling is not
  missed.
- Responders share a **queue group**, so each request is handled once no
  matter how many are running, and the reply is published with an
  idempotency key derived from the correlation — a redelivered request
  cannot produce a second reply.

Measured: 12 concurrent requesters against 3 competing responders, all 12
correlated correctly, 12 requests and 12 replies.

A request whose handler needs no answer is fine too — a message with no
`reply_to` is run and acknowledged without a reply.

## Effectively-once pipelines

`bjmsg pipe` reads a subject, transforms each message, and publishes the
result to another — with the guarantee assembled rather than assumed:

```sh
bjmsg pipe orders --consumer enrich --to orders.enriched --exec ./enrich.sh
```

Two things make it hold. The output carries an **idempotency key derived
from the input index** (`<consumer>.<subject>.<index>`), so rerunning a
message produces the same key and collapses onto the output already
there. And the publish **carries the input's acknowledgement with it** —
one call, one response:

```
POST /pub/<out>?id=<key>&ack_subject=<in>&ack_consumer=<c>&ack_index=<n>
```

Those are still two writes to two files and cannot be one atomic write.
The **order** is what carries the guarantee: publish, then ack. A crash in
between replays the input, the handler reruns, and the republished output
is deduplicated — so the effect is once. The other order would acknowledge
an input whose output never landed, and lose the message.

Measured, not assumed: 20 messages through a slow handler with the
pipeline `kill -9`'d six times mid-stream produced **20 outputs, zero
duplicates, zero missing**.

The handler contract is a shell filter — payload on stdin, replacement on
stdout, with `BJMSG_SUBJECT` / `BJMSG_CONSUMER` / `BJMSG_INDEX` in the
environment. Empty stdout drops the message but still acknowledges it; a
non-zero exit leaves it unacknowledged to be retried. `--raw` swaps the
text rendering for encoded binjson on both sides, which is what preserves
types through a pipeline.

### Where this stops

The broker can only join writes that are its own. If your handler's real
effect is somewhere else — a database row, an HTTP call — no amount of
broker machinery makes that atomic with the cursor. What you get there is
at-least-once plus the tools to be idempotent: `BJMSG_INDEX` is stable and
never shifts, so the handler can record it in the same transaction as its
own output and skip anything it has already seen. That is the same
pattern as above, with your store playing the part the broker plays here.

### The window

Ids are remembered for `--dedup-window` seconds (default 120), which is
sized for retries rather than for history. Two generations of the index
are kept: writes go to the current one, lookups check both, and once a
window elapses the older is cleared in O(1) — `bpt_reset` truncates an
append-only file — and becomes the new current.

So an id is remembered for **at least one window and at most two**, with
bounded space, no per-entry deletions to accumulate, and no compaction to
schedule. An id found in the older generation is copied forward, so one
still in active use is not dropped by the next rotation. Which generation
is current survives a restart.

An id is opaque to the broker: 1–128 printable bytes, no `/` (which
separates subject from id in the index key).

## Wildcard subscriptions

Subjects match token-wise on `.` — no regular expressions:

| pattern | matches | does not match |
| --- | --- | --- |
| `orders.*` | `orders.us`, `orders.eu` | `orders`, `orders.us.new` |
| `orders.>` | `orders.us`, `orders.us.new` | `orders` |
| `orders.*.new` | `orders.us.new` | `orders.new` |
| `>` | everything | |

`*` takes exactly one token, `>` takes that token and everything below
it, and `>` is only legal last. Neither character is legal in a subject
name, so a pattern can never be mistaken for one — nothing has to say
which it is.

```sh
bjmsg sub 'orders.*' --follow
orders.eu   1   "an order"
orders.us   1   "another"

bjmsg subjects 'orders.>'
```

Output gains a subject column, because **each matched subject keeps its
own receipt**. That is the whole of the design: subjects have independent
index spaces, so a wildcard subscription is N ordinary subscriptions
discovered by pattern instead of by name. Durable ones need no new
storage at all — receipts are already keyed `<subject>/<consumer>`, so
`sub 'orders.*' --consumer w` is simply a receipt per match, and
`bjmsg consumers orders.us` lists it like any other.

Matching happens **in the broker**, which is where the subjects are. A
pushed wildcard subscription is one registration, and the broker walks its
matches round-robin — so no subject starves behind a busy one, and a
client learns about a new subject by being sent a message from it rather
than by asking whether one appeared.

Two things worth knowing:

- **There is no order across subjects.** Each is delivered in its own
  order and the interleaving means nothing — `orders.us` index 5 and
  `orders.eu` index 5 are unrelated messages. If you need a total order
  over a set, they have to be one subject.
- **New subjects are picked up the moment they exist**, with nothing
  re-resolving: every append tells the broker, and a subject with no
  receipt for this consumer starts at its first message. `--tail` applies
  only to what existed when the subscription was registered, since
  nothing in a later subject predates it.

## Read receipts: durable subscriptions

`sub --consumer NAME` hands the cursor to the broker instead. It persists
a **read receipt** — the highest index that consumer has acknowledged —
so rejoining delivers exactly what was missed and nothing else:

```sh
bjmsg sub work --consumer w1             # ...run it, stop it, run it again
bjmsg consumers work
[{"consumer":"w1","acked":8,"lag":0}]
```

Receipts live in one B+ tree for the whole store (`_cursors.bpt`), keyed
`<subject>/<consumer>`. Neither name may contain `/`, so the key parses
back unambiguously and one subject's consumers are a contiguous range —
which is what `GET /consumers/<subject>` scans. A receipt only ever moves
forward, so a late or duplicated ack cannot rewind a consumer into replay.

For a pushed subscription the ack costs no request at all: it **is** the
response to the delivery, and the broker fsyncs the receipt before
choosing the next batch. `POST /ack/<subject>?consumer=&index=` remains
for moving a receipt by hand (`bjmsg seek`).

Delivery is **at-least-once**. An ack commits with a CRC, so it survives
the broker process dying, but it is not fsynced — that second fsync per
batch would buy only the difference between "redelivered after a power
cut" and "not redelivered", and a consumer has to tolerate redelivery
regardless. The explicit shutdown ack *is* fsynced, since losing that one
replays a batch the subscriber just finished.

Consumers are independent: each has its own receipt, and every consumer
sees every message. This is fan-out, not a work queue — two processes
sharing one consumer name would each advance the same receipt and skip
each other's messages.

A subscription exists from its first use until it is deleted:

```sh
bjmsg unsubscribe work --consumer w1     # forget the receipt entirely
bjmsg seek work --consumer w1 --index 40 # or just move it forward
```

`seek` only moves a receipt forward, keeping the invariant that receipts
never rewind. To replay a subject, delete the subscription and rejoin.

## Job queues

A **queue group** turns a subject into a work queue: each message goes to
exactly one member of the group instead of to all of them.

```sh
bjmsg work jobs --group workers --exec ./handle-job   # run one per job
bjmsg queue jobs                                      # what the groups are doing
```

`work` takes one job at a time, writes the payload to the command's stdin
with `BJMSG_SUBJECT` / `BJMSG_GROUP` / `BJMSG_INDEX` / `BJMSG_ATTEMPTS` in
its environment, and finishes the job if the command exits 0 or returns it
to the queue otherwise. Run as many as you like; they compete. The
primitives underneath are `take`, `done` and `fail` if you would rather
drive it yourself.

### Why a receipt could not do this

A durable subscription's state is one number, and competing consumers
break that immediately: worker A can still be on job 5 when worker B
finishes job 6, and no single high-water mark says so. A group therefore
keeps two things:

- **`next`** — the lowest index never yet handed out.
- **an in-flight table** — index → `(lease expiry, attempts)` for jobs
  taken but not finished.

Which is cheaper than it sounds, because *done needs no storage*: below
`next`, a job is either in the table or it is finished, so completion is
the absence of an entry. The table is bounded by how many jobs are being
worked on at once, never by how many have ever run. A million-job queue
with eight workers has eight entries.

### Leases, redelivery, and giving up

A taken job is held for `--lease` (default 30s). If it is neither finished
nor failed by then, the lease expires and the job goes back — so a worker
that dies loses nothing. Expired leases are handed out before untouched
messages, so a retry is not starved behind the queue.

That makes delivery **at-least-once**, and the reason is worth stating
plainly: the broker cannot tell a dead worker from a slow one, so a slow
job is run twice. Jobs must be idempotent. `BJMSG_ATTEMPTS` above 1 is a
handler's warning that it is seeing a job again.

A failed job does not come straight back. It waits `--backoff` (default
1s), **doubling with each attempt** up to `--max-backoff` (default 5m):

```sh
bjmsg queue jobs --group workers --backoff 1s --max-backoff 5m
bjmsg fail jobs --group workers --index 42 --delay 30s   # override for one job
```

Some delay is essential, not a nicety. A job failed with no delay is due
instantly, and the worker that just failed it is the one most likely to
ask next — so it takes the same job straight back and a failing job spins
as fast as the network allows. With backoff, the retries spread out and
other jobs keep flowing past. `--backoff 0` restores instant retry.

`--max-attempts` (default 10) is the other half. A job delivered that many
times without finishing stops being retried:

`--lease 0` opts out of the whole mechanism: jobs are taken and forgotten,
which is at-most-once and loses the job if the worker dies.

### The dead-letter channel

A job that runs out of attempts is republished to **`<subject>.dead`**,
which is an ordinary subject — so every tool that works on a subject works
on it. Inspect it, bound it with a retention policy, even consume it with
its own queue group.

```sh
bjmsg dead jobs
1  workers  orig=3  attempts=3  "poison task"

bjmsg requeue jobs --index 1     # put it back after fixing the handler
{"subject":"jobs","from_dead_index":1,"index":4}
```

The message there is an **envelope** — `{ subject, group, index, attempts,
failed_ms, payload }` — because the payload alone does not say which group
gave up on it or how many times it was tried. `bjmsg dead` renders it; a
plain `sub jobs.dead` shows the envelope with the payload as hex, since
the payload is stored as BINARY so requeue can hand the exact original
bytes back to the log.

`requeue` publishes to the subject the **envelope names**, not the one you
asked for, so it cannot be used to move a message between subjects. The
message is appended with a **new index** — the original is still in the
log and still dead, and reusing its id would misrepresent the ordering.
The dead-letter record stays put, so the history of what failed is not
erased by fixing it.

A subject already ending in `.dead` gets no channel of its own, which
stops `jobs.dead.dead.dead` when a group is consuming a dead-letter
channel and its jobs also fail.

### What you give up

- **Ordering.** Job 6 can finish before job 5. Inherent to competing
  consumers; if you need per-key ordering, use one subject per key.
- **Pull, not push.** NATS pushes round-robin to queue-group members;
  http11c cannot push, so a worker asks when it wants work. That is
  work-stealing, which balances load better — a slow worker simply asks
  less often instead of accumulating a backlog it cannot serve.

Queue groups and fan-out subscriptions are independent state on the same
subject, so a log can be tailed for audit while being consumed as a queue.
Retention knows about both: a trim will not discard a job that is leased
or has not been handed out yet.

## Query commands

These connect to a running broker, ask one question, print the answer and
exit. They neither publish, subscribe, nor serve:

```sh
bjmsg health              # {"ok":true,"backend":"kqueue","subjects":3,...}
bjmsg subjects            # ["logs","orders.new"]
bjmsg info logs           # {"subject":"logs","base":15,"first":16,"last":20,...}
bjmsg consumers logs      # [{"consumer":"w1","acked":18,"lag":2}]
bjmsg policy              # every retention policy
```

`info` is the one to read when reasoning about retention: `base` is where
trimming has cut to, `first` is the oldest message still readable, and
`bytes` is the file on disk.

## Retention policies

A subject can carry a policy the broker enforces on its own, sweeping
every 10 seconds:

```sh
bjmsg policy logs --max-age 7d --max-messages 1000000 --max-bytes 2G
bjmsg policy logs            # show one
bjmsg policy                 # list every subject that has one
bjmsg policy logs --clear
```

Durations take `s`/`m`/`h`/`d`/`w` and sizes take `K`/`M`/`G`/`T`; bare
numbers are seconds and bytes. Any dimension left unset is unlimited.

**Several limits can apply at once**: each proposes a trim boundary and
the tightest one wins, so whichever limit is reached first is the one that
acts. Setting `--max-age 7d --max-bytes 2G` means "a week of history, but
never more than 2 GB", and either can be the binding constraint at
different times.

By default a policy will not discard a message a subscription has not
read — retention loses to a read receipt. That is the safe default and
also the dangerous one, because a single forgotten consumer then pins the
log forever. `--ignore-consumers` inverts it and makes the bound real:

```sh
bjmsg policy logs --max-messages 1000 --ignore-consumers
```

A note on each dimension:

- **`--max-messages`** is exact.
- **`--max-bytes`** is approximate. Converting a byte budget to an index
  needs a per-message size, and only the average is known without walking
  the log — so a sweep removes slightly too few and the next sweep
  finishes the job. It converges rather than overshooting.
- **`--max-age`** is approximate, and needs state the log does not have.
  Entry logs store no timestamps, and payloads are opaque, so the store
  keeps its own sparse index: a bounded ring of `(index, time)` marks per
  subject, one written every `max_age / 128` seconds and only for
  subjects that actually have an age policy. Resolution is that interval
  — a 7-day policy marks roughly hourly — so a trim keeps at most one
  interval more than asked. Erring towards keeping is the safe direction,
  and it costs the publish path one small write per interval instead of a
  timestamp index per message.

## Trimming by hand

`trim` does the same thing immediately, with no policy involved:

```sh
bjmsg trim logs --keep 1000     # keep the newest 1000 messages
bjmsg trim logs --before 5000   # drop everything below index 5000
```

This is `elog_compact`: the surviving entries are rewritten into a second
file which is fsynced and then `renameat`d over the original. The rename
is atomic, so a crash at any point leaves either the whole old log or the
whole new one, never a half-trimmed file. **Indexes never shift** —
trimming raises the log's `base`, so a message keeps the id it was
published with forever.

By default the boundary is **clamped to the lowest read receipt**, so
trimming can never discard a message a subscription has not read:

```sh
bjmsg trim logs --keep 2          # {"removed":3,...}  clamped: a consumer was behind
bjmsg trim logs --keep 2 --force  # {"removed":5,...}  discarded its unread messages
```

What a reader sees after a trim depends on which kind of cursor it holds:

- A plain `--from` cursor below the boundary is **clamped** to the oldest
  surviving message, and the client prints how many it missed. "Read this
  subject from the start" should mean the start of what exists.
- A **consumer** cursor gets a `416` instead, naming both ways out:
  `bjmsg seek` to move it to the new base, or `bjmsg unsubscribe` to start
  over. A receipt is a claim about what was delivered, so skipping it
  silently would hide the loss.

## Known limits

- **One fsync per publish.** `elog_append` only buffers and `elog_sync`
  commits a whole batch, so the log is built for amortising this — but the
  sync has to happen before the `200` goes out, and http11c sends the
  response as soon as the handler returns. Batching needs deferred
  responses first.
- **A pushed subscriber must be reachable by the broker.** That is the
  webhook trade, and it is the right one for a topology of instances, but
  a client behind a NAT the broker cannot traverse has no push path.
  `--callback` covers a port forward; anything else would need the
  streamed/long-poll design that this deliberately avoided.
- **The broker polls its own event loop while delivering** — a 2 ms
  timeout on `http11c_poll` instead of blocking, because http11c does not
  expose its readiness fd for libcurl's sockets to be waited on alongside.
  It costs nothing when idle and nothing measurable in flight, but a
  `http11c_pollfd()` upstream would remove it.
- **Queue-group workers still poll.** `bjmsg work`, `bjmsg reply` and
  `bjmsg pipe` drive `POST /take` on an interval. The delivery engine
  generalises to them — a leased job POSTed to a callback, `2xx` meaning
  done and anything else meaning fail — and that is the next piece.
- **A queue group's in-flight table is capped** at 256 jobs. Past that a
  `take` returns nothing until acks come in, which is backpressure rather
  than an error — but it does bound how many jobs one group can have
  running at once.
- **Nothing bounds a dead-letter channel automatically.** It is a normal
  subject, so give it a retention policy —
  `bjmsg policy jobs.dead --max-age 30d` — or it grows without limit.
- **Consumer names are asserted, not authenticated.** Any client claiming
  a name advances that subscription's receipt, and can repoint its
  callback. A broker reachable by untrusted clients also needs its
  outbound callbacks restricted; validating the URL's syntax is not the
  same as deciding where the broker may connect.
- **The retention sweep is O(subjects with policies).** Fine for tens or
  hundreds; a store with very many policied subjects would want the sweep
  to prioritise rather than walk them all every 10 s.
- **No TLS and no auth.** http11c does not do TLS; a reverse proxy is the
  answer.

## Building

Needs a C11 compiler and libcurl (`curl-config` on `PATH`).

```sh
git submodule update --init      # top-level submodules only
make                             # -> bin/bjmsg
```

`-DBJIO_REQUIRE_SYNC` is on, so binjson-structures rejects at open time
any io that is writable but cannot fsync — a shipping broker should never
be silently non-durable. Our own sources build with `-Werror`; the
vendored ones deliberately do not.
