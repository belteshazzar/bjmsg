# Five-terminal demo

The [NATS hello-nats tutorial](https://docs.nats.io/tutorials/hello-nats)
in three terminals, widened to five: one broker, one publisher, and three
subscribers all on the subject `greet`.

Build first:

```sh
make
```

Then one terminal each. Clients wait for the broker and reconnect if it
restarts, so only one ordering rule matters: **start the publisher last**,
because the subscribers skip whatever is already in the log.

| terminal | command | NATS equivalent |
| --- | --- | --- |
| 1 | `./demo/1-server.sh` | `nats-server` |
| 2 | `./demo/2-subscriber-alice.sh` | `nats sub greet` |
| 3 | `./demo/3-subscriber-bob.sh` | `nats sub greet` |
| 4 | `./demo/4-subscriber-carol.sh` | `nats sub greet` |
| 5 | `./demo/5-publisher.sh` | `nats pub greet "Hello NATS!"` |

Terminal 5 publishes a short scripted series and exits. Other ways to
drive it:

```sh
./demo/5-publisher.sh "any message you like"   # one message
./demo/5-publisher.sh --loop                   # every 2s until Ctrl-C
```

## What you should see

Every subscriber prints every message, each with the same index — the
publish happened once and all three read the same log position:

```
[alice] 23:44:42 [#1] Received on "greet"
  "Hello NATS!"
```

Ctrl-C stops any terminal. `./demo/reset.sh` deletes the store so the next
run starts again from message 1.

## Read-receipt mode

Prefix any subscriber with `DURABLE=1` and the broker persists how far it
has read, so stopping and restarting it delivers exactly what it missed:

```sh
DURABLE=1 ./demo/2-subscriber-alice.sh
```

Add a sixth terminal to watch the receipts:

```sh
./demo/6-receipts.sh
```

The demo worth running: start alice durable, publish one message, Ctrl-C
her, publish two more, then start her again. She receives only the two she
missed, and the receipt table shows her `lag` rising to 2 and back to 0.

```
[{"consumer":"alice","acked":1,"lag":2}]
```

Mix modes freely — run alice with `DURABLE=1` and leave bob and carol
ephemeral, and only alice catches up after a restart.

## Restarting the broker

Ctrl-C terminal 1 and start it again. The subscribers report that they
cannot reach it, retry every 5 s, and carry on where they left off once it
is back. The subscriptions themselves are stored in the broker, so it
reads them back on start and resumes delivering. Nothing else needs
restarting.

## Where this differs from NATS

- **Subscribers are HTTP servers.** `bjmsg sub` starts one, registers it
  as a callback, and the broker POSTs to it — so the connection runs from
  broker to subscriber, and delivery takes well under a millisecond. The
  reply to that POST is the acknowledgement; there is no second request.
  What it costs is reachability: the broker has to be able to dial the
  subscriber.
- **The backlog is real, and opt-in.** NATS core drops a message with no
  active subscriber. Here every message is durable in the subject's entry
  log, and the demo passes `--tail` to *choose* NATS's behaviour. Drop
  `--tail` from `_subscriber.sh` and a subscriber replays the subject from
  message 1 — including messages published before it existed.
- **Positions live in the broker, and outlive the subscriber if you name
  it.** An unnamed subscription takes its position with it when it stops;
  `DURABLE=1` gives it a name, and the receipt stays behind to resume
  from — closer to a JetStream durable consumer than to NATS core.
- **Wildcards are matched in the broker.** `bjmsg sub 'greet.>'` is one
  registration, and a subject created later is delivered from its first
  message.

## Configuration

`_env.sh` reads these from the environment, so any of them can be
overridden across all five terminals:

```sh
export PORT=9090 SUBJECT=orders.new
```

`BJMSG`, `HOST`, `PORT`, `URL`, `SUBJECT`, `DATA_DIR`.
