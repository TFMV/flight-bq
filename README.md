# Flight-BQ

Flight SQL, stripped down to what actually matters: a bounded, streaming execution surface over BigQuery.

---

## What This Actually Is

Flight-BQ is a **concurrency-aware, resource-bounded execution layer** that sits between ADBC (BigQuery) and Arrow Flight SQL clients. It does one thing well: move data from BigQuery to the client as a stream, without losing control of the system along the way.

It is built on a few non-negotiables:

### Arrow or Nothing

Every result is an `array.RecordReader`. No row materialization. No hidden buffering. If it doesn’t fit in a stream, it doesn’t belong here.

### Streaming Has Consequences

Streams terminate once. Errors propagate once. Memory is retained and released deliberately. Context cancellation is not advisory — it is enforced.

### Bounded Systems Survive

Sessions, handles, goroutines — all of them are capped, tracked, and evicted. If something leaks, it’s a bug. If something grows unbounded, it’s a design failure.

---

## Architecture

### Server Boundary (`flightsql.go`)

This is the edge. gRPC comes in, Flight SQL goes out. Handles are validated, sessions are assigned, and requests are routed without ceremony.

### Session & Handle Management (`session.go`, `handle.go`)

**SessionManager**

* Fixed upper bound on active ADBC connections
* Backpressure via channel signaling — when full, callers wait
* TTL-based eviction in the background

**HandleStore**

* Opaque, unguessable tokens
* Single-use by design (GetFlightInfo → DoGet)
* Forced expiration if abandoned

If a client walks away, the system eventually forgets them.

### Statement Layer (`statement.go`)

Thin, defensive wrapper around ADBC statements:

* Mutex-protected
* Idempotent close
* No use-after-close surprises

It assumes clients will misbehave — and survives anyway.

### Streaming Bridge (`bridge/stream.go`)

This is where things either work or fall apart.

* Converts synchronous `RecordReader` → async Flight stream
* Channels carry batches, not promises
* Context cancellation tears everything down cleanly

If the client disappears mid-stream, nothing lingers. No stuck goroutines. No orphaned Arrow buffers.

### Configuration (`config.go`)

Typed config, not string soup.

`BigQueryConfig` defines the contract at the boundary so the driver layer doesn’t become a pile of fragile maps.

---

## Flight SQL Surface Area (Reality Check)

| Feature             | Status | Notes                                                                                             |
| ------------------- | :----: | ------------------------------------------------------------------------------------------------- |
| Ad-hoc Queries      |    ✅   | Core path. Fully streaming.                                                                       |
| DoGet (Fetch)       |    ✅   | Single-use opaque tickets.                                                                        |
| Prepared Statements |   ⚠️   | Structurally supported. Performance depends entirely on the driver. BigQuery won’t save you here. |
| Transactions        |   ⚠️   | Passed through. BigQuery’s model limits usefulness.                                               |
| Metadata APIs       |   ⚠️   | Whatever the driver returns is what you get.                                                      |
| ExecuteUpdate       |    ❌   | Not implemented.                                                                                  |

If the underlying driver cuts corners, this layer won’t pretend otherwise.

---

## Where This Breaks (and Why)

This system is honest about its limits. That’s intentional.

### Backpressure Isn’t Finished

Channel buffers + blocking writes are a start, not a solution.

Slow or adversarial clients can still stall producers before cancellation propagates. Real flow control (windowing, adaptive strategies) is still missing.

### BigQuery Isn’t a Database (Operationally)

Pooling exists here, but BigQuery runs remote jobs.

Reusing sessions doesn’t necessarily buy you anything and can introduce drift (auth, state). In many cases, short-lived or stateless execution is the more honest model.

### Observability Is a Hook, Not a System

You get `MetricsHook` and `Logger`. That’s it.

No aggregation. No export pipeline. No cardinality control. At scale, this will hurt unless you build something on top.

### No Real Admission Control

There are caps on sessions, but not on *work*.

One tenant can still dominate the system. Large scans can starve everything else. There’s no fairness model yet — no queueing, no prioritization.

### Memory Is Bounded… Until It Isn’t

Streaming avoids full buffering, but batches still exist concurrently.

There is no global memory accounting or per-query limit. At high concurrency, this becomes the next failure mode.

---

## Testing Philosophy

Happy paths are easy. Failure is where systems prove themselves.

* **Cancellation Load Test** (`load_cancel_test.go`)

  * 50 concurrent queries
  * 30 canceled mid-stream
  * Verifies: no goroutine leaks, sessions released, memory stabilizes

* **Race Detector**

  Everything runs clean under `-race`. If it doesn’t, it doesn’t ship.

---
