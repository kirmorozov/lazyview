# Design: refresh supervisor (concurrent, adaptive, overload-safe)

> **Not implemented.** None of the variables, metrics, states or files below exist yet; see
> [configuration](configuration.md) and [operations](operations.md) for current behaviour. Paths
> under `tmp/` refer to local, git-ignored benchmark scripts and output that are not in the
> repository.

Status: proposal. Scope: the flush path of `src/engine.rs`, meaning refreshes of live views and of changes
buffered during builds. Goal: the lowest change-to-MV latency the databases can sustain without overloading
them. When they are slow, lazyview falls back to today's sequential behaviour instead of piling up work.

## 1. Evidence (tmp/out/bench: mysql 8.4, shared 12-core host, load avg 15–40)

| observation | numbers | consequence |
|---|---|---|
| engine CPU is negligible | 2–4 % of one core, 25–60 µs/change, RSS < 21 MB | not CPU-bound; offload only big diffs (§4.5) |
| flush is serial, round-trip bound | flush p50 0.5–1.2 s, p95 ≈ 3 s; flushing 47–81 % of the time; 1-row commit p50 119 ms (`sync_binlog=1`, `flush_log_at_trx_commit=1`) | overlap round trips; concurrent commits share fsyncs |
| binlog not read while flushing | spj_single engine-bound at ~550 rows/s; catch-up 0 → 12k → 0 → 20k rows/s | keep reading during a flush (§3) |
| lookup-heavy views starve | lookup_fanout: 8.8k units/flush, 0 rows/s applied during load, lag 15.7 s | parallel lookups and chunk reads |
| freshness at 8 writers × 50 rows | p50 0.46–1.1 s, p95 0.9–3.0 s | target p50 ≈ window + one read→write chain |
| shared DB host | mysqld 30–50 % CPU, host 50–80 % busy | concurrency must be capped and adaptive (§4) |

Priorities follow: (1) parallel refresh, (2) pipelining, (3) adaptive limiter, (4) CPU offload.

## 2. Concurrency structure

| pool | server | used for | cap |
|---|---|---|---|
| `read` | source | fence, lookups, refresh SELECTs | `LAZYVIEW_MAX_SOURCE_CONCURRENCY` (8) |
| `write/<t>` | each target, incl. `source` | old MV rows, diff, DELETE+INSERT txn | `LAZYVIEW_MAX_TARGET_CONCURRENCY` (4) |

Dedicated connections stay as today: binlog, `meta` (lock, position, state), `src` control (catalog, probe),
one control connection per target (swaps, DDL, table checks), and builds (own connections,
`LAZYVIEW_BUILD_CONCURRENCY`). Each pool is a `mysql_async::Pool` with `PoolConstraints::new(0, max)`,
`with_reset_connection(false)`, `setup(["SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"])` (re-run
after any reset; replaces `mv::init_session`) and `inactive_connection_ttl` 30 s, so connections above the
current limit close. The pool caps connections; the limiter (§4) caps how many are in use.

**Flush job.** Input: one `ViewJob` per live, incremental, non-frozen version (`Arc<ViewPlan>`, `Layout`,
table, targets, the taken `ViewDirty`). One flush task runs `supervisor::run(jobs)`:
1. **Fence** once on a read connection: `WAIT_FOR_EXECUTED_GTID_SET(G_cut, 5)`, or the visibility delay.
2. **Lookups**, per view: one op per image chunk, merged into the view's `UnitSet`. Its chunk ops start when
   its lookups are done; other views proceed meanwhile.
3. **Chunk op**: take a read permit, `mv::read_source`, release it. Prepare the new rows once (Datums +
   grouping, shared via `Arc`; today this is redone per target). Then fan out concurrently: one **write op**
   per target takes a `t` write permit, runs the old-row SELECT, the diff and the DELETE+INSERT
   transaction, commits and releases.
4. **Scheduling**: views in ascending dirty size (small views finish first), round-robin across views for
   read permits. A chunk is materialized (a new `UnitSet::chunks_lazy`; today only `UnitSet::chunks`
   exists) and spawned (JoinSet) only after its read
   permit and row admission (§4.5) are granted.
5. **Chunk size** `c = clamp(ceil(U / (2·P)), 100, LAZYVIEW_QUERY_CHUNK)`, with U = the view's units and
   P = min(⌊L_read⌋, min over its targets of ⌊L_write⌋). 600 units at P = 4 means 6 chunks of 100 in flight
   instead of 1 serial chunk; at P = 1 chunks are as today.
6. **Rules**: an op never holds a read and a write permit at once, so there is no deadlock. A unit is in
   exactly one chunk (UnitSet dedup, lookups merged before chunking). An MV with no unit index (neither
   PRIMARY nor `_lv_unit`, e.g. a JSON unit) gets write concurrency 1 per (view, target), because full-scan
   DELETEs would lock-wait on each other. For indexed MVs under READ COMMITTED, disjoint units never overlap
   in row locks.
7. **Result** per view: stats, or a **carry-over** (units of failed chunks, unresolved images, flags) merged
   back into the live buffer. That is per chunk, not per view as today.

## 3. Pipelining

`Engine::flush()` splits in two:
* **Data flush**: refreshes the live incremental versions in the background, at most one at a time.
* **Control flush**: definition reloads, re-plans, `finish_next` (buffered changes, then swaps), starting
  full-mode / `rebuild` builds, dropping retired tables. It runs inline in the binlog loop as today (the
  binlog is not read meanwhile), after **waiting for the in-flight data flush** (barrier). Its refreshes use
  the same executor, awaited inline: due `finish_next` calls first, sequentially, then one executor run for
  all live dirty views.

**Cut**, only at a transaction boundary (`!in_txn`, as today): `dirty.take()` on every ViewJob (O(1) move),
record `P_cut = boundary` and `G_cut = last_gtid`, spawn the job. The loop keeps reading the binlog into the
fresh buffers (double buffering); the stream `select!` gets a branch for the flush result.

**Start**: when the window has elapsed or `key_count ≥ LAZYVIEW_BATCH_MAX_KEYS`, and no flush is in flight.
A flush longer than the window is followed at once by the next, bigger one, so batches grow with load.
**Back-pressure**: when the buffer reaches `BATCH_MAX_KEYS` during a flush, or control work is due, the loop
stops reading and awaits the flush (today's stop-and-go). Buffers stay ≤ 2 × `BATCH_MAX_KEYS` keys, except
for one huge transaction (as today). **Completion**, in the loop: merge carry-overs, update floors, persist,
set applied (§6), run housekeeping. Shutdown awaits the in-flight flush, then does today's final flush.

**Finer scheme** (optional, phase 4): per-view lanes, each version with its own in-flight flush and cut. I1
holds per (version, target) by the same argument, I2 by the per-view floors (§6). Not needed first.

## 4. Adaptive limiter (one per pool)

State: `L` (f64; permits = ⌊L⌋; 1 ≤ L ≤ max), in-flight, waiters, `epoch`, `cooldown`, slow start, baselines.

* **Samples**: each completed op gives `rtt` (time the permit was held, queueing excluded), `kind` ∈
  {lookup, read, apply_noop, apply_write} and size class `s = min(3, ⌊log16(rows+1)⌋)`. That is 16 buckets
  (kind, s) per pool, so a 1000-row chunk does not look like congestion next to a 10-row one.
* **Baseline** `b[k]`: the minimum over the last 24 five-second sub-windows of each sub-window's median rtt
  (sub-windows with ≥ 8 samples). Before the first one: the median of the bucket's first 8 samples. Floor
  0.2 ms. It follows real shifts (data growth, network) within 2 min, but not momentary queueing.
* **Round**: ends after ≥ max(4, ⌊L⌋) ops and ≥ 50 ms since the last decision, so roughly one decision per
  "RTT". `g = median(rtt_i / b[k_i])` over the round's samples with a baseline. `binding` means peak
  in-flight ≥ ⌊L⌋ or an op waited for a permit.

| condition (first match) | action |
|---|---|
| hard error from an op started in the current epoch | `L ← max(1, 0.5·L)`; epoch+1; cooldown 2; leave slow start |
| soft error (1213) from an op started in the current epoch | `L ← max(1, 0.75·L)`; epoch+1; cooldown 2 |
| `g ≥ 1.8` | `L ← max(1, L·max(0.5, 1.5/g))`; epoch+1; cooldown 1; leave slow start |
| `g ≤ 1.3 ∧ binding ∧ cooldown = 0` | slow start: `L ← min(max, 1.5·L)`, else `L ← min(max, L + 1)` |
| otherwise | hold; `cooldown ← max(0, cooldown − 1)` |

The limiter starts at `L0 = min(2, max)` in slow start. The hysteresis band (1.3, 1.8) stops flapping. One
decrease per epoch stops collapse storms: errors of ops issued before the last decrease belong to the same
episode and are ignored. Increases are additive, +1 per round. The equilibrium is just past the knee of the
server's throughput curve (latency 1.3–1.8× uncontended); at L = 1 the engine is today's sequential one.
Pools on the same host:port (`read`, `write/source`) share hard-error decreases: a 1040 on one halves both.

**4.2 Errors.** **Hard**: 1040 (too many connections), 1203 (max_user_connections), 1205 (lock wait
timeout), 2006 / 2013 (gone away / lost connection), I/O errors, client timeouts (op longer than
`LAZYVIEW_TARGET_TIMEOUT_MS`, connect longer than 15 s), 3024 / 1969 (statement time limit). **Soft**: 1213
(deadlock). Anything else (1054, 1146, …) is no congestion signal: the op fails, the view handles it as today.
Overload errors retry the op ≤ 3 times with backoff `50 ms·2^n` ± 50 % jitter, re-acquiring a permit each
time, so retries queue behind the reduced limit; a final failure becomes carry-over. Overload failures **do
not count** toward the 3-failure re-plan escalation: a rebuild is the worst response to an overloaded server.

**4.3 Zombie fence.** A write op that times out or loses its connection after its transaction began may
still commit on the server, after a newer write of the same unit (an I1 violation; today's code has the same
hazard, more ops in flight make it likelier). Before reporting the op failed, the supervisor sends
`KILL <thread id>` on the target's control connection (killing your own threads needs no privilege) and
waits until the id leaves `PROCESSLIST` (≤ 5 s). If the target is unreachable it is **quarantined**: no write
ops for it until the fence completes, and its chunks become carry-over.

**4.4 Permits.** A tokio `Semaphore` created with `max` permits. Growing is `add_permits`; shrinking adds a
debt counter, and permits released while debt > 0 are forgotten. In-flight ops are never cancelled.
**Modes**: `fixed` pins L = max with no adaptation (retries, backoff and fences stay); `off` is L = 1 with no
pipelining, exactly today's engine.

**4.5 CPU and memory.** The diff per write op is Datum conversion of the old rows, grouping and a sorted
compare. When old+new rows ≥ 4096 (or ≥ 64k datums) it runs in `spawn_blocking`, gated by a semaphore of
`LAZYVIEW_CPU_WORKERS` (default `min(4, max(1, cores/4))`); the op keeps its permit meanwhile (a few ms).
Smaller diffs run inline; binlog decoding stays on its own task. Memory: admission budget
`LAZYVIEW_MAX_INFLIGHT_ROWS` (200k). A chunk read starts only while the rows read but not yet written
everywhere are under it, so in-flight rows stay ≤ budget + ⌊L_read⌋ × rows(chunk). A slow target stalls
admission, then reads, then (§3) the binlog loop.

## 5. Configuration and metrics

| variable | default | meaning |
|---|---|---|
| `LAZYVIEW_SUPERVISOR` | `adaptive` | `adaptive`; `fixed` (limits pinned at max, still pipelined, retried, fenced); `off` (today's engine) |
| `LAZYVIEW_MAX_SOURCE_CONCURRENCY` | `8` | source read pool size = max read limit |
| `LAZYVIEW_MAX_TARGET_CONCURRENCY` | `4` | write pool size per target (incl. `source`); override `LAZYVIEW_TARGET_<NAME>_MAX_CONCURRENCY` |
| `LAZYVIEW_MAX_INFLIGHT_ROWS` | `200000` | row admission budget (§4.5) |
| `LAZYVIEW_CPU_WORKERS` | `min(4, max(1, cores/4))` | concurrent off-reactor diffs |
| `LAZYVIEW_STALE_AFTER_MS` | `300000` | a view failing this long stops holding back the saved position (§6, I2) |

Code constants, documented in `supervisor.rs`: L0 = 2, band 1.3/1.8, tolerance 1.5, decrease factors
0.5/0.75, 24 sub-windows of 5 s, min chunk 100, offload at 4096 rows / 64k datums, 3 retries, 50 ms backoff.

Metrics (`src/metrics.rs`, rendered by `health::prometheus`, also in `?format=json`):
* **Counters** (`lazyview_<name>_total`): `supervisor_increases`, `supervisor_decreases`,
  `supervisor_overload_errors`, `supervisor_retries`, `supervisor_zombie_kills`, `supervisor_cpu_offloads`,
  `flushes_overlapped`, `binlog_backpressure`.
* **Per-pool gauges**, labelled `{pool="read|write", endpoint="source|<target>"}`: `lazyview_supervisor_limit`
  (float), `_max`, `_inflight`, `_queued`, `_connections`, `_rtt_seconds` (last round median),
  `_rtt_baseline_seconds` (bucket with the most samples in that round), `_gradient` (last g).
* **Engine gauges**: `lazyview_flush_inflight`, `lazyview_flush_last_seconds`, `lazyview_buffered_keys`,
  `lazyview_inflight_rows`.

Registry: `Metrics.pools: Mutex<Vec<Arc<PoolGauges>>>`, floats as `f64` bits in `AtomicU64`. Logging: `info`
when L changes by ≥ 50 % or enters / leaves 1 ("source read concurrency 8 → 4: rtt 2.6× baseline"), at most
once per 10 s; other changes at `debug`.

## 6. Invariants

**I1. One operation per unit, and no older write after a newer one.**
* (a) At most one data flush is in flight and control flushes run behind a barrier, so at most one executor
  run touches a (version, target) at a time.
* (b) In a run, each unit is in exactly one chunk, with one write op per target. Live and shadow are
  different tables, and `finish_next(i)` runs before slot `i`'s live refresh.
* (c) Every source read of run *k* follows the fence on `G_cut`, so all changes that dirtied its units
  (≤ `P_cut`) are visible.
* (d) Run *k+1* starts only after every op of run *k* has finished, zombie fence included (§4.3), so no older
  write lands later. Retries happen inside the op that owns the chunk.
* (e) A chained view that reads another MV mid-update converges, because every MV write re-dirties it
  through the binlog (unchanged rule).

**I2. The saved position never passes an unapplied change.**
* Each live version `v` has a floor `F_v`: the `P_cut` of the last data flush in which `v` ended without
  carry-over (a flush with no work for `v` counts; a swapped-in version starts at the swap's boundary). After
  a flush's writes commit, the loop persists `max(persisted, min(P_cut, min_v F_v, XA floor))`; changes after
  `P_cut` belong to no completed flush, so they are never passed.
* **This also fixes a gap in today's code**: `flush()` persists `boundary` even when `refresh_live` failed
  and the carry-over exists only in memory; a crash then loses those changes for that view.
* A version failing for longer than `LAZYVIEW_STALE_AFTER_MS` gets state `stale` on the failing targets, so
  `try_resume` rebuilds it after a restart, and it stops holding the floor. It returns to `ready` once its
  carry-over is applied.
* `applied()` keeps its meaning (every change ≤ the value is applied to every target). After a completed
  flush with no carry-over and no builds, re-plans or pending definitions, it becomes `boundary` if the
  buffer is empty, else `P_cut`. The idle path is unchanged. A crash means replay from the saved position,
  which is idempotent (units are recomputed from the current source).

**I3. Swaps, builds, reloads and re-plans keep their semantics.** The control flush is today's code path,
serialized behind the barrier. Only its refresh calls use the executor (covered by I1(b)).

**I4. Bounded resources.** Connections on the source ≤ 4 (binlog, meta, `src`, control) + `MAX_SOURCE` +
`MAX_TARGET(source)` + `BUILD_CONCURRENCY`·(1 + targets on source), 20 by default; on each other target
≤ 1 + `MAX_TARGET` + `BUILD_CONCURRENCY`. Tasks ≤ 1 flush + ⌊L_read⌋ + Σ⌊L_write⌋ + `CPU_WORKERS` + builds,
each spawned only after it holds a permit. Memory: two key buffers ≤ 2·`BATCH_MAX_KEYS` plus the row budget.

**I5. Overload safety.** The limit never goes below 1. Hard errors halve it once per episode; gradient
decreases are at most ×0.5 per round and increases +1 per round, inside a hysteresis band. Retries queue
behind the reduced limit. At L = 1 there is one read and one write per target in flight, today's load;
pipelining adds only binlog reading. Overload never escalates to a rebuild.

## 7. Tests and benchmarks

**Unit tests (no DB):**
* Limiter against a simulated server (service time `s·max(1, n/knee)` with lognormal noise, the knee
  stepping 8 → 2 → 6): within 1 of the band within 20 rounds, ≤ 4 direction changes per minute in steady
  state, one halving per error burst, never below 1 or above max.
* Mixed op sizes at constant load cause no decrease; permit debt never cancels ops; random UnitSets chunk
  disjointly and completely for every P; random cuts / failures / XA keep the position monotone, ≤ min floor.

**Integration tests** (to be added as `tests/supervisor.rs`, all flavors in `LAZYVIEW_TEST_DSNS`, databases `sup_*`; every
case ends with the oracle matching):
1. Seeded random workload: 6 views, 2 targets, `LAZYVIEW_QUERY_CHUNK=7`, windows 0 / 200 ms, all three modes;
   peak in-flight > 1 (concurrency really happened).
2. Pipelining: continuous writers while the proxy slows a target; `flushes_overlapped > 0`.
3. Crash consistency: abort the engine at random points of an in-flight flush (20×), restart in `resume`.
4. Target outage: the proxy drops a target for 30 s and lazyview restarts meanwhile; a long outage gives `stale`.
5. Zombie: the proxy holds the COMMIT OK past `LAZYVIEW_TARGET_TIMEOUT_MS=1000`; a KILL is counted.
6. Caps: `PROCESSLIST` for user `lv`, sampled during a heavy run, stays ≤ the I4 formula per server.
7. Overload: a user with `MAX_USER_CONNECTIONS 6`; no view errors, `supervisor_decreases > 0`.

CI runs the existing suites in the default (adaptive) mode, plus a matrix leg with `LAZYVIEW_SUPERVISOR=off`.
**Fault proxy**: `tests/common/proxy.rs` will be an in-process tokio TCP proxy: latency and jitter per
direction, stall, drop or refuse connections, delay the n-th server packet. A standalone build of the same
code (`tmp/supproxy/`, local only) will serve the benchmarks.

**Benchmarks** (local `tmp/bench.sh`, not in the repository; `EXTRA="--env LAZYVIEW_SUPERVISOR=…"`): off / fixed / adaptive, interleaved,
×3 (the host is shared; report medians), on spj_single, join_direct, hot_keys, lookup_fanout, many_views, a
2-target variant and `RATES="250 500 1000 2000"`. The harness additionally scrapes `/metrics` per sample
(limit, in-flight and rtt timeline per pool in the JSON) and keeps the 1-row commit "victim" probe running
during the load. Acceptance: freshness p50 and p95 −50 % vs off at 8×50; lookup_fanout `keeping_up`;
catch-up rows/s ≥ 2× off; mysqld CPU per change at most +20 %; victim commit p99 under adaptive ≤ 1.5× idle;
correctness OK everywhere.

**Overload experiment** (a local script, `tmp/sup-overload.sh` in the plan; 120 s at a fixed 1000 rows/s): at t = 30–60 s, 16 hostile
sessions on the target / source (self-join scans of `order_items`, plus an fsync-heavy autocommit writer);
at t = 75–105 s, +50 ms latency through supproxy. Expected: limits ≤ 2 within 2 rounds of each disturbance
and recovery afterwards; no 1040 / 2013 storms; ≤ 4 direction changes per minute in steady phases; freshness
degrades but nothing fails. Compared with `fixed`, the victim probe and the hostile queries must do better
under adaptive, which shows lazyview backs off instead of piling on.

**Implementation order**: (1) a new `src/supervisor.rs` (pools, `fixed` limiter, executor), `mv::apply` split
into read-old / diff / write, `refresh()` on the executor inside today's synchronous flush, then bench;
(2) data / control split, pipelining, per-view floors (the I2 fix), applied metric; (3) adaptive mode, error
classes, retries, zombie fence, metrics, overload experiments; (4) optional per-view lanes, build permits.
