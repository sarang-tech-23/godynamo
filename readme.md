Requirements
- store and fetch data items against a primary key. (get and put ops over http)
- consitent hashing to distribute load/keys across node
- data object versioning using vector clocks
- conflict resolution at reads
- asynchronous replication using quorum systems
- gossip protocol for membership data


ok let me go through the life cycle of a request and then build requirements around it

- first is a client which can call either get or put

- now the request could either forwarded to load balancer or the exact node based on preferece list knowledge

- i think i would just go ahead with preference list logic available to to client, implementing load balancing would be out of scope of this project

- we need to build consistent hashing algrothm first to start with preference list

- define the hash function and its output range, 
- whenever a node start assign randome values from the hash function output to it
- store the node, [tokens..] or {token: node} dict for each node in its persistent node.

- whenver a keys comes get its hash and perform the binary search on sorted list of tokens, the token we my find can be a virtual token, need to find which physical service it is assigened to 
- get the minimum token assign to that node, and in the pysical minimu token list, find which other N-1 nodes, toke comes into it
- this will be the prefeerence list of physical nodes and their rexpective first tokens

- put request
    - request directly goes to the coordinator node
    - it persiste it to its local db, generate a vector clock and the share the whole object to be stored in N-1 replica
    - wait for W-1 replicas to return and then acknowldge the client

- in case of get request: 
    - check the quorum configuration, suppose R=2 we need to query the current node and R-1 node in the preference list for the keys value



So this is how i would implement it
1. create a vector clock. The function would (vector_clock, write_node), and it will return a new vector clock with counter incrmemented for write_node. It could happend that for the very first write for a key, the vector_clock arg will be empty, in this case we need to return (write_node: 1)

2. Simple write operation: takes the (key, data_object) arg, generate the vector clock and persist the whole object to bbolt
    - We need to create separate form of write for replicase which will take whole data object with vector clock already attached to the data object
    - in replica there should also be logic to reject write if the vector clock of the new object is not child of the old one
    - I think we also need a different authority of write which will overwrite the existing data object if the client has resolved the conflict.

3. Simple read opration: take the (key) arg, and fetches the whole data object with vector clock and return

4. After reads, we need to check for divergent branches of vector clock for each R read object and create a list of all the leafs in the tree after combining the vector clocks


---
---

# Design Notes (structured pass over the draft above)

## 0. Corrections to the first pass

Four things in the draft above are wrong. Two are cosmetic, two would break the system.

**C1 — Preference list must be built by walking the ring, not from "minimum token per node". (breaking)**
The draft says: find the owning token, then *"get the minimum token assigned to that node, and in the physical minimum token list, find which other N-1 nodes"*. If the preference list is derived from a per-node minimum-token ordering, then every key gets the **same** successor ordering, because that ordering doesn't depend on the key. The whole point of vnodes is that different keys land at different ring positions and therefore get *different* replica sets — that's what spreads load and what makes a node failure re-distribute across many peers instead of one.
Correct algorithm: from the key's ring position, walk clockwise over the sorted token list, appending each token's physical node to the list, **skipping nodes already present**, until you have the number you need. Tokens play no role after the walk.

**C2 — Replicas must NOT reject concurrent writes. (breaking)**
The draft says *"in replica there should also be logic to reject write if the vector clock of the new object is not child of the old one."* This is a compare-and-swap instinct, and it is the opposite of Dynamo. Dynamo is always-writeable; rejecting a concurrent write throws away an accepted client update. A replica receiving a version compares it against every version it already holds and:
| incoming vs stored | action |
| --- | --- |
| incoming descends from stored | replace stored |
| incoming is ancestor of stored | drop incoming (we already have newer) |
| equal | no-op (makes replica writes idempotent — needed for handoff retries) |
| concurrent | **keep both as siblings** |
The only thing a replica ever rejects is a malformed request.

**C3 — Reads go to N nodes, not R nodes.**
The draft says *"suppose R=2 we need to query the current node and R-1 node"*. Send the read to all N reachable replicas and **wait for R responses**. Querying exactly R means one slow or dead node fails the read, and it means you never see the stale replicas, which makes read repair impossible. `N` is the fan-out; `R` is the barrier. (The draft got this right on the write path — fan out to `N-1`, wait for `W-1` — just apply the same shape to reads.)

**C4 — "leaves in a tree" is really "maximal elements of a partial order".**
Vector clocks form a DAG, not a tree, and there's no combining step before the filter. Collect every version returned by every replica, then discard any version that is an ancestor of some other version in the set. What survives is an antichain — mutually concurrent siblings. Usually size 1.

**Bonus — the "different authority of write" in step 2 isn't needed.** When a client reconciles siblings and writes back, it passes a context that is the **pointwise max of all sibling clocks**. That merged clock descends from every sibling by construction, so rule C2 row 1 fires against each of them and the siblings collapse automatically. Conflict resolution needs no special write path — it falls out of the ordinary rule. This is the single most elegant thing in the paper; make sure the implementation actually gets it for free rather than special-casing it.

---

## 1. Scope decisions

| Decision | Choice | Why |
| --- | --- | --- |
| Request routing | Preference-list-aware client, no load balancer | Matches Dynamo's "partition-aware client library" mode; saves a hop and skips an irrelevant component |
| Membership | Static config file, all nodes listed | Gossip is a multi-hour side quest in failure detection that teaches little the quorum work doesn't |
| Token assignment | **Deterministic** from node ID (see §2) | Removes the need to persist or distribute a token map at all |
| Storage | In-memory map behind an interface, bbolt second | Distributed layer must not know which engine is underneath |
| Serialization | JSON | Readable in `curl` output while debugging; swap later if bored |
| N / R / W | 3 / 2 / 2 | R+W > N, the interesting default |

Note: the Requirements list at the top of this file includes gossip membership. That conflicts with the table above — **decide and cut one.** Recommendation: move gossip to a stretch goal and keep static config for the MVP.

## 2. Ring construction

- Hash: MD5 (what the paper uses), take the first 8 bytes as `uint64`. Any stable hash works; the requirement is that it is *identical across processes and runs* — so no `hash/maphash`, no map iteration order.
- Tokens: **derive deterministically**, `token_i = hash(nodeID + "#" + i)` for `i` in `[0, V)`, with `V` ≈ 64 vnodes per node.
  The draft proposed random tokens assigned at node startup and persisted locally. That works, but then every other node and every client has to *learn* those tokens, so you've accidentally created a membership-distribution problem before writing any replication code. Deriving tokens from the node ID makes the ring a pure function of the member set — every process computes the identical ring from the static config, with zero coordination and zero persistence.
- This is the paper's Strategy 1 (random-ish tokens, partition boundaries defined by tokens). Strategy 3 (fixed `Q` equal partitions) is better for real systems and is where anti-entropy and bootstrapping get easy — worth a README paragraph explaining the difference, not worth implementing.

```
type Ring struct {
    tokens  []Token   // sorted by Position
    members []string
}
func (r *Ring) PreferenceList(key string, n int) []string
```

## 3. Preference list

- Walk clockwise from `hash(key)`, dedupe by physical node (see C1).
- **Build it longer than N.** The paper: *"the preference list contains more than N nodes."* The first N are the *preferred* replicas; the tail is the fallback pool that sloppy quorum walks into when a preferred node is down. If the list is exactly N there is nowhere to fail over to and hinted handoff has no home. Suggested: compute `min(len(members), N+3)`.
- Terminology to keep straight in code and commits: **preference list** (ordered, length > N) vs **top-N / preferred replicas** vs **the N nodes actually written to this time** (may differ from top-N under failure). Conflating these is the most common source of confusion in the failure paths.

## 4. Vector clocks

Draft's increment function is correct as written, including the empty-clock case.

```
type VectorClock map[string]uint64

Increment(vc, nodeID) VectorClock      // copy, ++, empty -> {nodeID: 1}
Descends(a, b) bool                    // a >= b pointwise, and a != b
Concurrent(a, b) bool                  // !Descends(a,b) && !Descends(b,a) && !Equal(a,b)
Merge(a, b) VectorClock                // pointwise max — used for client reconciliation
```

Two things not in the draft:

- **Who increments?** The *coordinating node's* ID, not the client's. And the coordinator should be the top-ranked reachable node in the preference list whenever possible — if any node can coordinate any write, the clock accumulates an entry per node and grows without bound. This is exactly why the paper has the clock-truncation scheme.
- **Truncation** (paper §4.4): cap the clock at ~10 `(node, counter)` pairs with a timestamp on each, evict oldest. Note it, mark it optional, note that it can produce false-concurrent results.

## 5. Storage layer

`Get` returns a **slice** — siblings are the normal case, not an error case. The draft's step 3 (*"fetches the whole data object"*, singular) needs to become plural everywhere, including the HTTP response shape.

```
type VersionedValue struct {
    Value []byte
    Clock VectorClock
}
type Store interface {
    Get(key string) ([]VersionedValue, error)
    Put(key string, v VersionedValue) error   // applies the C2 merge rule internally
    Delete(key string) error
}
```

Put it behind the interface from commit one, `sync.RWMutex` around a `map[string][]VersionedValue`. bbolt later: one bucket, key -> JSON-encoded `[]VersionedValue`, one file per node (`node-A.db`).

## 6. Write path

Draft is correct. Restating with the gaps filled:

1. Client sends `PUT /kv/{key}` with body + **context** (the vector clock from its last read of this key, opaque blob). First write ever for a key: no context.
2. Coordinator = top reachable node in the preference list.
3. Coordinator increments the context clock with its own ID → new `VersionedValue`.
4. Writes locally, fans out to the other `N-1` preferred replicas concurrently.
5. Returns success once `W-1` remote acks are in — **without waiting for the slowest**.
6. Late acks must not leak goroutines. Buffer the result channel to `N-1` so stragglers can always send and exit even after the coordinator has returned.

The missing piece in the draft is **the context**. Without the client echoing back the clock it read, every write looks concurrent with every other write, siblings pile up forever, and nothing ever converges. `put(key, context, value)` is the paper's signature for a reason.

## 7. Read path

1. Fan out to all N preferred reachable replicas (C3).
2. `context.WithTimeout`, collect until `R` responses or deadline.
3. Union all returned versions, filter to maximal elements (C4).
4. Return the surviving version(s) plus a context = `Merge` of all their clocks.
5. If more than one survives, the client sees siblings and is expected to reconcile.

Response shape should always be a list, even at length 1 — otherwise the client has two code paths and the conflict case becomes the "weird" one instead of the normal one.

## 8. Read repair

Not in the draft; cheap to add and it's the payoff moment for the whole design. After step 3 above, any replica that returned a strictly-older version (or no version) gets an async write of the reconciled value. Fire-and-forget on a goroutine with its own timeout — **not** on the client's request context, which is about to be cancelled.

## 9. Sloppy quorum + hinted handoff

Not in the draft. This is the part that separates Dynamo from "quorum replication I read about once."

- **Sloppy quorum:** if a preferred replica is unreachable, walk further down the preference list and write to the next healthy node instead. `W` is still satisfied — the write lands on *some* N healthy nodes, not necessarily the *right* N.
- **Hinted handoff:** the fallback node stores the value tagged with a hint recording who it was really for.
  ```
  type Hint struct {
      IntendedNode string
      Key          string
      Value        VersionedValue
  }
  ```
  A background `time.Ticker` worker periodically retries delivery to the intended node; on success, delete the hint. Delivery must be idempotent (C2's equal-clock no-op gives you this free).
- Failure detection: purely local and timeout-based. A node is "down" to me if my last RPC to it timed out; retry after a backoff. No shared failure state, no gossip — the paper explicitly notes each node makes this judgement locally.

## 10. Deletes

Not in the draft, and it's a trap. A plain delete on one replica gets resurrected by a stale replica during the next read repair. Deletes need a **tombstone** — a `VersionedValue` with a deleted flag that participates in vector-clock comparison like any other version. Real systems then need GC for tombstones, which needs a cluster-wide time bound. Implement the tombstone, skip the GC, write a sentence about why that's unsound in production.

## 11. Explicitly out of scope

Gossip membership · Merkle-tree anti-entropy · Strategy 2/3 partitioning · node bootstrap & rebalance · multi-DC · auth · load balancer · tombstone GC · performance work.

Each of these gets one line in the final README explaining what Dynamo does and why it was skipped — that list is interview material in its own right.

## 12. Build order

Reordered from the draft: **vector clocks come first**, because `VersionedValue` is in the `Store` interface signature and every later phase depends on the comparison semantics being right. It's also the only phase that is pure logic — no I/O, no concurrency — so it's the cheapest place to get the hardest concept correct.

| # | Phase | Deliverable |
| --- | --- | --- |
| 1 | `internal/version` | Clock ops + exhaustive table-driven tests |
| 2 | `internal/store` | In-memory store applying the C2 merge rule |
| 3 | `internal/ring` | Deterministic tokens, preference list, distribution tests |
| 4 | `internal/transport` | HTTP server, external + internal routes, static membership |
| 5 | `internal/coordinator` | Concurrent fan-out, `W-1` / `R` barriers, timeouts |
| 6 | reconciliation | Sibling detection surfaced through the HTTP API |
| 7 | sloppy quorum | Kill a node, writes still succeed |
| 8 | hinted handoff | Background worker, hint delivery on recovery |
| 9 | read repair | Stale replica converges after a read |
| 10 | bbolt | Swap the engine, distributed tests unchanged |

`go test -race ./...` from phase 1 onward, not as a cleanup step at the end.

## 13. Test cases worth writing

- **Ring:** same key → same owner across processes; preference list has N *distinct physical* nodes; adding a node moves only ~1/n of keys (assert the bound, don't eyeball it); key count spread across nodes stays within a sane band with 64 vnodes.
- **Clocks:** `[(A,1)]` vs `[(A,2)]` → ancestor; `[(A,2)]` vs `[(A,2),(B,1)]` → ancestor; `[(A,2),(B,1)]` vs `[(A,2),(C,1)]` → concurrent; `Merge` of the last pair descends from both.
- **Store merge rule:** all four rows of the C2 table, plus "same write applied twice is a no-op."
- **Quorum:** 3 alive → ok; 2 alive → ok; 1 alive → fail; slow third replica does not delay a `W=2` success (assert on elapsed time).
- **Handoff:** kill C, write, assert hint exists on D; revive C, assert hint delivered and removed; assert re-delivery is idempotent.
- **Read repair:** A=V5 B=V5 C=V3 → read returns V5, C converges to V5.
- **Convergence:** create siblings, reconcile, write merged, assert all three replicas hold exactly one version.
