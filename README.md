# Instagram Feed Clone — Backend System Design

This is a backend-only project focused on system design for a high-scale social media feed. The goal was to build the write and read path for an Instagram-style feed that can handle heavy write load, fan out posts to thousands of followers, and serve reads as fast as possible using a layered caching strategy.

There is no frontend. This is purely about the architecture.

---

## What This Project Is

When a user posts something on Instagram, that post has to appear in the feed of every person who follows them — sometimes millions of people. Doing that at request time would be too slow. The standard approach is fan-out on write: the moment a post is created, you pre-compute and push it into every follower's feed in the background. Reads then become a simple cache lookup.

This project implements that full pipeline: post creation, async fan-out via Kafka, Redis-backed feed storage, cursor-based pagination, and a fallback system for when Kafka is unavailable.

---

## Performance

Tested with Artillery under sustained load:

- **350 write requests per second** (POST /api/posts) with a **100% success rate**
- Each post triggers fan-out to 500 followers asynchronously via Kafka
- Feed reads from Redis return in under 1ms on a warm cache

The write test runs at 300–350 arrivals/second for 60 seconds and measures throughput, latency, and error rate. The read test hits the feed endpoint at 200 requests/second across random user IDs.

To run the write load test:

```bash
npx artillery run load-test-baseline-500Req300.yml
```

To run the read load test:

```bash
npx artillery run read-feed-test200.yml
```

---

## Architecture

### Fan-out on Write

When a post is created:

1. The post is written to PostgreSQL
2. A message is published to Kafka in parallel (non-blocking)
3. The API returns a 201 immediately — it does not wait for fan-out
4. The Kafka consumer picks up the event and writes the post ID into each follower's Redis feed
5. If Kafka is down, a Redis-backed fallback queue handles the task instead

Fan-out processes followers in batches of 500 to avoid memory pressure. Each batch updates Redis atomically using a Lua script that adds the post, trims the feed to the 100 most recent posts, and refreshes the TTL — all in a single round trip.

### Feed Reads — 3-Tier Cache

Feed reads go through three layers:

1. **Response cache** — the full serialized JSON response is stored in Redis. A cache hit here is a single GET and a JSON parse, typically under 0.1ms.
2. **Feed sorted set** — if the response cache is stale or missing, the feed is reconstructed from a Redis sorted set (ZSET) keyed by timestamp. Post details are fetched from PostgreSQL by ID.
3. **PostgreSQL fallback** — if Redis has no feed data (cold start or expired TTL), the feed is rebuilt from PostgreSQL, warm-loaded back into Redis, and served.

### Cursor-Based Pagination

Feeds beyond the first 100 posts fall back to PostgreSQL with cursor-based pagination. The cursor is a compound string: `ISO-timestamp_post-uuid`. This avoids duplicate or skipped posts when new content arrives between pages, which offset-based pagination cannot handle reliably.

For pages within the first 100 (Redis), the system uses a hybrid approach: it filters the Redis sorted set by the cursor, and only queries PostgreSQL for the remainder if Redis does not have enough results.

### Kafka Topics and Partitioning

There is one Kafka broker running locally via Docker in KRaft mode (no Zookeeper). Each topic is created with **3 partitions**. Three partitions allow the consumer group to scale up to 3 concurrent consumers, each handling a different partition independently. For this project one consumer handles all three, but the partition layout means horizontal scaling is ready without any topic reconfiguration.

Topics:
- `post-created` — triggers feed fan-out
- `user-followed` — triggers feed backfill for new follows
- `user-unfollowed` — triggers feed cleanup on unfollow
- `*-dlq` — dead-letter queues for messages that failed after 5 retries

---

## Why PostgreSQL and Not Cassandra

Cassandra is a common choice for feed systems at massive scale because of its write throughput and linear horizontal scaling. For this project, PostgreSQL made more sense for a few reasons.

The data model here is relational. Follows, users, and posts have real relationships and integrity constraints. Cassandra does not have foreign keys or joins — you model everything around your query patterns, and any change to those patterns requires rethinking the schema. With PostgreSQL, the follow and user tables stay clean, and Sequelize handles the ORM layer without ceremony.

The feed itself — the part that needs to scale for reads — is stored in Redis, not the database. PostgreSQL is only hit for writes, for cold cache rebuilds, and for pagination beyond the Redis window. That workload is manageable with a connection pool and proper indexing.

Operationally, PostgreSQL on Supabase is easy to run and inspect. Cassandra requires a cluster to be useful and has a steeper operational overhead for a project at this scale.

The short version: Cassandra solves problems that Redis already handles here. PostgreSQL covers everything else cleanly.

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Framework | Express.js 5 (Node.js, ESM) | Lightweight, async-friendly |
| Primary DB | PostgreSQL via Supabase | Relational model, hosted, easy to inspect |
| ORM | Sequelize | Schema sync, model validation |
| Cache / Feed store | Redis 7 | Sub-millisecond reads, sorted sets for feed ordering |
| Message queue | Kafka (KRaft, 1 broker) | Async fan-out, DLQ, offset management |
| Containers | Docker Compose | Redis and Kafka run locally without host installs |
| Load testing | Artillery | Scriptable HTTP load with arrival rate control |

Docker is required because Kafka and Redis are run as containers. Neither needs to be installed on the host — `docker compose up -d` handles both.

---

## Database Schema

**users** — id (int, PK), username, email, bio, avatar_url, followers_count, following_count, is_celebrity

**posts** — id (UUID, PK), user_id (FK), caption, image_url, likes_count, comments_count, created_at
- Index on `(user_id, created_at DESC)` for user post queries
- Index on `(created_at DESC, id)` for cursor pagination

**follows** — id, follower_id (FK), following_id (FK), created_at
- Unique constraint on `(follower_id, following_id)`

Users with over 10,000 followers are flagged as `is_celebrity`. This flag exists to support a future hybrid fan-out strategy (fan-out on write for normal users, fan-in on read for celebrities). It is tracked but the celebrity path is not yet implemented.

---

## Redis Data Layout

- `feed:user:{id}` — ZSET, sorted by post timestamp (ms). Capped at 100 entries per user. TTL: 7 days.
- `feed:user:{id}:response` — String, full serialized feed JSON. TTL: 7 days.
- `post:{id}` — String, serialized post object. TTL: 1 hour.
- `user:{id}:followers_count` / `following_count` — String, atomic counters. TTL: 1 hour.
- `fallback:fanout:queue` — List, fan-out tasks queued when Kafka is unavailable.
- `fanout:idempotency:{userId}:{postId}` — String, prevents duplicate fan-out writes. TTL: 7 days.

All feed mutations go through Lua scripts loaded at startup. This keeps the ZADD + ZREMRANGEBYRANK + EXPIRE sequence atomic and avoids extra round trips.

---

## Lua Scripts

All Redis mutations in this system go through server-side Lua scripts rather than individual commands issued from the application. There are 10 scripts in total, all loaded at startup via `SCRIPT LOAD` and executed using `EVALSHA` (the SHA1 hash) instead of `EVAL` (the full script body). This means the script is transmitted to Redis once at boot, and every subsequent call is just a hash lookup — significantly less data over the wire on every request.

If Redis is flushed between calls and returns a `NOSCRIPT` error, the execution layer automatically falls back to `EVAL` and reloads the script. This makes the system resilient to Redis restarts without any manual intervention.

### Why Lua and not regular commands

The main reason is atomicity. Consider adding a post to a feed: you need to ZADD the post, check the size, ZREMRANGEBYRANK if it exceeds 100, and EXPIRE the key. If you send these as four separate commands, another client can read the feed between any of them and see an inconsistent state — either a feed with 101 entries or a feed missing its TTL refresh. A Lua script runs as a single atomic unit on the Redis server. No other command executes between steps.

The secondary reason is round-trip reduction. Scripts that combine ZREVRANGE + post key lookups (MGET) or batch DEL across multiple users save multiple network round trips per request.

### The scripts

**Add post to feed** — the core fan-out script. Takes a feed key, a timestamp score, a post ID, the max feed size (100), and a TTL. Calls ZADD, then checks the sorted set size and removes the oldest entries if it exceeds the cap, then sets the TTL. All three operations are atomic. This is called once per follower during fan-out.

**Batch add post to feeds** — runs the above script across all followers in a batch using `Promise.all`. Node-redis v4 automatically pipelines commands issued in the same event loop tick, so the 500 EVALSHA calls in a batch are sent as a single pipeline rather than 500 sequential round trips.

**Get feed with TTL refresh** — ZREVRANGE with scores, plus an EXPIRE to slide the TTL forward on every read. A feed that is actively read never expires.

**Get feed with posts** — combines ZREVRANGE and MGET in a single script. Extracts post IDs from the sorted set, builds the `post:{id}` keys, and fetches all post details in one MGET call. Saves one full round trip compared to doing this in two separate commands from the application.

**Warm up feed cache** — used when rebuilding a cold feed from PostgreSQL. Accepts a variable number of score/postId pairs and does a batch ZADD + trim + EXPIRE in one atomic call. Without this, warming a 100-post feed would require 100 separate ZADD calls.

**Get cached feed response** — fetches the pre-serialized JSON response from `feed:user:{id}:response` and refreshes the TTL on both the response key and the feed sorted set key in the same operation.

**Batch invalidate response caches** — after fan-out completes for a batch of followers, their `feed:user:{id}:response` keys are stale. This script takes a list of user IDs and DELs all their response cache keys in one atomic pass. Without this, you would need one DEL call per follower, which at 500 followers per batch adds up quickly.

**Decrement count with bounds check** — handles follower/following counts. A plain DECR in Redis can go negative if there is a race between two unfollow events. This script checks the result after DECR and resets to 0 if it goes below, keeping counts accurate without needing a distributed lock.

---

## API Endpoints

**Users**

```
POST   /api/users                    Create user
GET    /api/users                    List all users
GET    /api/users/:id                Get user by ID
POST   /api/users/:id/follow         Follow a user   { follower_id }
POST   /api/users/:id/unfollow       Unfollow a user { follower_id }
```

**Posts**

```
POST   /api/posts                    Create post     { user_id, image_url, caption? }
GET    /api/posts                    List posts      ?limit=50
GET    /api/posts/:id                Get post by ID
GET    /api/posts/user/:user_id      Posts by user
GET    /api/posts/feed/:user_id      User feed       ?limit=20&cursor=<cursor>&refresh=true
```

**Health**

```
GET    /api/ready                    Service readiness (PostgreSQL, Redis, Kafka)
GET    /api/health                   Health + Redis memory info
GET    /api/redis/test               Quick Redis connectivity test
GET    /api/kafka/test               Quick Kafka connectivity test
```

---

## Setup

**Prerequisites:** Node.js 18+, Docker

### 1. Clone and install

```bash
git clone <repo>
cd Instagram_feed_clone/backend
npm install
```

### 2. Environment variables

Create `backend/.env`:

```env
DATABASE_URL=postgresql://<user>:<password>@<host>:5432/postgres

REDIS_HOST=localhost
REDIS_PORT=6379

KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=instagram-feed-backend

NODE_ENV=development
PORT=3000
```

### 3. Start Redis and Kafka

```bash
cd ..  # project root
docker compose up -d
```

Kafka takes about 30–60 seconds to elect its leader. You can watch it with `docker compose logs -f kafka`.

### 4. Start the server

```bash
cd backend
node server.js
```

Check that all services are up:

```bash
curl http://localhost:3000/api/ready
```

All three services — postgresql, redis, kafka — should show as ready.

### 5. Seed users

```bash
node script/seedUsers.js
```

This creates `user1` as the celebrity with `is_celebrity: true`, then generates 499 users with random names, and makes all of them follow `user1`.

---

## Load Testing

Install Artillery if you haven't:

```bash
npm install -g artillery
```

**Write load test** — 300 posts/second for 60 seconds, each post triggering fan-out to 500 followers:

```bash
npx artillery run load-test-baseline-500Req300.yml
```

**Read load test** — 200 feed reads/second across random users:

```bash
npx artillery run read-feed-test200.yml
```

Artillery prints p50, p95, p99 latency, total requests, and error rate at the end of each run. A healthy run shows 0 errors and p99 under 500ms for writes (fan-out is async so the write itself is fast).

---

## Scaling Considerations

The main bottlenecks at larger scale would be:

**Fan-out for celebrities.** If a user has 10 million followers, sequential batch fan-out takes too long even asynchronously. The standard fix is a hybrid strategy: fan-out on write for normal users, fan-in on read for celebrities. The `is_celebrity` flag is already in the schema for this.

**Single Kafka broker.** The current setup has no replication. A broker restart loses in-flight messages. Moving to 3 brokers with replication factor 2 and `min.insync.replicas=2` fixes this.

**PostgreSQL connection pool.** The pool is capped at 20 connections. Supabase's direct port has a hard limit. The right fix at scale is to point the app at Supabase's PgBouncer pooler on port 6543 and set `connection_limit=1` in the connection string.

**Redis memory.** At 100 posts per user in a sorted set, a million active users uses roughly 15 GB of Redis memory. Redis Cluster handles this by sharding across nodes.
