# Market

> A Bluesky index node

Market is a backfilling firehose consumer for bluesky atproto records. It will
progressively backfill all data accessible through the configured upstream
relay and maintain that index moving forward.

## Requirements

I haven't tested this too widely yet, but a reasonably performant postgres
instance is all thats really needed, the CPU usage is quite low (could be
optimized a lot more but i see no need yet).

## Building

```
go build
```

## Running

```
./market --db-url="postgres://postgres:password@localhost:5432/dbname"
```

## Running on a raspberry pi 5

This is the exact invocation i'm currently using for running this on a raspberry pi 5 (16gb, but 8 probably works too)

```
export DATABASE_URL=...
./market --batching --skip-aggregations --max-db-connections=8 --max-consumer-workers=50 --max-consumer-queue=40 --backfill-parallel-record-creates=10 --backfill-workers=20 --post-cache-size=3000000
```

This turns on write batching, skips aggregations (so we will unfortunately not
have like counts on posts, among other things) and lowers cache sizes and
parallelism compared to the default values.

In this setup I have postgres configured to use 4gb of memory, but could
probably bump that up to 6, as market in this configuration usually doesn't
take more than 8.

Note that with batching enabled, theres a chance that we miss data on shutdown
if things don't go cleanly.

It's probably reasonable to turn aggregations back on after most of the sync
has completed, it's just a lot of extra load for the poor little database on
first run, building counts for all historical likes/reposts/replies/etc on all
posts.

## License

MIT
