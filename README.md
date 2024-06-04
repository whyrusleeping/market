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

## License

MIT
