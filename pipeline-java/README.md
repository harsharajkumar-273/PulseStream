# PulseStream Java/Beam aggregation pipeline

A Java + [Apache Beam](https://beam.apache.org/) companion to this repo's
Node.js consumer (`../src/consumer.ts`) and Python/Spark aggregation job
(`../src/spark_processor.py`). It reads the same `metrics.raw` Kafka/Redpanda
topic and computes the same sliding-window statistics -- avg/min/max/count
per `eventType`, in a 5-minute window sliding every 1 minute -- so it's a
direct, comparable reimplementation on the Beam model instead of Spark
Structured Streaming.

**Status: windowing/aggregation logic is written and unit-tested; the
KafkaIO source has not been run against a live broker.** See
[`WindowAndAggregateTest`](src/test/java/com/pulsestream/pipeline/WindowAndAggregateTest.java):
it drives the `WindowAndAggregate` transform on Beam's DirectRunner with a
synthetic `TestStream`, including a deliberately out-of-order (late) event to
exercise `withAllowedLateness`, and asserts the computed avg/min/max/count are
correct. That's real, verified logic. What's *not* verified is
`PulseStreamAggregationPipeline`'s `KafkaIO.read()` wiring end to end against
Redpanda -- do that before quoting any throughput number for this pipeline,
the same rule this repo applies to every other benchmark.

## Why this exists next to the Spark job

Not a replacement -- `spark_processor.py` already does this in Python. This
version exists to have the same aggregation logic in Java on the Beam model,
which runs unchanged on Dataflow, Flink, or Spark's own Beam runner.

## Structure

| File | What it does |
|---|---|
| [`MetricEvent.java`](src/main/java/com/pulsestream/pipeline/MetricEvent.java) | POJO matching the JSON PulseStream's API publishes: `{id, deviceId, eventType, value, timestamp}` |
| [`ParseEventFn.java`](src/main/java/com/pulsestream/pipeline/ParseEventFn.java) | Parses one JSON record; a malformed record goes to a separate output tag instead of failing the bundle -- the same "one bad record shouldn't poison the batch" idea as the `SAVEPOINT` fix in `../src/consumer.ts`, applied at parse time here |
| [`WindowAndAggregate.java`](src/main/java/com/pulsestream/pipeline/WindowAndAggregate.java) | The core transform: assigns event time, applies the sliding window, keys by `eventType`, and combines with `StatsAccumulatorFn` |
| [`StatsAccumulatorFn.java`](src/main/java/com/pulsestream/pipeline/StatsAccumulatorFn.java) | A `Combine.CombineFn` computing avg/min/max/count in one pass, so no per-window event list is ever materialized |
| [`PulseStreamAggregationPipeline.java`](src/main/java/com/pulsestream/pipeline/PulseStreamAggregationPipeline.java) | `main()`: wires `KafkaIO.read()` -> parse -> `WindowAndAggregate` -> `TextIO.write()`. **Not yet run against a live broker.** |

## Run the tests

```bash
mvn test
```

## Run against a live broker (once verified, update this section with real numbers)

```bash
# from the PulseStream repo root
docker-compose up --build -d   # provisions Redpanda
cd pipeline-java
mvn compile exec:java -Dexec.mainClass=com.pulsestream.pipeline.PulseStreamAggregationPipeline \
  -Dexec.args="--bootstrapServers=localhost:19092 --inputTopic=metrics.raw --outputPath=output/windowed-stats"
```
