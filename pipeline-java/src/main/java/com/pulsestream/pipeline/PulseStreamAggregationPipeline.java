package com.pulsestream.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Java + Apache Beam companion to PulseStream's existing Node.js consumer
 * (../src/consumer.ts) and Python/Spark aggregation job
 * (../src/spark_processor.py). It reads the same {@code metrics.raw}
 * Kafka/Redpanda topic and produces the same avg/min/max/count sliding-
 * window statistics per eventType, on the Beam programming model (so it
 * can run on the DirectRunner locally, or on Dataflow/Flink/Spark's Beam
 * runner without code changes).
 *
 * Status: the windowing and aggregation logic (WindowAndAggregate) is
 * covered by DirectRunner unit tests with a synthetic TestStream -- see
 * PulseStreamAggregationPipelineTest. The KafkaIO source/sink wiring below
 * has NOT been run against a live broker in this environment (no Docker
 * available here); it needs to be exercised against docker-compose's
 * Redpanda before any throughput number is claimed for it.
 */
public class PulseStreamAggregationPipeline {

  public interface Options extends PipelineOptions {
    @Description("Kafka/Redpanda bootstrap servers")
    @Default.String("localhost:9092")
    String getBootstrapServers();
    void setBootstrapServers(String value);

    @Description("Topic to read metrics events from")
    @Default.String("metrics.raw")
    String getInputTopic();
    void setInputTopic(String value);

    @Description("Where to write windowed aggregate output (text files, one JSON-ish line per window/eventType)")
    @Default.String("output/windowed-stats")
    String getOutputPath();
    void setOutputPath(String value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> rawJson = pipeline.apply(
        "ReadFromKafka",
        KafkaIO.<String, String>read()
            .withBootstrapServers(options.getBootstrapServers())
            .withTopic(options.getInputTopic())
            .withKeyDeserializer(StringDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            .withoutMetadata())
        .apply("ExtractValue", MapElements.via(new SimpleFunction<org.apache.beam.sdk.values.KV<String, String>, String>() {
          @Override
          public String apply(org.apache.beam.sdk.values.KV<String, String> kv) {
            return kv.getValue();
          }
        }));

    PCollectionTuple parsed = rawJson.apply(
        "ParseEvents",
        org.apache.beam.sdk.transforms.ParDo.of(new ParseEventFn())
            .withOutputTags(ParseEventFn.PARSED_TAG,
                org.apache.beam.sdk.values.TupleTagList.of(ParseEventFn.MALFORMED_TAG)));

    PCollection<MetricEvent> events = parsed.get(ParseEventFn.PARSED_TAG);
    PCollection<WindowedStats> stats = events.apply("WindowAndAggregate", new WindowAndAggregate());

    stats.apply(
        "FormatOutput",
        MapElements.via(new SimpleFunction<WindowedStats, String>() {
          @Override
          public String apply(WindowedStats s) {
            return s.toString();
          }
        }))
        .apply("WriteOutput", TextIO.write().to(options.getOutputPath()).withWindowedWrites().withNumShards(1));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
  }
}
