package com.pulsestream.pipeline;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * The core of the pipeline: event-time sliding-window aggregation by
 * eventType. Kept as its own PTransform, separate from KafkaIO, so it can
 * be unit tested with an in-memory TestStream instead of a live broker --
 * see PulseStreamAggregationPipelineTest.
 *
 * Window shape matches spark_processor.py exactly: a 5-minute window
 * sliding every 1 minute, so the two pipelines' outputs are directly
 * comparable for the same input stream.
 */
public class WindowAndAggregate extends PTransform<PCollection<MetricEvent>, PCollection<WindowedStats>> {

  private static final DateTimeFormatter TS_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneOffset.UTC);

  @Override
  public PCollection<WindowedStats> expand(PCollection<MetricEvent> input) {
    PCollection<MetricEvent> timestamped = input.apply(
        "AssignEventTime",
        WithTimestamps.of((MetricEvent e) -> Instant.ofEpochMilli(e.timestampMillis)));

    PCollection<MetricEvent> windowed = timestamped.apply(
        "SlidingWindow5MinEvery1Min",
        Window.<MetricEvent>into(
                SlidingWindows.of(Duration.standardMinutes(5)).every(Duration.standardMinutes(1)))
            .withAllowedLateness(Duration.standardMinutes(10))
            // Accumulating (not discarding): a late-arriving event within the
            // allowed-lateness window must refine the window's existing
            // totals, not emit a separate, disjoint delta pane for just that
            // one event.
            .accumulatingFiredPanes());

    PCollection<KV<String, Double>> keyed = windowed.apply(
        "KeyByEventType",
        MapElements.via(new SimpleFunction<MetricEvent, KV<String, Double>>() {
          @Override
          public KV<String, Double> apply(MetricEvent e) {
            return KV.of(e.eventType, e.value);
          }
        }));

    PCollection<KV<String, double[]>> aggregated =
        keyed.apply("ComputeStats", Combine.perKey(new StatsAccumulatorFn()));

    return aggregated.apply(
        "FormatWindowedStats",
        ParDo.of(new DoFn<KV<String, double[]>, WindowedStats>() {
          @ProcessElement
          public void processElement(@Element KV<String, double[]> kv, BoundedWindow window,
              org.apache.beam.sdk.transforms.DoFn.OutputReceiver<WindowedStats> out) {
            IntervalWindow iw = (IntervalWindow) window;
            double[] stats = kv.getValue();
            out.output(new WindowedStats(
                kv.getKey(),
                stats[0], stats[1], stats[2], (long) stats[3],
                TS_FORMAT.format(java.time.Instant.ofEpochMilli(iw.start().getMillis())),
                TS_FORMAT.format(java.time.Instant.ofEpochMilli(iw.end().getMillis()))));
          }
        }));
  }
}
