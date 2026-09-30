package com.pulsestream.pipeline;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

/**
 * Exercises WindowAndAggregate end to end on Beam's DirectRunner with a
 * synthetic, event-time-ordered stream (TestStream), including a
 * deliberately out-of-order (late-arriving) event -- no Kafka, Docker, or
 * network access required. This is what's actually been run and verified
 * in this environment; PulseStreamAggregationPipeline's KafkaIO wiring has
 * not been (see its class-level javadoc).
 */
public class WindowAndAggregateTest {

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  private static final Instant BASE = new Instant(0);

  @Test
  public void computesAvgMinMaxCountPerEventTypeInWindow() {
    TestStream<MetricEvent> events = TestStream.create(
            org.apache.beam.sdk.coders.SerializableCoder.of(MetricEvent.class))
        .advanceWatermarkTo(BASE)
        .addElements(
            event("e1", "temperature", 10.0, BASE.plus(Duration.standardSeconds(0))),
            event("e2", "temperature", 20.0, BASE.plus(Duration.standardSeconds(10))),
            event("e3", "temperature", 30.0, BASE.plus(Duration.standardSeconds(20))),
            event("e4", "humidity", 50.0, BASE.plus(Duration.standardSeconds(5))))
        .advanceWatermarkToInfinity();

    PCollection<WindowedStats> result =
        pipeline.apply(events).apply("WindowAndAggregate", new WindowAndAggregate());

    // The [0s, 5min) sliding window should contain all four events: three
    // "temperature" readings (10, 20, 30 -> avg 20, min 10, max 30, count 3)
    // and one "humidity" reading (count 1).
    PAssert.that(result)
        .satisfies((SerializableFunction<Iterable<WindowedStats>, Void>) stats -> {
          boolean foundTemperatureWindow = false;
          boolean foundHumidityWindow = false;
          for (WindowedStats s : stats) {
            if (s.eventType.equals("temperature") && s.count == 3) {
              foundTemperatureWindow = true;
              assertThat(s.avgValue, closeTo(20.0, 1e-9));
              assertThat(s.minValue, closeTo(10.0, 1e-9));
              assertThat(s.maxValue, closeTo(30.0, 1e-9));
            }
            if (s.eventType.equals("humidity") && s.count == 1) {
              foundHumidityWindow = true;
              assertThat(s.avgValue, closeTo(50.0, 1e-9));
            }
          }
          assertThat(foundTemperatureWindow, equalTo(true));
          assertThat(foundHumidityWindow, equalTo(true));
          return null;
        });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void lateEventWithinAllowedLatenessIsStillCounted() {
    // An event that arrives after the watermark has passed its window, but
    // within the 10-minute allowed lateness, must still be incorporated --
    // this is the whole point of allowedLateness rather than dropping
    // anything the watermark has moved past.
    TestStream<MetricEvent> events = TestStream.create(
            org.apache.beam.sdk.coders.SerializableCoder.of(MetricEvent.class))
        .advanceWatermarkTo(BASE)
        .addElements(event("e1", "temperature", 10.0, BASE))
        .advanceWatermarkTo(BASE.plus(Duration.standardMinutes(6)))
        // Late, event-time timestamp still falls in the [0,5min) window.
        .addElements(event("e2-late", "temperature", 30.0, BASE.plus(Duration.standardSeconds(1))))
        .advanceWatermarkToInfinity();

    PCollection<WindowedStats> result =
        pipeline.apply(events).apply("WindowAndAggregate", new WindowAndAggregate());

    PAssert.that(result)
        .satisfies((SerializableFunction<Iterable<WindowedStats>, Void>) stats -> {
          boolean sawBothEvents = false;
          for (WindowedStats s : stats) {
            if (s.eventType.equals("temperature") && s.count == 2) {
              sawBothEvents = true;
              assertThat(s.avgValue, closeTo(20.0, 1e-9));
            }
          }
          assertThat(sawBothEvents, equalTo(true));
          return null;
        });

    pipeline.run().waitUntilFinish();
  }

  private static org.apache.beam.sdk.values.TimestampedValue<MetricEvent> event(
      String id, String eventType, double value, Instant ts) {
    return org.apache.beam.sdk.values.TimestampedValue.of(
        new MetricEvent(id, "device-1", eventType, value, ts.getMillis()), ts);
  }
}
