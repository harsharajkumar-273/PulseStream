package com.pulsestream.pipeline;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses one metrics.raw JSON record into a MetricEvent. A record that
 * fails to parse or is missing a required field goes to the
 * {@link #MALFORMED_TAG} output instead of failing the bundle -- the same
 * "don't let one bad record poison the batch" principle as the Node
 * consumer's per-message SAVEPOINT fix in src/consumer.ts, applied here at
 * the parse step instead of the DB-write step.
 */
public class ParseEventFn extends DoFn<String, MetricEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(ParseEventFn.class);

  public static final TupleTag<MetricEvent> PARSED_TAG = new TupleTag<>() {};
  public static final TupleTag<String> MALFORMED_TAG = new TupleTag<>() {};

  @ProcessElement
  public void processElement(@Element String json, MultiOutputReceiver out) {
    try {
      JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
      if (!obj.has("id") || !obj.has("eventType") || !obj.has("value") || !obj.has("timestamp")) {
        out.get(MALFORMED_TAG).output(json);
        return;
      }
      MetricEvent event = new MetricEvent(
          obj.get("id").getAsString(),
          obj.has("deviceId") && !obj.get("deviceId").isJsonNull() ? obj.get("deviceId").getAsString() : "unknown",
          obj.get("eventType").getAsString(),
          obj.get("value").getAsDouble(),
          obj.get("timestamp").getAsLong());
      out.get(PARSED_TAG).output(event);
    } catch (JsonSyntaxException | IllegalStateException | NullPointerException e) {
      LOG.warn("Dropping malformed metrics.raw record: {}", e.toString());
      out.get(MALFORMED_TAG).output(json);
    }
  }
}
