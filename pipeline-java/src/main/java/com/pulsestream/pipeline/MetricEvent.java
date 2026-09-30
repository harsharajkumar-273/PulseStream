package com.pulsestream.pipeline;

import java.io.Serializable;
import java.util.Objects;

/**
 * Mirrors the JSON shape PulseStream's Express API publishes to the
 * {@code metrics.raw} topic (see ../../src/config/kafka.js and
 * ../../src/index.js in the Node.js service): {id, deviceId, eventType,
 * value, timestamp}. timestamp is unix milliseconds, matching
 * spark_processor.py's schema.
 */
public final class MetricEvent implements Serializable {
  public final String id;
  public final String deviceId;
  public final String eventType;
  public final double value;
  public final long timestampMillis;

  public MetricEvent(String id, String deviceId, String eventType, double value, long timestampMillis) {
    this.id = id;
    this.deviceId = deviceId;
    this.eventType = eventType;
    this.value = value;
    this.timestampMillis = timestampMillis;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof MetricEvent)) return false;
    MetricEvent that = (MetricEvent) o;
    return Double.compare(value, that.value) == 0
        && timestampMillis == that.timestampMillis
        && Objects.equals(id, that.id)
        && Objects.equals(deviceId, that.deviceId)
        && Objects.equals(eventType, that.eventType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, deviceId, eventType, value, timestampMillis);
  }

  @Override
  public String toString() {
    return "MetricEvent{id=" + id + ", deviceId=" + deviceId + ", eventType=" + eventType
        + ", value=" + value + ", timestampMillis=" + timestampMillis + "}";
  }
}
