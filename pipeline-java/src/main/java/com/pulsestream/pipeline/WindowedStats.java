package com.pulsestream.pipeline;

import java.io.Serializable;

/**
 * Output row: the same four statistics spark_processor.py computes
 * (avg/min/max/count) per eventType per window, so the two pipelines are
 * directly comparable.
 */
public final class WindowedStats implements Serializable {
  public final String eventType;
  public final double avgValue;
  public final double minValue;
  public final double maxValue;
  public final long count;
  public final String windowStart;
  public final String windowEnd;

  public WindowedStats(String eventType, double avgValue, double minValue, double maxValue,
      long count, String windowStart, String windowEnd) {
    this.eventType = eventType;
    this.avgValue = avgValue;
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.count = count;
    this.windowStart = windowStart;
    this.windowEnd = windowEnd;
  }

  @Override
  public String toString() {
    return String.format(
        "[%s, %s) eventType=%s count=%d avg=%.3f min=%.3f max=%.3f",
        windowStart, windowEnd, eventType, count, avgValue, minValue, maxValue);
  }
}
