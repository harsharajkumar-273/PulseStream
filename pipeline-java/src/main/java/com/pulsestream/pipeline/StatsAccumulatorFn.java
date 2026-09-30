package com.pulsestream.pipeline;

import java.io.Serializable;
import org.apache.beam.sdk.transforms.Combine;

/**
 * Streaming avg/min/max/count in one pass, so we never materialize the full
 * per-window event list. Mirrors spark_processor.py's
 * {@code .agg(avg(...), min(...), max(...), count(...))} call.
 */
public class StatsAccumulatorFn extends Combine.CombineFn<Double, StatsAccumulatorFn.Accum, double[]> {

  public static final class Accum implements Serializable {
    double sum = 0.0;
    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;
    long count = 0;
  }

  @Override
  public Accum createAccumulator() {
    return new Accum();
  }

  @Override
  public Accum addInput(Accum accum, Double input) {
    accum.sum += input;
    accum.min = Math.min(accum.min, input);
    accum.max = Math.max(accum.max, input);
    accum.count += 1;
    return accum;
  }

  @Override
  public Accum mergeAccumulators(Iterable<Accum> accums) {
    Accum merged = createAccumulator();
    for (Accum a : accums) {
      merged.sum += a.sum;
      merged.min = Math.min(merged.min, a.min);
      merged.max = Math.max(merged.max, a.max);
      merged.count += a.count;
    }
    return merged;
  }

  /** Returns [avg, min, max, count]. */
  @Override
  public double[] extractOutput(Accum accum) {
    double avg = accum.count == 0 ? 0.0 : accum.sum / accum.count;
    return new double[] {avg, accum.min, accum.max, accum.count};
  }
}
