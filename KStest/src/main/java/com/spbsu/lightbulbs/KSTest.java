package com.spbsu.lightbulbs;

import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesSketchIterator;

import java.util.function.Function;

public class KSTest {
    private final static double EPS = 1e-4;

    private KSTest() {
    }

    /**
     * @param sketch the sketch of the sample
     * @param cdf    the cumulative distribution function
     * @return sqrt(n) * D, D is an estimate of the KS-statistic, n is the sample size
     */
    public static double oneSampleTest(DoublesSketch sketch, Function<Double, Double> cdf) {
        double result = 0;
        DoublesSketchIterator it = sketch.iterator();
        while (it.next()) {
            double x = it.getValue();
            double quantile = binSearch(x, sketch);
            result = Math.max(result, Math.abs(quantile - cdf.apply(x)));
        }

        return Math.sqrt(sketch.getN()) * result;
    }

    /**
     * @param sketch1 the sketch of the first sample
     * @param sketch2 the sketch of the second sample
     * @return sqrt(n * m / ( n + m)) * D, D is an estimate of the KS-statistic, n is the size of the first sample,
     * m is the size of the second sample
     */
    public static double twoSampleTest(DoublesSketch sketch1, DoublesSketch sketch2) {
        long n = sketch1.getN();
        long m = sketch2.getN();
        return Math.sqrt((double) n * m / (n + m))
                * Math.max(twoSampleStatistic(sketch1, sketch2), twoSampleStatistic(sketch2, sketch1));
    }

    /**
     * @param sketch1 the sketch of the first sample
     * @param sketch2 the sketch of the second sample
     * @return an estimate of the KS-statistic
     */
    private static double twoSampleStatistic(DoublesSketch sketch1, DoublesSketch sketch2) {
        double result = 0;
        DoublesSketchIterator it = sketch1.iterator();
        while (it.next()) {
            double x = it.getValue();
            double quantile1 = binSearch(x, sketch1);
            double quantile2 = binSearch(x, sketch2);
            result = Math.max(result, Math.abs(quantile1 - quantile2));
        }
        return result;
    }

    /**
     * @param key    the sketch element
     * @param sketch the sketch of the sample
     * @return quantile of the sketch element
     */
    private static double binSearch(double key, DoublesSketch sketch) {
        double l = 0;
        double r = 1;

        double m = -1;
        while (r - l > EPS) {
            m = (l + r) / 2;

            double quantile = sketch.getQuantile(m);

            int comparison = Double.compare(key, quantile);
            if (comparison == 0) {
                return m;
            }
            if (comparison > 0) {
                l = m;
            } else {
                r = m;
            }
        }

        return m;
    }
}