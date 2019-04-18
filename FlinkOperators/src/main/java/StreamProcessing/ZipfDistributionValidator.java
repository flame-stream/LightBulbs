package StreamProcessing;

import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import org.apache.flink.api.common.functions.MapFunction;

import org.apache.commons.math3.stat.inference.ChiSquareTest;
import org.apache.commons.math3.util.FastMath;

import java.util.Arrays;

public class ZipfDistributionValidator implements MapFunction<WordWithID, Integer> {
    private long counter;
    private final int each;
    private final double alpha;
    private final int boundary;
    private final Object2IntOpenHashMap<String> counts;

    public ZipfDistributionValidator(int each, double alpha, int boundary) {
        counter = 0L;
        this.each = each;
        this.alpha = alpha;
        this.boundary = boundary;

        counts = new Object2IntOpenHashMap<>();
    }

    private double[] parameterEstimation(int[] sorted_counts, int start, int minValue) {
        double[] parameters = new double[2];
        int bound = sorted_counts.length;
        for (int i = sorted_counts.length - 1; i >= 0; i--) {
            if (sorted_counts[i] == minValue) {
                bound = i + 1;
                break;
            }
        }

        double[] logf = new double[bound];
        double sumLogf = 0;
        double[] logn = new double[bound];
        double sumLogn = 0;
        double dotLogfLogn = 0;
        double dotLognLogn = 0;

        for (int i = start; i < bound; i++) {
            logf[i] = FastMath.log(sorted_counts[i]);
            sumLogf += logf[i];
            logn[i] = FastMath.log(i);
            sumLogn += logn[i];
            dotLogfLogn += logf[i] * logn[i];
            dotLognLogn += logn[i] * logn[i];
        }

        parameters[0] = ((bound - start) * dotLogfLogn / sumLogn - sumLogf) /
                        (sumLogn - (bound - start) * dotLognLogn / sumLogn);
        if (parameters[0] < 1) {
            parameters[0] = 1;
        }
        parameters[1] = FastMath.exp((parameters[0] * dotLognLogn + dotLogfLogn) / sumLogn);
        System.out.println(parameters[0]);
        System.out.println(parameters[1]);
        return parameters;
    }

    private boolean verifyDistribution() {
        int[] observed = counts.values()
                               .toIntArray();
        IntArrays.quickSort(observed, (a, b) -> b - a);

        double[] parameters = parameterEstimation(observed, 10, 5);
        double s = parameters[0];
        double multiplier = parameters[1];

        double[] expected = new double[counts.size()];
        for (int i = 0; i < expected.length; i++) {
            expected[i] = multiplier / FastMath.pow(i + 1, s);
        }

        int skip = (int) (expected.length * 0.005);

        ChiSquareTest test = new ChiSquareTest();
        return !test.chiSquareTest(Arrays.copyOfRange(expected, skip, expected.length),
                                   Arrays.copyOfRange(Arrays.stream(observed)
                                                            .asLongStream()
                                                            .toArray(), skip, observed.length),
                                   alpha);
    }

    @Override
    public Integer map(WordWithID entry) {
        counter++;
        counts.addTo(entry.word, 1);

        if (counter > boundary && counter % each == 0) {
            verifyDistribution();
        }

        return entry.id;
    }
}