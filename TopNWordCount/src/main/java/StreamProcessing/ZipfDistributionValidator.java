package StreamProcessing;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.apache.commons.math3.stat.inference.ChiSquareTest;
import org.apache.commons.math3.util.FastMath;

import java.util.Arrays;

public class ZipfDistributionValidator implements SinkFunction<Tuple2<String, Integer>> {
    private long counter;
    private double harmonicNum;

    private final int each;
    private final double alpha;
    private final double s;
    private final double beta;
    private final int boundary;

    private final Object2IntOpenHashMap<String> counts;
    private final DoubleArrayList reciprocals;

    public ZipfDistributionValidator(int each, double alpha, double S, double beta, int boundary) {
        counter = 0L;
        harmonicNum = 0;

        this.each = each;
        this.alpha = alpha;
        this.s = S;
        this.beta = beta;
        this.boundary = boundary;

        counts = new Object2IntOpenHashMap<>();
        reciprocals = new DoubleArrayList();
    }

    private void verifyDistribution(boolean numUniqueWordsChanged) {
        if (numUniqueWordsChanged) {
            reciprocals.add(1.0 / FastMath.pow(counts.size() + beta, s));
            harmonicNum += reciprocals.getDouble(reciprocals.size() - 1);
        }

        if (counter > boundary && counter % each == 0) {
            double[] expected = new double[counts.size()];
            for (int i = 0; i < expected.length; i++) {
                expected[i] = counter * reciprocals.getDouble(i) / harmonicNum;
            }

            int[] observed = counts.values().toIntArray();
            IntArrays.quickSort(observed, (a, b) -> b - a);

            ChiSquareTest test = new ChiSquareTest();
            if (test.chiSquareTest(expected, Arrays.stream(observed).asLongStream().toArray(), alpha))
                System.err.println("Invalid!");
            else
                System.err.println("Valid");
        }
    }

    @Override
    public void invoke(Tuple2<String, Integer> entry, Context context) {
        counter++;
        int oldCount = counts.put(entry.f0, entry.f1.intValue());

        // If we got default value of 0, then it's a new word
        verifyDistribution(oldCount == 0);
    }
}