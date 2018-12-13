package StreamProcessing;

import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class ZipfDistributionValidator implements SinkFunction<Tuple2<String, Integer>> {

    static private final double EPS = 1e-8;
    private long counter;
    private double[] zipfCDF;
    private int[] sortedCounts;
    private final Object2IntOpenHashMap<String> counts;

    public ZipfDistributionValidator() {
        this.counter = 0L;
        this.counts = new Object2IntOpenHashMap<>();
    }

    private void verifyDistribution(boolean numUniqueWordsChanged) {
        // If number of unique words has increased, allocate a new array
        // of bigger capacity and recalculate Zipf CDF, else reuse the old array
        if (numUniqueWordsChanged) {
            sortedCounts = counts.values()
                                 .toIntArray();
            zipfCDF = new double[counts.size()];
            zipfCDF[0] = 1.0;
            for (int i = 1; i < zipfCDF.length; i++) {
                zipfCDF[i] = zipfCDF[i - 1] + zipfCDF[i];
            }
            int last = zipfCDF.length - 1;
            for (int i = 0; i < zipfCDF.length; i++) {
                zipfCDF[i] /= zipfCDF[last];
            }
        } else {
            counts.values()
                  .toArray(sortedCounts);
        }
        IntArrays.quickSort(sortedCounts, (x, y) -> y - x);

        long sumCounts = 0L;
        for (int count : sortedCounts) {
            sumCounts += count;
        }

        // Calculate test statistics
        double d = 0.0;
        long cumSum = 0L;
        for (int i = 0; i < sortedCounts.length; i++) {
            cumSum += sortedCounts[i];
            double ecdf = (double) cumSum / sumCounts;
            double diff = Math.abs(zipfCDF[i] - ecdf);
            if (diff > d) {
                d = diff;
            }
        }

        double[] cs = getProbabilitiesC(d);
        double[] fs = getProbabilitiesF(d);
        double criticalLevelMinus = getCriticalLevel(cs);
        double criticalLevelPlus = getCriticalLevel(fs);
        double criticalLevel = criticalLevelMinus + criticalLevelPlus;

        if (counter % 100 == 0) {
            System.out.println(
                    counter + ". " +
                    "d: " + d + ", " +
                    "critical level: " + criticalLevel + ", " +
                    "critical level+: " + criticalLevelMinus + ", " +
                    "critical level-: " + criticalLevelPlus + ", " +
                    "n: " + counts.size());
        }
    }

    private double[] getProbabilitiesC(double d) {
        int n = counts.size();
        double[] c = new double[(int) (n * (1 - d)) + 1];
        for (int j = 0; j < c.length; j++) {
            double line = d + (double) j / n;
            int i = 0;
            while ((i < zipfCDF.length) && (zipfCDF[i] < line)) { i++; }
            if ((i > 0) && (zipfCDF[i - 1] - line < EPS)) {
                c[j] = 1 - line;
            } else {
                c[j] = 1 - zipfCDF[i];
            }
        }
        return c;
    }

    private double[] getProbabilitiesF(double d) {
        int n = counts.size();
        double[] f = new double[(int) (n * (1 - d)) + 1];
        for (int j = 0; j < f.length; j++) {
            double line = 1 - d - (double) j / n;
            int i = 0;
            while ((i < zipfCDF.length) && (zipfCDF[i] < line)) { i++; }
            if (zipfCDF[i] - line < EPS) {
                f[j] = line;
            } else {
                if (i == 0) {
                    f[j] = 0.0;
                } else {
                    f[j] = zipfCDF[i - 1];
                }
            }
        }
        return f;
    }

    private double getCriticalLevel(double[] c) {
        int n = counts.size();
        double[] b = new double[c.length];
        b[0] = 1;

        double localSum = 0;
        int Cjk = 1;
        double[][] cPowers = new double[c.length][n];
        for (int j = 0; j < c.length; j++) {
            cPowers[j][0] = 1;
        }
        for (int j = 0; j < c.length; j++) {
            for (int pow = 1; pow < n; pow++) {
                cPowers[j][pow] = cPowers[j][pow - 1] * c[j];
            }
        }

        for (int k = 1; k < c.length; k++) {
            localSum = Math.pow(c[0], k);
            Cjk = 1;
            for (int j = 1; j < k; j++) {
                Cjk *= ((k - j + 1) / j);
                localSum += Cjk * cPowers[j][k - j] * b[j];
            }
            b[k] = 1 - localSum;
        }

        double criticalLevel = Math.pow(c[0], n) * b[0];
        Cjk = 1;
        for (int j = 1; j < c.length; j++) {
            Cjk *= ((n - j + 1) / j);
            criticalLevel += Cjk * cPowers[j][n - j] * b[j];
        }

        return criticalLevel;
    }

    @Override
    public void invoke(Tuple2<String, Integer> entry, Context context) {
        counter++;
        int oldCount = counts.put(entry.f0, entry.f1.intValue());

        if (counts.size() < 2) {
            System.out.println("We are good anyway.");
        }

        // If we got default value of 0, then it's a new word
        verifyDistribution(oldCount == 0);
    }
}