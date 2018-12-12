package StreamProcessing;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.HashMap;

public class ZipfDistributionValidator implements SinkFunction<Tuple2<String, Integer>> {

    private final HashMap<String, Integer> counts;
    private long counter;
    private double sumZipf = 0;
    private int n = 0;

    public ZipfDistributionValidator() {
        this.counter = 0L;
        this.counts = new HashMap<>();
    }

    private void verifyDistribution() {

        int[] sortedCounts = counts.entrySet()
                                   .stream()
                                   .mapToInt((s) -> s.getValue())
                                   .map(i -> -i)
                                   .sorted()
                                   .map(i -> -i)
                                   .toArray();

        double d = 0;
        double localSumCounts = 0;
        double localSumZipf = 0;
        double diff = 0;
        int maxDiffStep = 0;

        double[] zipfDistribFunc = new double[sortedCounts.length];
        zipfDistribFunc[0] = 0;
        zipfDistribFunc[zipfDistribFunc.length - 1] = 1;

        for (int i = 1; i < sortedCounts.length; i++) {
            localSumCounts += sortedCounts[i - 1];
            localSumZipf += 1.0 / i;
            zipfDistribFunc[i] = localSumZipf / sumZipf;

            diff = Math.abs(localSumCounts / counter - localSumZipf / sumZipf);
            if (diff > d) {
                d = diff;
                maxDiffStep = i;
            }
        }
        double[] cMinus = getProbabilitiesC(d, n, zipfDistribFunc, true, 0.001);
        double[] cPlus = getProbabilitiesC(d, n, zipfDistribFunc, false, 0.001);
        double criticalLevelMinus = getCriticalLevel(n, cMinus);
        double criticalLevelPlus = getCriticalLevel(n, cPlus);
        double criticalLevel = criticalLevelMinus + criticalLevelPlus;

        System.out.print("maxDiffStep : " + maxDiffStep + ", ");
        System.out.print("criticalLevel : " + criticalLevel + ", ");
        System.out.print("criticalLevelMinus : " + criticalLevelMinus + ", ");
        System.out.print("criticalLevelPlus : " + criticalLevelPlus + ", ");
        System.out.print("d : " + d + ", ");
        System.out.print("counter : " + counter + ", ");
        System.out.println("n : " + n + ", ");

    }

    private double[] getProbabilitiesC(double d, int n, double[] distribFunc, boolean typeMinus, double eps) {
        double[] c = new double[Math.round((int) (n * (1 - d))) + 1];
        double line = 0;
        int i = 1;
        for (int j = 0; j < c.length; j++) {
            line = d + j * 1.0 / n;
            while (line > distribFunc[i] && i < distribFunc.length - 1) {
                i++;
            }

            if ((typeMinus && Math.abs(line - distribFunc[i - 1]) > eps) || Math.abs(line - distribFunc[i]) < eps) {
                c[j] = 1 - distribFunc[i];
            } else {
                c[j] = 1 - distribFunc[i - 1];

            }
        }
        return c;
    }

    private double getCriticalLevel(int n, double[] c) {

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
        counts.put(entry.f0, entry.f1);
        counter++;
        for (int i = n + 1; i <= counts.size(); i++) {
            sumZipf += 1.0 / i;
        }
        n = counts.size();
        if (counter % 100 == 0 && counter < 30001) {
            verifyDistribution();
        }
    }
}