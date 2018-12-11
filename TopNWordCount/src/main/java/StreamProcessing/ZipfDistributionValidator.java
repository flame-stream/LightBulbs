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
        for (int i = 1; i < sortedCounts.length; i++) {

            localSumCounts += sortedCounts[i - 1];
            localSumZipf += 1.0 / i;
            diff = Math.abs(localSumCounts / counter - localSumZipf / sumZipf);

            if (diff > d) {
                d = diff;
                maxDiffStep = 1;
            }
        }

        double criticalLevel = getCriticalLevel(d, n) * 2;

        if (counter % 1000 == 0) {
            System.out.print("res :");
            System.out.print(maxDiffStep + " ");
            System.out.print(criticalLevel + " ");
            System.out.print(d + " ");
            System.out.print(counter + " ");
            System.out.println(n + " ");
        }
    }

    private double getCriticalLevel(double d, int n) {

        ArrayList<Double> cList = new ArrayList<Double>();
        double cj = 0;
        for (int j = 0; j < n * (1 - d); j++) {
            cj = 1 - d - j * 1.0 / n;
            if (cj > 0) {
                cList.add(cj);
            } else {
                break;
            }
        }

        Double[] c = cList.toArray(new Double[cList.size()]);
        double[] b = new double[cList.size()];
        b[0] = 1;

        double localSum = 0;
        int Cjk = 1;
        for (int k = 1; k < cList.size(); k++) {
            localSum = Math.pow(c[0], k);
            Cjk = 1;
            for (int j = 1; j < k; j++) {
                Cjk *= ((k - j + 1) / j);
                localSum += Cjk * Math.pow(c[j], k - j) * b[j];
            }
            b[k] = 1 - localSum;
        }

        double criticalLevel = Math.pow(c[0], n) * b[0];
        Cjk = 1;
        for (int j = 1; j < cList.size(); j++) {
            Cjk *= ((n - j + 1) / j);
            criticalLevel += Cjk * Math.pow(c[j], n - j) * b[j];
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
        if (counter % 1000 == 0) {
            verifyDistribution();
        }
    }
}