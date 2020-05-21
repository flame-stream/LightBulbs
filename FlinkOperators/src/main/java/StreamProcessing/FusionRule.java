package StreamProcessing;

import org.apache.flink.api.common.functions.MapFunction;

import java.util.TreeMap;
import java.util.function.BinaryOperator;

public class FusionRule implements MapFunction<Stat, Integer> {
    private final TreeMap<Integer, double[]> actual = new TreeMap<>();
    private int cnt = 0;

    public static final double[] thresholds = new double[]{0.24, 0.15, 0.9, 0.7};

    @Override
    public Integer map(Stat stat) throws Exception {
        actual.put(stat.node, stat.stats);
        double[] stats = actual.values().stream()
            .reduce(
                new double[4],
                (doubles, doubles2) -> {
                    double[] ans = new double[4];
                    for (int i = 0; i < 4; i++) {
                        ans[i] = doubles[i] + doubles2[i];
                    }
                    return ans;
                }
            );
        int flag = 0;
        for (int i = 0; i < 4; i++) {
            stats[i] /= actual.values().size();
            if (stats[i] > thresholds[i]) {
                flag = 1;
            }
        }
        cnt++;
        /*if (cnt % 1000 == 0) {
            System.out.println(String.format("Flag = %d", flag));
        }*/
        //return stat.id;
        return flag;
    }
}
