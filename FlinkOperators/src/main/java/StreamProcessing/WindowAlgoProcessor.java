package StreamProcessing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

public class WindowAlgoProcessor implements MapFunction<WordWithID, Stat> {
  private static final int[] WINDOW_SIZES = new int[]{200, 400, 800, 1600};
  private final ArrayList<ArrayDeque<Double>> referenceWindows = new ArrayList<>();
  private final ArrayList<ArrayDeque<Double>> slidingWindows = new ArrayList<>();
  private final double[] ans = new double[4];
  private int cnt = 0;

  public WindowAlgoProcessor() {
    System.out.println(String.format("Created node %d", this.hashCode()));
    for (int i = 0; i < 4; i++) {
      referenceWindows.add(new ArrayDeque<>());
      slidingWindows.add(new ArrayDeque<>());
      ans[i] = 0;
    }
  }

  private double kolmogorovSmirnovDistance(Collection<Double> sample1, Collection<Double> sample2) {
    ArrayList<Tuple2<Double, Double>> kek = sample1.stream()
            .map(x -> new Tuple2<>(x, -1 / (double) sample1.size()))
            .collect(Collectors.toCollection(ArrayList::new));
    kek.addAll(
            sample2.stream()
                    .map(x -> new Tuple2<>(x, 1 / (double) sample2.size()))
                    .collect(Collectors.toCollection(ArrayList::new))
    );
    kek.sort(Comparator.comparing(x -> x.f0));
    double max = 0;
    double prefix = 0;
    for (Tuple2<Double, Double> pair: kek) {
      prefix += pair.f1;
      max = Math.max(max, Math.abs(prefix));
    }
    return max;
  }

  @Override
  public Stat map(WordWithID word) {
    double value = word.word.length();
    cnt++;
    for (int i = 0; i < 4; i++) {
      if (referenceWindows.get(i).size() < WINDOW_SIZES[i]) {
        referenceWindows.get(i).addLast(value);
      } else if (slidingWindows.get(i).size() < WINDOW_SIZES[i]) {
        slidingWindows.get(i).addLast(value);
      } else {
        slidingWindows.get(i).removeFirst();
        slidingWindows.get(i).addLast(value);
        ans[i] = Math.max(ans[i], kolmogorovSmirnovDistance(referenceWindows.get(i), slidingWindows.get(i)));
        /*if (cnt % 1000 == 0) {
          System.out.println(String.format("Node = %d, size = %d, stat = %f"
                  , this.hashCode()
                  , WINDOW_SIZES[i]
                  , ans[i]
          ));
        }*/
      }
    }

    return new Stat(ans.clone(), this.hashCode(), word.id);
  }
}
