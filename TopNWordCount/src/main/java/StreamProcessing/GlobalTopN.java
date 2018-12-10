package StreamProcessing;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

public class GlobalTopN extends TopN implements SinkFunction<Tuple2<String, Integer>> {

    GlobalTopN(int n) { super(n); }

    @Override
    public void invoke(Tuple2<String, Integer> entry, Context context) {
        if (evictWith(entry)) {
            System.out.println(counts.entrySet()
                                     .stream()
                                     .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                                     .collect(Collectors.toList()));
        }
    }
}