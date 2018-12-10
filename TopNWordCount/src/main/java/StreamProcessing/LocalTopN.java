package StreamProcessing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class LocalTopN extends TopN
        implements FlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

    LocalTopN(int n) { super(n); }

    public void flatMap(Tuple2<String, Integer> entry,
                        Collector<Tuple2<String, Integer>> out) {
        if (evictWith(entry)) {
            out.collect(entry);
        }
    }
}