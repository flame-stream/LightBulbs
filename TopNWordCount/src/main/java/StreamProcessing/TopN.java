package StreamProcessing;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TopN implements ListCheckpointed<Tuple2<String, Integer>>, Serializable {

    private final int n;
    HashMap<String, Integer> counts;

    TopN(int n) {
        this.n = n;
        this.counts = new HashMap<>(n);
    }

    /**
     * Updates top N with a new record.
     * If the record changes top N, it is sent further down the stream.
     *
     * @param entry The record received from the stream
     * @return Boolean indicating whether the record changed top N
     */
    boolean evictWith(Tuple2<String, Integer> entry) {
        String word = entry.f0;
        Integer count = entry.f1;

        if (counts.size() < n || counts.containsKey(word)) {
            counts.put(word, count);
            return true;
        } else {
            Map.Entry<String, Integer> min;
            min = counts.entrySet()
                        .stream()
                        .min(Map.Entry.comparingByValue())
                        .get();
            if (min.getValue() < count) {
                counts.remove(min.getKey());
                counts.put(word, count);
                return true;
            }
        }

        return false;
    }

    @Override
    public List<Tuple2<String, Integer>> snapshotState(long checkpointId, long timestamp) {
        return counts.entrySet()
                     .stream()
                     .map(entry -> Tuple2.of(entry.getKey(), entry.getValue()))
                     .collect(Collectors.toList());
    }

    @Override
    public void restoreState(List<Tuple2<String, Integer>> state) {
        state.stream()
             .sorted((word1, word2) -> -Integer.compare(word1.f1, word2.f1))
             .limit(n)
             .forEach(entry -> counts.put(entry.f0, entry.f1));
    }
}