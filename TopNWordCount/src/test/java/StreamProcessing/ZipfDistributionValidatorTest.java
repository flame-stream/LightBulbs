package StreamProcessing;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.junit.Test;

import static org.junit.Assert.*;

public class ZipfDistributionValidatorTest {
    private final String dataDirectoryName = "test-data/";

    @Test
    public void validateWarAndPeace() throws Exception {
        String filePath = dataDirectoryName + "war_and_peace.txt";
        assertTrue(validateText(filePath, 1000, 0.05, 1.16, 3, 1, 1000));
    }

    @Test
    public void validateExpon() throws Exception {
        String filePath = dataDirectoryName + "expon.txt";
        assertFalse(validateText(filePath, 1000, 0.05, 4.5, 28, 1, 1000));
    }

    @Test
    public void validateUniform() throws Exception {
        String filePath = dataDirectoryName + "uniform.txt";
        assertFalse(validateText(filePath, 1000, 0.05, 1, 6, 1, 1000));
    }

    @Test
    public void validateBrown() throws Exception {
        String filePath = dataDirectoryName + "brown.txt";
        assertTrue(validateText(filePath, 1000, 0.05, 1.05, 1, 1, 1000));
    }

    @Test
    public void validateCorpus() throws Exception {
        String filePath = dataDirectoryName + "corpus.txt";
        assertTrue(validateText(filePath, 1000, 0.05, 1.15, 3, 1, 1000));
    }

    @Test
    public void validateCorruptWarAndPeace() throws Exception {
        String filePath = dataDirectoryName + "corrupt_war_and_peace.txt";
        assertFalse(validateText(filePath, 1000, 0.05, 1.16, 3, 1, 1000));
    }


    private boolean validateText(final String filePath, final int each, final double alpha, final double s,
                              final double beta, final int parallelism, int boundary) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<String> text = env.readTextFile(filePath).setParallelism(1);

        DataStream<Tuple2<String, Integer>> counts = text.flatMap(new TopNWordCount.Splitter())
                                                         .keyBy(0)
                                                         .sum(1);

        counts.flatMap(new ZipfDistributionValidator(each, alpha, s, beta, boundary))
              .setParallelism(parallelism)
              .addSink(new CollectSink())
              .name("Validator");

        env.execute();

        return CollectSink.valid;
    }

    private static class CollectSink implements SinkFunction<Boolean> {
        static boolean valid;

        CollectSink() {
            valid = false;
        }

        @Override
        public void invoke(Boolean entry, Context context) {
            valid = entry;
        }
    }
}
