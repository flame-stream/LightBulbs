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
    public void validateWarAndPeaceParallelism1() throws Exception {
        String filePath = dataDirectoryName + "war_and_peace.txt";
        assertTrue(validateText(filePath, 1000, 0.05, 1, 1000));
    }

    @Test
    public void validateWarAndPeaceParallelism2() throws Exception {
        String filePath = dataDirectoryName + "war_and_peace.txt";
        assertTrue(validateText(filePath, 1000, 0.05, 2, 1000));
    }

    @Test
    public void validateWarAndPeaceParallelism4() throws Exception {
        String filePath = dataDirectoryName + "war_and_peace.txt";
        assertTrue(validateText(filePath, 1000, 0.05, 4, 1000));
    }

    @Test
    public void validateExponParallelism1() throws Exception {
        String filePath = dataDirectoryName + "expon.txt";
        assertFalse(validateText(filePath, 1000, 0.05, 1, 1000));
    }

    @Test
    public void validateExponParallelism2() throws Exception {
        String filePath = dataDirectoryName + "expon.txt";
        assertFalse(validateText(filePath, 1000, 0.05, 2, 1000));
    }

    @Test
    public void validateExponParallelism4() throws Exception {
        String filePath = dataDirectoryName + "expon.txt";
        assertFalse(validateText(filePath, 1000, 0.05, 4, 1000));
    }

    @Test
    public void validateUniformParallelism1() throws Exception {
        String filePath = dataDirectoryName + "uniform.txt";
        assertFalse(validateText(filePath, 1000, 0.05, 1, 1000));
    }

    @Test
    public void validateUniformParallelism2() throws Exception {
        String filePath = dataDirectoryName + "uniform.txt";
        assertFalse(validateText(filePath, 1000, 0.05, 2, 1000));
    }

    @Test
    public void validateUniformParallelism4() throws Exception {
        String filePath = dataDirectoryName + "uniform.txt";
        assertFalse(validateText(filePath, 1000, 0.05, 4, 1000));
    }

    @Test
    public void validateBrownParallelism1() throws Exception {
        String filePath = dataDirectoryName + "brown.txt";
        assertTrue(validateText(filePath, 1000, 0.05, 1, 1000));
    }

    @Test
    public void validateBrownParallelism2() throws Exception {
        String filePath = dataDirectoryName + "brown.txt";
        assertTrue(validateText(filePath, 1000, 0.05, 2, 1000));
    }

    @Test
    public void validateBrownParallelism4() throws Exception {
        String filePath = dataDirectoryName + "brown.txt";
        assertTrue(validateText(filePath, 1000, 0.05, 4, 1000));
    }

    @Test
    public void validateCorpusParallelism1() throws Exception {
        String filePath = dataDirectoryName + "corpus.txt";
        assertTrue(validateText(filePath, 1000, 0.05, 1, 1000));
    }

    @Test
    public void validateCorpusParallelism2() throws Exception {
        String filePath = dataDirectoryName + "corpus.txt";
        assertTrue(validateText(filePath, 1000, 0.05, 2, 1000));
    }

    @Test
    public void validateCorpusParallelism4() throws Exception {
        String filePath = dataDirectoryName + "corpus.txt";
        assertTrue(validateText(filePath, 1000, 0.05, 4, 1000));
    }


    private boolean validateText(final String filePath, final int each, final double alpha,
                                 final int parallelism, int boundary) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<String> text = env.readTextFile(filePath);

        DataStream<Tuple2<String, Integer>> counts = text.flatMap(new TopNWordCount.Splitter())
                .keyBy(0)
                .sum(1);

        counts.flatMap(new ZipfDistributionValidator(each, alpha, boundary))
                .setParallelism(parallelism)
                .addSink(new CollectSink())
                .name("Validator");
        //System.out.println(env.getExecutionPlan());
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
