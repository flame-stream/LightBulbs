package StreamProcessing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Test;

import java.util.concurrent.TimeUnit;


public class ZipfDistributionValidatorTest {
    private final String dataDirectoryName = "test-data/";

    @Test
    public void validateWarAndPeace() throws Exception {
        String filePath = dataDirectoryName + "war_and_peace.txt";
        validateText(filePath, 1000, 0.05, 1.16, 3, 1, 1000);
    }

    @Test
    public void validateExpon() throws Exception {
        String filePath = dataDirectoryName + "expon.txt";
        validateText(filePath,1000, 0.05,4.5,28,1,1000);
    }

    @Test
    public void validateUniform() throws Exception {
        String filePath = dataDirectoryName + "uniform.txt";
        validateText(filePath, 1000, 0.05, 1, 6, 1, 1000);
    }

    @Test
    public void validateBrown() throws Exception {
        String filePath = dataDirectoryName + "brown.txt";
        validateText(filePath,1000, 0.05,1.05,1,1,1000);
    }

    @Test
    public void validateCorpus() throws Exception {
        String filePath = dataDirectoryName + "corpus.txt";
        validateText(filePath, 1000, 0.05, 1.15, 3, 1, 1000);
    }

    @Test
    public void validateCorruptWarAndPeace() throws Exception {
        String filePath = dataDirectoryName + "corrupt_war_and_peace.txt";
        validateText(filePath,1000, 0.05,1.16,3,1,1000);
    }


    private void validateText(final String filePath, final int each, final double alpha, final double s,
                              final double beta, final int parallelism, int boundary) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<String> text = env.readTextFile(filePath).setParallelism(1);

        DataStream<Tuple2<String, Integer>> counts;
        counts = text.flatMap(new TopNWordCount.Splitter())
                     .keyBy(0)
                     .sum(1);
        counts.addSink(new ZipfDistributionValidator(each, alpha, s, beta, boundary))
              .name("Validator")
              .setParallelism(parallelism);

        final JobExecutionResult result = env.execute("Top N Word Count");
        System.out.println("Job took " + result.getNetRuntime(TimeUnit.MILLISECONDS) + " milliseconds to finish.");
    }
}
