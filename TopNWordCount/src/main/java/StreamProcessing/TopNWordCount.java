package StreamProcessing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class TopNWordCount {

    /**
     * Create a source that continuously monitors a file for changes.
     * Only needed to keep the source task running, otherwise the source task
     * finishes quickly and prevents Flink from creating checkpoints.
     *
     * @param filePath Path to the file to be read
     * @param env      Stream environment to attach the source to
     * @return Stream with file contents
     */
    private static DataStream<String> readAndMonitorTextFile(String filePath, StreamExecutionEnvironment env) {
        TextInputFormat format = new TextInputFormat(new Path(filePath));
        format.setFilesFilter(FilePathFilter.createDefaultFilter());
        format.setCharsetName("UTF-8");
        TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
        return env.readFile(format, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 10000, typeInfo);
    }

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final int n;
        if (params.has("n")) {
            n = params.getInt("n");
        } else {
            n = 10;
        }

        final String filePath;
        if (params.has("in")) {
            filePath = params.get("in");
        } else {
            filePath = "test-data/war_and_peace.txt";
        }

        final int branchingFactor;
        if (params.has("b")) {
            branchingFactor = params.getInt("b");
        } else {
            branchingFactor = 2;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final String checkpointsDir = "file://" + System.getProperty("user.dir") + "/state";
        final DataStream<String> text;
        if (params.has("chk")) {
            text = readAndMonitorTextFile(filePath, env);
            env.enableCheckpointing(5000)
               .setStateBackend((StateBackend) new FsStateBackend(checkpointsDir));
        } else {
            text = env.readTextFile(filePath)
                      .setParallelism(1);
        }

        DataStream<Tuple2<String, Integer>> counts;
        counts = text.flatMap(new Splitter())
                     .keyBy(0)
                     .sum(1);

//        DataStream<Tuple2<String, Integer>> topN = counts.flatMap(new LocalTopN(n));
//        int layerParallelism = env.getParallelism() / branchingFactor;
//        while (layerParallelism > 1) {
//            topN = topN.keyBy(0)
//                       .flatMap(new LocalTopN(n))
//                       .setParallelism(layerParallelism);
//            layerParallelism /= branchingFactor;
//        }
//
//        topN.addSink(new GlobalTopN(n))
//            .name("Top N")
//            .setParallelism(1);

        final JobExecutionResult result = env.execute("Top N Word Count");
        System.out.println(
                "Job took " +
                result.getNetRuntime(TimeUnit.MILLISECONDS) +
                " milliseconds to finish.");
        //System.out.println(env.getExecutionPlan());
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) {
            for (String word : sentence.toLowerCase().split("[^а-яa-z0-9]+")) {
                if (!word.isEmpty()) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }
    }
}