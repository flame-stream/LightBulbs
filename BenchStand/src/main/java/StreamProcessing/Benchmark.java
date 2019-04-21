package StreamProcessing;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Benchmark {

    public static void main(String[] args) throws Exception {
        final Config config;
        if (args.length > 0) {
            config = ConfigFactory.parseFile(new File(args[0]));
        } else {
            config = ConfigFactory.load("bench.conf");
        }

        final BenchConfig benchConfig = new BenchConfig(
                config.getString("benchstand.file_path"),
                config.getInt("benchstand.word_count"),
                config.getInt("benchstand.drop_first_n_words"),
                config.hasPath("remote") ? config.getString("remote.manager_hostname") : null,
                config.hasPath("remote") ? config.getInt("remote.manager_port") : -1,
                config.getInt("job.computation_delay"),
                config.getInt("job.validation_delay"),
                config.getInt("job.log_each"),
                config.getString("job.bench_host"),
                config.getInt("job.source_port"),
                config.getInt("job.sink_port"));

        final List<Integer> parallelism = config.getIntList("benchstand.parallelism");
        final List<Double> steps = config.getDoubleList("benchstand.steps");
        final long minDelayBetweenWords = config.getLong("benchstand.min_delay_between_words");
        final long maxDelayBetweenWords = config.getLong("benchstand.max_delay_between_words");

        final String log = System.getProperty("user.home") + "/results.txt";
        try (PrintWriter pw = new PrintWriter(log, "UTF-8")) {
            pw.println("Shuffle on N nodes");
            benchConfig.mergeOnSingleNode = false;

            for (int i = 0; i < parallelism.size(); i++) {
                int p = parallelism.get(i);
                double s = steps.get(i);
                pw.println("Parallelism " + p);
                benchConfig.parallelism = p;
                benchConfig.delayBetweenWords =
                        selectDelayBetweenWords(benchConfig,
                                                (long) (minDelayBetweenWords / s),
                                                maxDelayBetweenWords);
                for (int j = 0; j < 5; j++) {
                    pw.println(benchmark(benchConfig));
                    pw.flush();
                }
            }

            pw.println();
            pw.println("Shuffle on N nodes, then merge on 1 node");
            benchConfig.mergeOnSingleNode = true;

            for (int i = 0; i < parallelism.size(); i++) {
                int p = parallelism.get(i);
                double s = steps.get(i);
                pw.println("Parallelism " + p);
                benchConfig.parallelism = p;
                benchConfig.delayBetweenWords = selectDelayBetweenWords(benchConfig,
                                                                        (long) (minDelayBetweenWords / s),
                                                                        maxDelayBetweenWords);
                for (int j = 0; j < 5; j++) {
                    pw.println(benchmark(benchConfig));
                    pw.flush();
                }
            }
        }
    }

    public static long selectDelayBetweenWords(BenchConfig config, long minDelay,
                                               long maxDelay) throws Exception {
        List<Tuple2<Long, Double>> latencies = new ArrayList<>();
        for (long delay = minDelay; delay < maxDelay; delay += 100_000) {
            config.delayBetweenWords = delay;
            latencies.add(Tuple2.of(delay, benchmark(config)._99));
        }

        Tuple2<Long, Double> minLatency = latencies.get(0);
        double minScore = minLatency.f0 * minLatency.f1;
        for (Tuple2<Long, Double> l : latencies) {
            if (l.f0 * l.f1 < minScore) {
                minLatency = l;
                minScore = l.f0 * l.f1;
            }
        }
        return minLatency.f0;
    }

    public static BenchResult benchmark(BenchConfig config) throws Exception {
        final Runnable deployer = () -> {
            final StreamExecutionEnvironment env;
            if (config.managerHostname != null) {
                env = StreamExecutionEnvironment.createRemoteEnvironment(
                        config.managerHostname, config.managerPort,
                        config.parallelism, "BenchStand.jar");
            } else {
                env = StreamExecutionEnvironment.createLocalEnvironment(config.parallelism);
            }

            final SingleOutputStreamOperator<WordWithID> words =
                    env.addSource(new KryoSocketSource(config.benchHost, config.sourcePort))
                       .keyBy("word")
                       .map(new Pass(config.computationDelay));
            if (config.mergeOnSingleNode) {
                words.map(new Sleeper(config.validationDelay))
                     .setParallelism(1)
                     .addSink(new KryoSocketSink(config.benchHost, config.sinkPort))
                     .setParallelism(1);
            } else {
                words.map(new Sleeper(config.validationDelay))
                     .addSink(new KryoSocketSink(config.benchHost, config.sinkPort));
            }
            env.setBufferTimeout(0);

            new Thread(() -> {
                try {
                    env.execute();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, "Flink Job").start();
        };

        try (BenchStand benchStand = new BenchStand(config, deployer)) {
            return benchStand.run();
        }
    }
}
