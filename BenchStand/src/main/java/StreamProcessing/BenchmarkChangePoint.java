package StreamProcessing;

import akka.actor.Identify;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.base.Functions;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class BenchmarkChangePoint {

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

        final String log = System.getProperty("user.home") + "/results.txt";
        try (PrintWriter pw = new PrintWriter(log, "UTF-8")) {
            pw.println();
            pw.println("Shuffle on N nodes, then merge on 1 node");
            benchConfig.mergeOnSingleNode = true;

            for (int p : parallelism) {
                pw.println("Parallelism " + p);
                benchConfig.parallelism = p;
                for (int j = 0; j < 5; j++) {
                    pw.println(benchmark(benchConfig));
                    pw.flush();
                }
            }
        }
    }

    public static BenchResult benchmark(BenchConfig config) throws Exception {
        final Runnable deployer = () -> {
            final StreamExecutionEnvironment env;
            int parallelism = config.parallelism;

            if (config.managerHostname != null) {
                env = StreamExecutionEnvironment.createRemoteEnvironment(
                        config.managerHostname, config.managerPort,
                        parallelism, "BenchStand.jar");
            } else {
                env = StreamExecutionEnvironment.createLocalEnvironment(parallelism);
            }

            final SingleOutputStreamOperator<Stat> words =
                    env.addSource(new KryoSocketSource(config.benchHost, config.sourcePort))
                            .keyBy("word")
                            .map(new WindowAlgoProcessor())
                            .setParallelism(parallelism);
            words.map(x -> x)
                .setParallelism(1)
                .map(new FusionRule())
                .addSink(new KryoSocketSink(config.benchHost, config.sinkPort))
                .setParallelism(1);
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
