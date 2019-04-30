package StreamProcessing;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
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
        final List<Integer> benchSleep = config.getIntList("benchstand.bench_sleep");
        final int numRuns = config.getInt("benchstand.num_runs");

        final String header = "bench_delay,_50,_75,_90,_99,time_total,throughput";
        final String shuffle = System.getProperty("user.home") + "/shuffle.txt";
        try (PrintWriter pw = new PrintWriter(new FileWriter(shuffle), true)) {
            benchConfig.mergeOnSingleNode = false;
            for (int j = 0; j < parallelism.size(); j++) {
                int p = parallelism.get(j);
                int delay = benchSleep.get(j);
                benchConfig.parallelism = p;
                pw.println("Parallelism: " + p);
                pw.println(header);
                for (int i = 0; i < numRuns; i++) {
                    benchConfig.delayBetweenWords = delay;
                    pw.println(delay + "," + benchmark(benchConfig));
                }
            }
        }

        final String merge = System.getProperty("user.home") + "/merge.txt";
        try (PrintWriter pw = new PrintWriter(new FileWriter(merge), true)) {
            benchConfig.mergeOnSingleNode = true;
            for (int j = 0; j < parallelism.size(); j++) {
                int p = parallelism.get(j);
                int delay = benchSleep.get(j);
                benchConfig.parallelism = p;
                pw.println("Parallelism: " + p);
                pw.println(header);
                for (int i = 0; i < numRuns; i++) {
                    benchConfig.delayBetweenWords = delay;
                    pw.println(delay + "," + benchmark(benchConfig));
                }
            }
        }
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

            SingleOutputStreamOperator<WordWithID> words =
                    env.addSource(new KryoSocketSource(config.benchHost, config.sourcePort))
                       .keyBy("word")
                       .map(new PassPi<>(config.computationDelay));
            if (config.mergeOnSingleNode) {
                words.map(new PiSleeper(config.validationDelay))
                     .setParallelism(1)
                     .addSink(new KryoSocketSink(config.benchHost, config.sinkPort))
                     .setParallelism(1);
            } else {
                words.map(new PiSleeper(config.validationDelay))
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
