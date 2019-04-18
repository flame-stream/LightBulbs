package StreamProcessing;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.io.IOException;

public class Benchmark {

    public static void main(String[] args) throws IOException, InterruptedException {
        final Config benchConfig;
        if (args.length > 0) {
            benchConfig = ConfigFactory.parseFile(new File(args[0]));
        } else {
            benchConfig = ConfigFactory.load("bench.conf");
        }

        final Runnable deployer = () -> {
            final int parallelism = benchConfig.getInt("job.parallelism");
            final StreamExecutionEnvironment env;
            if (benchConfig.hasPath("remote")) {
                env = StreamExecutionEnvironment.createRemoteEnvironment(
                        benchConfig.getString("remote.manager_hostname"),
                        benchConfig.getInt("remote.manager_port"), parallelism, "BenchStand.jar");
            } else {
                env = StreamExecutionEnvironment.createLocalEnvironment(parallelism);
            }
            env.setBufferTimeout(0);

            final String hostname = benchConfig.getString("job.bench_host");
            final int frontPort = benchConfig.getInt("job.source_port");
            final int rearPort = benchConfig.getInt("job.sink_port");
            final int meanDelay = benchConfig.getInt("job.mean_delay");

            env.addSource(new KryoSocketSource(hostname, frontPort))
               .keyBy("word")
               .map(new Sleeper(meanDelay))
               .addSink(new KryoSocketSink(hostname, rearPort));

            new Thread(() -> {
                try {
                    env.execute();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, "Flink Job").start();
        };

        try (BenchStand benchStand = new BenchStand(benchConfig, deployer)) {
            benchStand.run();
        }
        System.exit(0);
    }
}
