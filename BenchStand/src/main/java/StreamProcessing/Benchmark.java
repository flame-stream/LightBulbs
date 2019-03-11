package StreamProcessing;

import com.esotericsoftware.minlog.Log;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

public class Benchmark {

    public static void main(String[] args) throws InterruptedException {
        Log.set(Log.LEVEL_DEBUG);
        final Config benchConfig;
        if (args.length > 0) {
            benchConfig = ConfigFactory.parseFile(new File(args[0]));
        } else {
            benchConfig = ConfigFactory.load("bench.conf");
        }

        final GraphDeployer deployer = new GraphDeployer() {
            @Override
            public void deploy() {
                final int parallelism = benchConfig.getInt("job.parallelism");
                final StreamExecutionEnvironment env;
                if (benchConfig.hasPath("remote")) {
                    env = StreamExecutionEnvironment.createRemoteEnvironment(
                            benchConfig.getString("remote.manager_hostname"), benchConfig.getInt("remote.manager_port"),
                            parallelism, "BenchStand.jar");
                } else {
                    env = StreamExecutionEnvironment.createLocalEnvironment(parallelism);
                }

                final String hostname = benchConfig.getString("job.bench_host");
                final int frontPort = benchConfig.getInt("job.source_port");
                final int rearPort = benchConfig.getInt("job.sink_port");
                final int checkEach = benchConfig.getInt("job.check_each");
                final int logEach = benchConfig.getInt("job.log_each");

                env.addSource(new KryoSocketSource(hostname, frontPort))
                   .map(new ZipfDistributionValidator(checkEach, 0.05, 100, logEach))
                   .addSink(new KryoSocketSink(hostname, rearPort));

                new Thread(() -> {
                    try {
                        env.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).start();
            }

            @Override
            public void close() {}
        };

        try (BenchStand benchStand = new BenchStand(benchConfig, deployer)) {
            benchStand.run();
        }
        System.exit(0);
    }
}
