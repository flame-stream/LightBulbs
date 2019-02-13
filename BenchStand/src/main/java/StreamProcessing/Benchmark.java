package StreamProcessing;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Benchmark {
    private static String FILE = "TopNWordCount/test-data/war_and_peace.txt";
    private static int NUM_WORDS = 586222;
    private static final String hostname = "127.0.0.1";
    private static final int frontPort = 9998;
    private static final int rearPort = 9999;

    public static void main(String[] args) throws InterruptedException {
        final GraphDeployer deployer = new GraphDeployer() {
            @Override
            public void deploy() {
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);

                env.addSource(new KryoSocketSource(hostname, frontPort))
                   .setParallelism(1)
                   .keyBy("word")
                   .sum("count")
                   .map(new ZipfDistributionValidator(1000, 0.05, 1.16, 3, 100))
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

        try (BenchStand benchStand = new BenchStand(FILE, NUM_WORDS, frontPort, rearPort, deployer)) {
            benchStand.run();
        }
        System.exit(0);
    }
}
