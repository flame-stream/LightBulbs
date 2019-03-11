package StreamProcessing;

import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class BenchStand implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(BenchStand.class);

    private final int frontPort;
    private final int rearPort;
    private final String file;
    private final int expectedSize;
    private final int logEach;
    private final int sleepFor;
    private final int parallelism;

    private final GraphDeployer deployer;
    private final AwaitCountConsumer awaitConsumer;
    private final Server producer;
    private final Server consumer;

    // TODO: Needs synchronized access?
    private final long[] latencies;

    public BenchStand(Config benchConfig, GraphDeployer deployer) {
        this.file = benchConfig.getString("benchstand.file_path");
        this.expectedSize = benchConfig.getInt("benchstand.word_count");
        this.frontPort = benchConfig.getInt("job.source_port");
        this.rearPort = benchConfig.getInt("job.sink_port");
        this.logEach = benchConfig.getInt("job.log_each");
        this.sleepFor = benchConfig.getInt("benchstand.sleep_for");
        this.parallelism = benchConfig.getInt("job.parallelism");
        this.deployer = deployer;
        this.awaitConsumer = new AwaitCountConsumer(expectedSize);
        this.latencies = new long[expectedSize];

        try {
            this.producer = producer();
            this.consumer = consumer();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void run() throws InterruptedException {
        deployer.deploy();
        awaitConsumer.await(10, TimeUnit.MINUTES);
        producer.close(); // TODO: Why not stop?
        consumer.close();
    }

    private Server producer() throws IOException {
        final Server producer = new Server(100000, 1000);
        producer.getKryo()
                .register(WordWithID.class);

        final Connection[] connections = new Connection[parallelism];
        new Thread(() -> {
            synchronized (connections) {
                try {
                    connections.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                int i = 0;
                for (String line; (line = br.readLine()) != null; ) {
                    for (String word : line.toLowerCase()
                                           .split("[^а-яa-z0-9]+")) {
                        synchronized (connections) {
                            // DO NOT CHANGE THE ORDER. WRITING TO latencies SHOULD COME FIRST IN THE BLOCK
                            latencies[i] = System.nanoTime();
                            connections[i % parallelism].sendTCP(new WordWithID(i, word));
                            LockSupport.parkNanos(sleepFor);
                            i++;
                        }
                    }
                }
                LOG.info("Done sending words");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();

        producer.addListener(new Listener() {
            private int numConnected = 0;

            @Override
            public void connected(Connection connection) {
                connection.setName("Server Source " + numConnected);
                synchronized (connections) {
                    LOG.info("Accepting connection: ({}), {}", connection, connection.getRemoteAddressTCP());
                    connections[numConnected] = connection;
                    numConnected++;

                    if (numConnected == parallelism) {
                        connections.notify();
                    }
                }
            }

            @Override
            public void received(Connection connection, Object o) {
                LOG.info("My debug {}, {}", connection, o);
            }

            @Override
            public void disconnected(Connection connection) {
                LOG.info("Server closed connection {}", connection);
            }
        });

        producer.start();
        producer.bind(frontPort);
        return producer;
    }

    private Server consumer() throws IOException {
        final Server consumer = new Server(100000, 1000000);

        consumer.addListener(new Listener() {
            private int numConnected = 0;

            @Override
            public void connected(Connection connection) {
                connection.setName("Sink " + numConnected);
                LOG.info("Sink connected ({}), {}", connection, connection.getRemoteAddressTCP());
            }

            @Override
            public void disconnected(Connection connection) {
                LOG.info("Sink disconnected ({})", connection);
            }

            @Override
            public void received(Connection connection, Object object) {
                final int wordId;
                if (object instanceof Integer) {
                    wordId = (int) object;
                } else {
                    LOG.warn("Unknown object type", object);
                    return;
                }

                latencies[wordId] = System.nanoTime() - latencies[wordId];
                awaitConsumer.accept(wordId);
                if (awaitConsumer.got() % logEach == 0) {
                    LOG.info("Progress: {}/{}", awaitConsumer.got(), expectedSize);
                }
            }
        });

        consumer.start();
        consumer.bind(rearPort);
        return consumer;
    }

    @Override
    public void close() {
        producer.stop();
        consumer.stop();

        final String logPath = System.getProperty("user.home") + "/latencies.txt";
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(logPath))) {
            for (long latency : latencies) {
                bw.write(String.valueOf(latency));
                bw.newLine();
            }
        } catch (IOException e) {
            LOG.error("Failed to write latencies");
            throw new RuntimeException(e);
        }
        LOG.info("Successfully wrote latencies to a file");

        Arrays.sort(latencies);
        final double[] levels = {0.5, 0.75, 0.9, 0.99};
        final long[] quantiles = quantiles(latencies, levels);
        for (int i = 0; i < levels.length; i++) {
            LOG.info("Level {} percentile: {}", (int) (levels[i] * 100), quantiles[i]);
        }

    }

    public static long[] quantiles(long[] sample, double... levels) {
        long[] quantiles = new long[levels.length];
        for (int i = 0; i < levels.length; i++) {
            int K = (int) (levels[i] * (sample.length - 1));
            if (K + 1 < levels[i] * sample.length) {
                quantiles[i] = sample[K + 1];
            } else {
                quantiles[i] = sample[K];
            }
        }
        return quantiles;
    }
}
