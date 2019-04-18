package StreamProcessing;

import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.typesafe.config.Config;
import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class BenchStand implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(BenchStand.class);
    private static final int PRODUCER_BUFFER_SIZE = 4_194_304; // bytes
    private static final int CONSUMER_BUFFER_SIZE = 2_097_152; // bytes

    private final String file;
    private final int numWords;
    private final int frontPort;
    private final int rearPort;
    private final int logEach;
    private final int sleepFor;
    private final int dropFirstNWords;
    private final int parallelism;

    private final Runnable deployer;
    private final AwaitCountConsumer awaitConsumer;
    private final Server producer;
    private final Server consumer;

    // TODO: Needs synchronized access?
    private final long[] latencies;

    public BenchStand(Config benchConfig, Runnable deployer) throws IOException {
        this.file = benchConfig.getString("benchstand.file_path");
        this.numWords = benchConfig.getInt("benchstand.word_count");
        this.frontPort = benchConfig.getInt("job.source_port");
        this.rearPort = benchConfig.getInt("job.sink_port");
        this.logEach = benchConfig.getInt("job.log_each");
        this.sleepFor = benchConfig.getInt("benchstand.sleep_for");
        this.dropFirstNWords = benchConfig.getInt("benchstand.drop_first_n_words");
        this.parallelism = benchConfig.getInt("job.parallelism");
        this.deployer = deployer;
        this.awaitConsumer = new AwaitCountConsumer(numWords);
        this.latencies = new long[numWords];
        this.producer = producer();
        this.consumer = consumer();
    }

    public void run() throws InterruptedException {
        deployer.run();
        awaitConsumer.await(10, TimeUnit.MINUTES);
        producer.close();
        consumer.close();
    }

    private Server producer() throws IOException {
        final Server producer = new Server(PRODUCER_BUFFER_SIZE, 1_048_576);
        final Connection[] connections = new Connection[parallelism];

        producer.getKryo()
                .register(WordWithID.class);
        producer.addListener(new Listener() {
            private int numConnected = 0;

            @Override
            public void connected(Connection connection) {
                connection.setName("Bench to source");
                synchronized (connections) {
                    if (numConnected < parallelism) {
                        LOG.info("Source connected: ({}), {}", connection,
                                 connection.getRemoteAddressTCP());
                        connections[numConnected] = connection;
                        numConnected++;
                        if (numConnected == parallelism) {
                            connections.notify();
                        }
                    } else {
                        LOG.info("Closing connection {}", connection);
                        connection.close();
                    }
                }
            }
        });

        Thread sender = new Thread(() -> {
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
                            // DO NOT CHANGE THE LINE ORDER.
                            // WRITING TO latencies SHOULD COME FIRST IN THE BLOCK
                            latencies[i] = System.nanoTime();
                            connections[i % parallelism].sendTCP(new WordWithID(i, word));
                            i++;
                            LockSupport.parkNanos(sleepFor);
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        try {
            producer.start();
            producer.bind(frontPort);
            sender.start();
        } catch (IOException e) {
            producer.stop();
            throw e;
        }
        return producer;
    }

    private Server consumer() throws IOException {
        final Server consumer = new Server(1_048_576, CONSUMER_BUFFER_SIZE);

        consumer.addListener(new Listener() {
            @Override
            public void connected(Connection connection) {
                connection.setName("Bench to sink");
                LOG.info("Sink connected ({}), {}", connection, connection.getRemoteAddressTCP());
            }

            @Override
            public void received(Connection connection, Object object) {
                final int wordId;
                if (object instanceof Integer) {
                    wordId = (int) object;
                } else {
                    LOG.warn("Unknown object type {}", object);
                    return;
                }

                latencies[wordId] = System.nanoTime() - latencies[wordId];
                awaitConsumer.accept(wordId);
                if (awaitConsumer.got() % logEach == 0) {
                    LOG.info("Progress: {}/{}", awaitConsumer.got(), numWords);
                }
            }
        });

        try {
            consumer.start();
            consumer.bind(rearPort);
        } catch (IOException e) {
            consumer.stop();
            throw e;
        }
        return consumer;
    }

    @Override
    public void close() throws IOException {
        producer.stop();
        consumer.stop();

        final long[] jitLatencies = Arrays.copyOfRange(latencies, dropFirstNWords, latencies.length);
        final String log = System.getProperty("user.home") + "/latencies.txt";
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(log))) {
            for (long latency : jitLatencies) {
                bw.write(String.valueOf(latency));
                bw.newLine();
            }
        }
        LOG.info("Latencies saved");

        Arrays.sort(jitLatencies);
        final double[] levels = {0.5, 0.75, 0.9, 0.99};
        final long[] quantiles = quantiles(jitLatencies, levels);
        for (int i = 0; i < levels.length; i++) {
            LOG.info("Level {} percentile: {} ms", (int) (levels[i] * 100),
                     Precision.round(quantiles[i] / 1000000.0, 4));
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
