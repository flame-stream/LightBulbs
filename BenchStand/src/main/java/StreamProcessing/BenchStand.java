package StreamProcessing;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
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
    private static final double[] levels = {0.5, 0.75, 0.9, 0.99};

    private final String file;
    private final int numWords;
    private final int sourcePort;
    private final int sinkPort;
    private final int logEach;
    private final long delayBetweenWords;
    private final int dropFirstNWords;
    private final int parallelism;
    private long start;
    private long finish;

    private final Runnable deployer;
    private final AwaitCountConsumer awaitConsumer;
    private final Server producer;
    private final Server consumer;

    // TODO: Needs synchronized access?
    private final long[] latencies;

    public BenchStand(BenchConfig config, Runnable deployer) throws IOException {
        this.file = config.file;
        this.numWords = config.numWords;
        this.sourcePort = config.sourcePort;
        this.sinkPort = config.sinkPort;
        this.logEach = config.logEach;
        this.delayBetweenWords = config.delayBetweenWords;
        this.dropFirstNWords = config.dropFirstNWords;
        this.parallelism = config.parallelism;
        this.deployer = deployer;
        this.awaitConsumer = new AwaitCountConsumer(numWords);
        this.latencies = new long[numWords];
        this.producer = producer();
        this.consumer = consumer();
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

    public BenchResult run() throws InterruptedException {
        deployer.run();
        awaitConsumer.await(10, TimeUnit.MINUTES);

        finish = System.currentTimeMillis() - start;
        producer.sendToAllTCP(new Stop());
        producer.close();
        consumer.close();

        final long[] jitLatencies = Arrays.copyOfRange(latencies, dropFirstNWords,
                                                       latencies.length);
        Arrays.sort(jitLatencies);

        final long[] quantiles = quantiles(jitLatencies, levels);
        final double[] quantilesMs = new double[quantiles.length];
        for (int i = 0; i < levels.length; i++) {
            quantilesMs[i] = Precision.round(quantiles[i] / 1000000.0, 4);
            LOG.info("Level {} percentile: {} ms", (int) (levels[i] * 100), quantilesMs[i]);
        }

        final double timeSeconds = Precision.round(finish / 1000.0, 4);
        final int throughPut = (int) ((numWords - dropFirstNWords) / timeSeconds);
        LOG.info("Time total {} s", timeSeconds);
        LOG.info("Throughput {} words/s", throughPut);

        return new BenchResult(quantilesMs[0], quantilesMs[1], quantilesMs[2], quantilesMs[3],
                               timeSeconds, throughPut);
    }

    private Server producer() throws IOException {
        final Server producer = new Server(PRODUCER_BUFFER_SIZE, 1_048_576);
        final Connection[] connections = new Connection[parallelism];

        Kryo kryo = producer.getKryo();
        kryo.register(WordWithID.class);
        kryo.register(Stop.class);

        producer.addListener(new Listener() {
            private int numConnected = 0;

            @Override
            public void connected(Connection connection) {
                connection.setName("Bench to source");
                connection.setTimeout(20000);
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
                        if (i == dropFirstNWords) {
                            start = System.currentTimeMillis();
                        }
                        synchronized (connections) {
                            // DO NOT CHANGE THE LINE ORDER.
                            // WRITING TO latencies SHOULD COME FIRST IN THE BLOCK
                            latencies[i] = System.nanoTime();
                            connections[i % parallelism].sendTCP(new WordWithID(i, word));
                            i++;
                            LockSupport.parkNanos(delayBetweenWords);
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        try {
            producer.start();
            producer.bind(sourcePort);
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
                connection.setTimeout(20000);
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
            consumer.bind(sinkPort);
        } catch (IOException e) {
            consumer.stop();
            throw e;
        }
        return consumer;
    }

    @Override
    public void close() {
        producer.stop();
        consumer.stop();
    }
}
