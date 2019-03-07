package StreamProcessing;

import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class BenchStand implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(BenchStand.class);

    private final int frontPort;
    private final int rearPort;
    private final String file;
    private final int expectedSize;

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
        final Server producer = new Server(100000, 1000000);
        producer.getKryo()
                .register(WordWithID.class);

        final Connection[] connection = new Connection[1];
        new Thread(() -> {
            synchronized (connection) {
                try {
                    connection.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                int i = 0;
                for (String line; (line = br.readLine()) != null; ) {
                    for (String word : line.toLowerCase()
                                           .split("[^а-яa-z0-9]+")) {
                        synchronized (connection) {
                            connection[0].sendTCP(new WordWithID(i, word));
                            latencies[i] = System.nanoTime();
                            LockSupport.parkNanos(5000);
                            i++;
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();

        producer.addListener(new Listener() {
            @Override
            public void connected(Connection newConnection) {
                synchronized (connection) {
                    LOG.info("There is a new connection: {}", newConnection.getRemoteAddressTCP());
                    if (connection[0] == null) {
                        LOG.info("Accepting connection: {}", newConnection.getRemoteAddressTCP());
                        connection[0] = newConnection;
                        connection.notify();
                    } else {
                        LOG.info("Closing connection {}", newConnection.getRemoteAddressTCP());
                        newConnection.close();
                    }
                }
            }
        });

        producer.start();
        producer.bind(frontPort);
        return producer;
    }

    private Server consumer() throws IOException {
        final Server consumer = new Server(100000, 1000000);

        consumer.addListener(new Listener() {
            @Override
            public void connected(Connection connection) {
                LOG.info("Consumer connected {}, {}", connection, connection.getRemoteAddressTCP());
            }

            @Override
            public void disconnected(Connection connection) {
                LOG.info("Consumer disconnected {}", connection);
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
                if (awaitConsumer.got() % 100000 == 0) {
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
    }
}
