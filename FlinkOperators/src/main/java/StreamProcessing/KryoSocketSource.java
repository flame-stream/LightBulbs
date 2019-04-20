package StreamProcessing;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KryoSocketSource extends RichParallelSourceFunction<WordWithID> {
    private static final long serialVersionUID = 1;
    private static final Logger LOG = LoggerFactory.getLogger(KryoSocketSource.class);
    private static final int INPUT_BUFFER_SIZE = 4_194_304; // bytes
    private static final int CONNECTION_AWAIT_TIMEOUT = 5000; // milliseconds

    private final String hostname;
    private final int port;
    private transient Client client = null;

    public KryoSocketSource(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    @Override
    public void open(Configuration parameters) {
        client = new Client(1_048_576, INPUT_BUFFER_SIZE);
        Kryo kryo = client.getKryo();
        kryo.register(WordWithID.class);
        kryo.register(Stop.class);
    }

    @Override
    public void run(SourceContext<WordWithID> ctx) throws Exception {
        client.addListener(new Listener() {
            @Override
            public void connected(Connection connection) {
                connection.setName("Source to bench");
            }

            @Override
            public void received(Connection connection, Object object) {
                if (object instanceof WordWithID) {
                    ctx.collect((WordWithID) object);
                } else if (object instanceof Stop) {
                    synchronized (hostname) {
                        hostname.notify();
                    }
                } else {
                    LOG.warn("Unknown object type {}", object);
                }
            }

            @Override
            public void disconnected(Connection connection) {
                LOG.info("Source disconnected");
            }
        });

        LOG.info("Connecting to {}:{}", hostname, port);
        client.start();
        client.connect(CONNECTION_AWAIT_TIMEOUT, hostname, port);
        LOG.info("CONNECTED");

        synchronized (hostname) {
            hostname.wait();
        }
    }

    @Override
    public void cancel() {
        synchronized (hostname) {
            hostname.notify();
        }

        final Client socket = client;
        if (socket != null) {
            socket.close(); // TODO: Why not stop?
        }
    }
}
