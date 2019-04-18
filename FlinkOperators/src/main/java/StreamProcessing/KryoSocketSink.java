package StreamProcessing;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KryoSocketSink extends RichSinkFunction<Integer> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(KryoSocketSink.class);
    private static final int OUTPUT_BUFFER_SIZE = 2_097_152; // bytes
    private static final int CONNECTION_AWAIT_TIMEOUT = 5000; // milliseconds

    private final String hostname;
    private final int port;
    private transient Client client = null;

    public KryoSocketSink(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        client = new Client(OUTPUT_BUFFER_SIZE, 1_048_576);
        client.addListener(new Listener() {
            @Override
            public void connected(Connection connection) {
                connection.setName("Sink to bench");
            }

            @Override
            public void disconnected(Connection connection) {
                LOG.warn("Sink disconnected");
            }
        });

        LOG.info("Connecting to {}:{}", hostname, port);
        client.start();
        client.connect(CONNECTION_AWAIT_TIMEOUT, hostname, port);
        LOG.info("CONNECTED");
    }

    @Override
    public void invoke(Integer value, Context context) {
        if (client != null && client.isConnected()) { // TODO: How client can be null?
            client.sendTCP(value);
        } else {
            throw new RuntimeException("Writing to the closed log");
        }
    }

    @Override
    public void close() {
        if (client != null) {
            LOG.info("Closing sink connection");
            client.stop();
        }
    }
}
