package StreamProcessing;

public interface GraphDeployer extends AutoCloseable {
    void deploy();

    @Override
    void close();
}
