package StreamProcessing;

public class BenchConfig {
    public final String file;
    public final int numWords;
    public final int dropFirstNWords;

    public final String managerHostname;
    public final int managerPort;

    public final int computationDelay;
    public final int validationDelay;
    public final int logEach;
    public final String benchHost;
    public final int sourcePort;
    public final int sinkPort;

    public int parallelism;
    public long delayBetweenWords;
    public boolean mergeOnSingleNode;

    public BenchConfig(String file, int numWords, int dropFirstNWords, String managerHostname,
                       int managerPort, int computationDelay, int validationDelay, int logEach,
                       String benchHost, int sourcePort, int sinkPort) {
        this.file = file;
        this.numWords = numWords;
        this.dropFirstNWords = dropFirstNWords;
        this.managerHostname = managerHostname;
        this.managerPort = managerPort;
        this.computationDelay = computationDelay;
        this.validationDelay = validationDelay;
        this.logEach = logEach;
        this.benchHost = benchHost;
        this.sourcePort = sourcePort;
        this.sinkPort = sinkPort;
    }
}
