package StreamProcessing;

public class Stat {
    public final double[] stats;
    public int node;
    public int id;

    Stat(double[] stats, int node, int id) {
        this.stats = stats;
        this.node = node;
        this.id = id;
    }
}
