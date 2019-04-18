package StreamProcessing;

import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.concurrent.locks.LockSupport;

public class Sleeper implements MapFunction<WordWithID, Integer> {
    private final ExponentialDistribution exp;

    public Sleeper(int mean) {
        this.exp = new ExponentialDistribution(mean);
    }

    @Override
    public Integer map(WordWithID word) {
        LockSupport.parkNanos((long) exp.sample());
        return word.id;
    }
}
