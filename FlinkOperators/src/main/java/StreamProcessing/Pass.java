package StreamProcessing;

import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.concurrent.locks.LockSupport;

public class Pass implements MapFunction<WordWithID, WordWithID> {
    private final ExponentialDistribution exp;

    public Pass(int mean) {
        this.exp = new ExponentialDistribution(mean);
    }

    @Override
    public WordWithID map(WordWithID word) {
        System.out.println(String.format("Id: %d", this.hashCode()));
        LockSupport.parkNanos((long) exp.sample());
        return word;
    }
}
