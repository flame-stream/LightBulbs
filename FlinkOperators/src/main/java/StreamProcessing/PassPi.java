package StreamProcessing;

import org.apache.flink.api.common.functions.MapFunction;

public class PassPi<IN> implements MapFunction<IN, IN> {
    private final int numDigits;

    public PassPi(int numDigits) {
        this.numDigits = numDigits;
    }

    @Override
    public IN map(IN value) {
        PiCalculator.calculateDigits(numDigits);
        return value;
    }
}
