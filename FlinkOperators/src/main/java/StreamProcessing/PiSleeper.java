package StreamProcessing;

import org.apache.flink.api.common.functions.MapFunction;

public class PiSleeper implements MapFunction<WordWithID, Integer> {
    private final int numDigits;

    public PiSleeper(int numDigits) {
        this.numDigits = numDigits;
    }

    @Override
    public Integer map(WordWithID word) {
        PiCalculator.calculateDigits(numDigits);
        return word.id;
    }
}
