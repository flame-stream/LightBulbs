package StreamProcessing;

import org.apache.flink.api.common.functions.MapFunction;

public class Pass implements MapFunction<WordWithID, Integer> {
    @Override
    public Integer map(WordWithID word) {
        return word.id;
    }
}
