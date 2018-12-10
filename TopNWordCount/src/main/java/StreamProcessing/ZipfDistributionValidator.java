package StreamProcessing;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class ZipfDistributionValidator implements SinkFunction<Tuple2<String, Integer>> {}
