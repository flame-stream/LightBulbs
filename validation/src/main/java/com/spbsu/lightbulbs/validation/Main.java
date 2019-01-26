package com.spbsu.lightbulbs.validation;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws Exception{
        final ParameterTool params = ParameterTool.fromArgs(args);

        final String filePath;
        if (params.has("in")) {
            filePath = params.get("in");
        } else {
            filePath = "test-data/war_and_peace.txt";
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<String> text = env.readTextFile(filePath)
                                           .setParallelism(1);

        DataStream<Tuple2<String, Integer>> counts;
        counts = text.flatMap(new Splitter())
                     .keyBy(0)
                     .sum(1)
                     .map(new ZipfDistributionValidator(1000, 0.05, 1.16, 3, 100))
                     .setParallelism(1);

        final JobExecutionResult result = env.execute("Validation");
        System.out.println("Job took " + result.getNetRuntime(TimeUnit.MILLISECONDS) + " milliseconds to finish.");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) {
            for (String word : sentence.toLowerCase().split("[^а-яa-z0-9]+")) {
                if (!word.isEmpty()) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }
    }
}
