package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

public class WordCountJob {
    public static void main(String[] args) throws Exception {
        // 1. Set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Read from a socket text stream (simulate data)
        env.socketTextStream("host.docker.internal", 9999)
                .flatMap(new Tokenizer())
                .keyBy(tuple -> tuple.f0)
                .sum(1)
                .print();

        // 3. Trigger execution
        env.execute("WordCount");
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.toLowerCase().split("\\W+")) {
                if (!word.isEmpty()) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}
