package com.zozospider.flink.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// 流处理 WordCount - 简化版
public class StreamWordCount2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = streamEnv.socketTextStream("localhost", 7777);

        dataStreamSource.flatMap((String s, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = s.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .sum(1)
                .print();

        streamEnv.execute();
    }

}
