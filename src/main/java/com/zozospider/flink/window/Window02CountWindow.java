package com.zozospider.flink.window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// Window - 计数窗口
// 相关内容参考: Window01TimeWindow.java
public class Window02CountWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        DataStreamSource<String> dataStreamSource = streamEnv.socketTextStream("localhost", 7777);
        dataStreamSource.print();
        SingleOutputStreamOperator<String> dataStream = dataStreamSource.flatMap((String s, Collector<String> out) -> {
            String[] words = s.split(" ");
            for (String word : words) {
                out.collect(word);
            }
        }).returns(Types.STRING);
        // (word, 1)
        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream2 = dataStream
                .map((String s) -> new Tuple2<>(s, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        // 测试 A: 增量聚合函数 - reduce(), 求 WordCount
        // 后续结果将一直为 6, 因为每个 word 的窗口大小 6 正好就是要统计的 count
        dataStream2
                .keyBy((Tuple2<String, Integer> tuple2) -> tuple2.f0)
                .countWindow(6, 2)
                .reduce((Tuple2<String, Integer> tuple2a, Tuple2<String, Integer> tuple2b) ->
                        new Tuple2<>(tuple2a.f0, tuple2a.f1 + tuple2b.f1))
                .map((Tuple2<String, Integer> tuple2) -> "word: " + tuple2.f0 + ", count: " + tuple2.f1)
                .print("Test-A");

        // 测试 B: 增量聚合函数 - aggregate(), 求 WordCount
        // 后续结果将一直为 6, 因为每个 word 的窗口大小 6 正好就是要统计的 count
        dataStream2
                .keyBy((Tuple2<String, Integer> tuple2) -> tuple2.f0)
                .countWindow(6, 2)
                .aggregate(new Window01TimeWindow.MyAggregateFunction())
                .print("Test-B");

        streamEnv.execute("Window");
    }

}
