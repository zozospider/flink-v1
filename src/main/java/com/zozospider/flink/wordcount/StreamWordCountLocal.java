package com.zozospider.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// 流处理 WordCount - 本地测试版本
public class StreamWordCountLocal {

    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从 socket 文本流读取数据
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");
        DataStreamSource<String> dataStream = streamEnv.socketTextStream(hostname, port);

        // 对 DataStream 数据流进行处理, 按空格分词展开, 并转换成 (word, 1) 的二元组形式
        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream2 = dataStream.flatMap(new MyFlatMapFunction());

        // 通过第 1 个位置的 word 分组
        // KeyedStream<Tuple2<String, Integer>, Tuple> dataStream3 = dataStream2.keyBy(0);
        KeyedStream<Tuple2<String, Integer>, String> dataStream3 = dataStream2.keyBy(tuple2 -> tuple2.f0);

        // 对第 2 个位置上的数据求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream4 = dataStream3.sum(1);

        dataStream4.print();

        // 执行任务
        streamEnv.execute();
    }

    // 对 DataStream 数据流进行处理, 按空格分词展开, 并转换成 (word, 1) 的二元组形式
    static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 按空格分词
            String[] ss = value.split(" ");
            // 遍历所有 word, 输出二元组
            for (String s : ss) {
                out.collect(new Tuple2<>(s, 1));
            }
        }
    }

}
