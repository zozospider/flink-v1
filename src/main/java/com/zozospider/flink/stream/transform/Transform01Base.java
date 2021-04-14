package com.zozospider.flink.stream.transform;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// 转换算子 - 基本 - map() flatMap() filter()
public class Transform01Base {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        // 从文件读取数据
        DataStreamSource<String> dataStreamSource = streamEnv.readTextFile("data-dir/word-count");

        // 1. map()
        // 把 string 转换成长度输出
        /*dataStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) {
                return value.length();
            }
        });*/
        // dataStream.map((MapFunction<String, Integer>) value -> value.length());
        // dataStream.map(value -> value.length());
        // dataStream.map((MapFunction<String, Integer>) String::length);
        SingleOutputStreamOperator<Integer> dataStream2 = dataStreamSource.map(String::length);
        dataStream2.print("dataStream2");

        // 2. flatMap()
        /*SingleOutputStreamOperator<String> dataStream3 = dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });*/
        // 需要加 returns() 方法, 否则会报错
        // 参考: https://ci.apache.org/projects/flink/flink-docs-stable/dev/java_lambdas.html
        /*SingleOutputStreamOperator<String> dataStream3 = dataStream.flatMap((FlatMapFunction<String, String>) (value, out) -> {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(word);
            }
        }).returns(Types.STRING);*/
        SingleOutputStreamOperator<String> dataStream3 = dataStreamSource.flatMap((String value, Collector<String> out) -> {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(word);
            }
        }).returns(Types.STRING);
        dataStream3.print("dataStream3");

        // 3. filter()
        /*dataStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) {
                return value.contains("Flink");
            }
        });*/
        // dataStream.filter((FilterFunction<String>) value -> value.contains("Flink"));
        SingleOutputStreamOperator<String> dataStream4 = dataStreamSource.filter(value -> value.contains("Flink"));
        dataStream4.print("dataStream4");

        streamEnv.execute("Transform");
    }

}
