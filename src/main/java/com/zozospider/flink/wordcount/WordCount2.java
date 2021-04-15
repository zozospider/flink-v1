package com.zozospider.flink.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

// 批处理 WordCount - 简化版
public class WordCount2 {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = env.readTextFile("data-dir/word-count");

        dataSource.flatMap((String s, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = s.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0)
                .sum(1)
                .print();
    }

}
