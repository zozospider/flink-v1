package com.zozospider.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

// 批处理 WordCount
public class WordCount {

    public static void main(String[] args) throws Exception {
        // 创建批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据, 得到 DataSet 数据集
        DataSource<String> dataSet = env.readTextFile("data-dir/word-count");

        // 对 DataSet 数据集进行处理, 按空格分词展开, 并转换成 (word, 1) 的二元组形式
        FlatMapOperator<String, Tuple2<String, Integer>> dataSet2 = dataSet.flatMap(new MyFlatMapFunction());

        // 通过第 1 个位置的 word 分组
        UnsortedGrouping<Tuple2<String, Integer>> grouping = dataSet2.groupBy(0);

        // 对第 2 个位置上的数据求和
        AggregateOperator<Tuple2<String, Integer>> dataSet3 = grouping.sum(1);
        dataSet3.print();
    }

    // 对 DataSet 数据集进行处理, 按空格分词展开, 并转换成 (word, 1) 的二元组形式
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
