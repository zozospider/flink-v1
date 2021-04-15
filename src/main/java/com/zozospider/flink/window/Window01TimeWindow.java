package com.zozospider.flink.window;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

// Window - 时间窗口
// 参考: https://zhuanlan.zhihu.com/p/151781508
public class Window01TimeWindow {

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

        // 1. 分组:
        //   .keyBy(Sensor::getId)

        // 2. 开窗:
        //   a. 滚动窗口 - 事件时间
        //   .window(TumblingEventTimeWindows.of(Time.seconds(5)));
        //   a. 滚动窗口 - 处理时间
        //   .window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
        //
        //   b. 滑动窗口 - 事件时间
        //   .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)));
        //   b. 滑动窗口 - 处理时间
        //   .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)));
        //
        //   c. 会话窗口 - 事件时间
        //   .window(EventTimeSessionWindows.withGap(Time.seconds(5)));
        //   c. 会话窗口 - 处理时间
        //   .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))

        // 3. 对窗口函数进行计算操作:
        //   a. incremental aggregation functions (增量聚合函数): 每条数据到来就进行计算, 保持一个简单的状态
        //   .reduce()
        //   .aggregate()
        //   b. full window functions - (全窗口函数): 先把窗口的数据收集起来, 等到计算的时候会遍历所有数据
        //   apply(WindowFunction)
        //   process(ProcessWindowFunction)

        // 测试 A: 增量聚合函数 - reduce(), 求 WordCount
        dataStream2
                .keyBy((Tuple2<String, Integer> tuple2) -> tuple2.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce((Tuple2<String, Integer> tuple2a, Tuple2<String, Integer> tuple2b) ->
                        new Tuple2<>(tuple2a.f0, tuple2a.f1 + tuple2b.f1))
                .map((Tuple2<String, Integer> tuple2) -> "word: " + tuple2.f0 + ", count: " + tuple2.f1)
                .print("Test-A");

        // 测试 B: 增量聚合函数 - aggregate(), 求 WordCount
        dataStream2
                .keyBy((Tuple2<String, Integer> tuple2) -> tuple2.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new MyAggregateFunction())
                .print("Test-B");

        // 测试 C: 全窗口函数 - apply(WindowFunction), 求 WordCount
        // Tips: 经测试, apply() 方法不能用 lambda 表达式方式, 会报错
        dataStream2
                .keyBy((Tuple2<String, Integer> tuple2) -> tuple2.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new MyWindowFunction())
                .print("Test-C");

        streamEnv.execute("Window");
    }

    // Accumulator 增量数据类型 (类似缓冲区)
    static class WordCountAccumulator {
        String word;
        long count;
    }

    // IN: 输入类型: Tuple2<String, Integer>
    // ACC: Accumulator 增量数据类型 (类似缓冲区): WordCountAccumulator
    // OUT: 输出类型: String
    static class MyAggregateFunction implements AggregateFunction<Tuple2<String, Integer>, WordCountAccumulator, String> {

        // 初始化: 创建 Accumulator 对象
        @Override
        public WordCountAccumulator createAccumulator() {
            return new WordCountAccumulator();
        }

        // 增加数据: 更新 Accumulator 对象
        @Override
        public WordCountAccumulator add(Tuple2<String, Integer> tuple2, WordCountAccumulator accumulator) {
            accumulator.word = tuple2.f0;
            accumulator.count += tuple2.f1;
            return accumulator;
        }

        // 输出值
        @Override
        public String getResult(WordCountAccumulator accumulator) {
            return "word: " + accumulator.word + ", count: " + accumulator.count;
        }

        // 合并多个 Accumulator 对象
        @Override
        public WordCountAccumulator merge(WordCountAccumulator accumulator1, WordCountAccumulator accumulator2) {
            accumulator1.count += accumulator2.count;
            return accumulator1;
        }
    }

    // KEY - key 类型 (keyBy() 的那个字段): String
    // W extends Window - Window 子类 (可以用于获取 Window 相关信息): TimeWindow
    // IN – 输入类型: Tuple2<String, Integer>
    // OUT - 输出类型: String
    static class MyWindowFunction implements WindowFunction<Tuple2<String, Integer>, String, String, TimeWindow> {
        @Override
        public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<String> out) {
            int count = IteratorUtils.toList(input.iterator()).size();
            out.collect("word: " + key + ", count: " + count + ", window: " + window);
        }
    }

}
