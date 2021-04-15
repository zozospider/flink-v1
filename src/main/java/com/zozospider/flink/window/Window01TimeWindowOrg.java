package com.zozospider.flink.window;

import com.zozospider.flink.beans.Sensor;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
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
public class Window01TimeWindowOrg {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        DataStreamSource<String> dataStreamSource = streamEnv.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<Sensor> dataStream = dataStreamSource.map((String s) -> {
            String[] fields = s.split(" ");
            return new Sensor(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

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
        //   WindowFunction
        //   ProcessWindowFunction

        // 测试 A: 增量聚合函数 - reduce()
        dataStream
                .keyBy(Sensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                // 求最大温度的 Sensor
                .reduce((Sensor sensor1, Sensor sensor2) ->
                        sensor2.getTemp() > sensor1.getTemp() ? sensor2 : sensor1)
                .print("A");

        // 测试 B: 增量聚合函数 - aggregate()
        dataStream
                .keyBy(Sensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                // 求个数
                .aggregate(new AggregateFunction<Sensor, Integer, Integer>() {
                    // 初始值
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(Sensor value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return null;
                    }
                })
                .print("B");

        // 测试 C: 全窗口函数 - apply(WindowFunction)
        dataStream
                .keyBy(Sensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<Sensor, Tuple3<String, Long, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String id, TimeWindow window, Iterable<Sensor> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        Long windowEnd = window.getEnd();
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<>(id, windowEnd, count));
                    }
                })
                .print("C");

        streamEnv.execute("Window");
    }

}
