package com.zozospider.flink.window;

import com.zozospider.flink.beans.User;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

// Window - 时间窗口
// 相关内容参考: Window02CountWindow2.java
// 参考: https://zhuanlan.zhihu.com/p/151781508
public class Window01TimeWindow2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        DataStreamSource<String> dataStreamSource = streamEnv.socketTextStream("localhost", 7777);
        dataStreamSource.print();
        // User
        SingleOutputStreamOperator<User> dataStream = dataStreamSource.flatMap((String s, Collector<User> out) -> {
            String[] words = s.split(" ");
            out.collect(new User(words[0], words[1], Integer.valueOf(words[2])));
        }).returns(Types.POJO(User.class));

        // 测试 A: 增量聚合函数 - reduce(), 求每个组用户的最大年龄
        dataStream
                .keyBy(User::getGroup)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce((User user1, User user2) ->
                        new User(null, user1.getGroup(), user1.getAge() > user2.getAge() ? user1.getAge() : user2.getAge()))
                .map((User user) -> "Group: " + user.getGroup() + ", Max age: " + user.getAge())
                .returns(Types.STRING)
                .print("Test-A");

        // 测试 B: 增量聚合函数 - aggregate(), 求每个组用户的平均年龄
        dataStream
                .keyBy(User::getGroup)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new MyAggregateFunction2())
                .print("Test-B");

        // 测试 C: 全窗口函数 - apply(WindowFunction), 求每个组用户的平均年龄
        // Tips: 经T测试, apply() 方法不能用 lambda 表达式方式, 会报错
        dataStream
                .keyBy(User::getGroup)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new MyWindowFunction2())
                .print("Test-C");

        streamEnv.execute("Window");
    }

    // Accumulator 增量数据类型 (类似缓冲区)
    static class AvgAccumulator {
        String group;
        long ageSum;
        long count;
    }

    // IN - 输入类型: User
    // ACC - Accumulator 增量数据类型 (类似缓冲区): AvgAccumulator
    // OUT - 输出类型: String
    static class MyAggregateFunction2 implements AggregateFunction<User, AvgAccumulator, String> {

        // 初始化: 创建 Accumulator 对象
        @Override
        public AvgAccumulator createAccumulator() {
            return new AvgAccumulator();
        }

        // 增加数据: 更新 Accumulator 对象
        @Override
        public AvgAccumulator add(User user, AvgAccumulator accumulator) {
            accumulator.group = user.getGroup();
            accumulator.ageSum += user.getAge();
            accumulator.count++;
            return accumulator;
        }

        // 输出值
        @Override
        public String getResult(AvgAccumulator accumulator) {
            double ageAvg = accumulator.ageSum / (double) accumulator.count;
            return "Group: " + accumulator.group + ", Average age: " + ageAvg;
        }

        // 合并多个 Accumulator 对象
        @Override
        public AvgAccumulator merge(AvgAccumulator accumulator1, AvgAccumulator accumulator2) {
            accumulator1.ageSum += accumulator2.ageSum;
            accumulator1.count += accumulator2.count;
            return accumulator1;
        }
    }

    // KEY - key 类型 (keyBy() 的那个字段): String
    // W extends Window - Window 子类 (可以用于获取 Window 相关信息): TimeWindow
    // IN – 输入类型: User
    // OUT - 输出类型: String
    static class MyWindowFunction2 implements WindowFunction<User, String, String, TimeWindow> {
        @Override
        public void apply(String key, TimeWindow window, Iterable<User> input, Collector<String> out) {
            long ageSum = 0;
            long count = 0;
            for (User user : input) {
                ageSum += user.getAge();
                count++;
            }
            double ageAvg = ageSum / (double) count;
            out.collect("Group: " + key + ", Average age: " + ageAvg);
        }
    }

}
