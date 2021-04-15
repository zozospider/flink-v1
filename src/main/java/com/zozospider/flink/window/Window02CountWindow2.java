package com.zozospider.flink.window;

import com.zozospider.flink.beans.User;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// Window - 计数窗口
// 相关内容参考: Window01TimeWindow2.java
public class Window02CountWindow2 {

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
                .countWindow(6, 2)
                .reduce((User user1, User user2) ->
                        new User(null, user1.getGroup(), user1.getAge() > user2.getAge() ? user1.getAge() : user2.getAge()))
                .map((User user) -> "Group: " + user.getGroup() + ", Max age: " + user.getAge())
                .returns(Types.STRING)
                .print("Test-A");

        // 测试 B: 增量聚合函数 - aggregate(), 求每个组用户的平均年龄
        dataStream
                .keyBy(User::getGroup)
                .countWindow(6, 2)
                .aggregate(new Window01TimeWindow2.MyAggregateFunction2())
                .print("Test-B");

        streamEnv.execute("Window");
    }

}
