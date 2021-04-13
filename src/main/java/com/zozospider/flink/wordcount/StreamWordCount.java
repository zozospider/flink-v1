package com.zozospider.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// 流处理 WordCount - 提交到生产环境版本
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // 并行度:
        //     当前环境设置并行度, 开发环境默认为可用 CPU 核数, 生产环境默认 conf/flink-conf.yaml 配置的 parallelism.default 值
        //     另外, 下面的部分 DataStream 也可以通过 setParallelism() 设置并行度
        //     另外, 还有部分 DataStream 的并行度只能固定某个值, 如 streamEnv.socketTextStream() 读取文件流的并行度一定为 1
        //     并行度配置优先级如下:
        //         1. 代码中每个任务设置的并行度, 如: dataStream3.setParallelism(2);
        //         2. 代码中运行环境设置的并行度, 如: streamEnv.setParallelism(16);
        //         3. 提交 Job 时指定的 Parallelism
        //         4. 安装包中 conf/flink-conf.yaml 配置的 parallelism.default 配置项
        // streamEnv.setParallelism(16);

        // 任务链:
        //     当前环境不要合并任务链
        //     另外, 下面的部分 DataStream 也可以通过 disableChaining() 指定某个操作不要合并任务链
        //     streamEnv.disableOperatorChaining();

        // 从文件中读取数据, 得到 DataStream 数据流
        // DataStreamSource<String> dataStream = streamEnv.readTextFile("data-dir/word-count");

        // 从 socket 文本流读取数据
        // DataStreamSource<String> dataStream = streamEnv.socketTextStream("localhost", 7777);

        // 从 socket 文本流读取数据
        // 用 parameter tool 工具从程序启动参数中提取配置项
        // 并行度:
        //     socketTextStream() 的并行度一定为 1, 因为不能多线程读取文件流
        // slot 共享组:
        //     当前 Source 为第一个操作, 共享组为 default
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");
        DataStreamSource<String> dataStream = streamEnv.socketTextStream(hostname, port);

        // 对 DataStream 数据流进行处理, 按空格分词展开, 并转换成 (word, 1) 的二元组形式
        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream2 = dataStream.flatMap(new MyFlatMapFunction())
                // slot 共享组:
                //     相同的组可以共享同一个 slot, 不同的组不能共享同一个 slot
                //     默认和前一个操作的共享组相同, 第一个操作的共享组默认为 default
                .slotSharingGroup("A");

        // 通过第 1 个位置的 word 分组
        // KeyedStream<Tuple2<String, Integer>, Tuple> dataStream3 = dataStream2.keyBy(0);
        KeyedStream<Tuple2<String, Integer>, String> dataStream3 = dataStream2.keyBy(tuple2 -> tuple2.f0);

        // 对第 2 个位置上的数据求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream4 = dataStream3.sum(1)
                // 并行度:
                //     当前算子单独设置并行度, 优先级最高
                .setParallelism(2)
                // slot 共享组:
                //     相同的组可以共享同一个 slot, 不同的组不能共享同一个 slot
                //     默认和前一个操作的共享组相同, 第一个操作的共享组默认为 default
                .slotSharingGroup("B");
        //      任务链:
        //          当前操作不要参与到任务链的合并 (包括和前后的操作合并)
        //      .disableChaining();
        //          当前操作不要和前面的操作合并任务链, 可以和后面的操作合并
        //      .startNewChain();
        //      重分区:
        //      .shuffle();
        //      .rebalance();

        // slot 共享组:
        //     默认和前一个操作的共享组相同, 共享组为 B
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
