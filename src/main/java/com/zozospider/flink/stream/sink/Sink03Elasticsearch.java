package com.zozospider.flink.stream.sink;

import com.zozospider.flink.beans.Sensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// Sink - 输出到 Elasticsearch
// 参考: https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/connectors/elasticsearch.html
public class Sink03Elasticsearch {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 官方文档中是这么写的, 但是找不到 ElasticsearchSink.Builder ???

        DataStreamSource<String> dataStreamSource = streamEnv.readTextFile("data-dir/sensor.txt");
        SingleOutputStreamOperator<Sensor> dataStream = dataStreamSource.map((String s) -> {
            String[] fields = s.split(" ");
            return new Sensor(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        streamEnv.execute("Sink");
    }

}
