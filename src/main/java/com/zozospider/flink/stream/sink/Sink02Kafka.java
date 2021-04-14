package com.zozospider.flink.stream.sink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

// Sink - 输出到 Kafka
// 参考: https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/connectors/kafka.html
public class Sink02Kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        // TODO 未测试

        // 输出到 Kafka

        String topic = "topic01";
        DeserializationSchema<String> valueDeserializer = new SimpleStringSchema();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> dataStreamSource = streamEnv.addSource(
                new FlinkKafkaConsumer<>(topic, valueDeserializer, properties));

        // 输出到 Kafka
        dataStreamSource.addSink(
                new FlinkKafkaProducer<>(topic, new SimpleStringSchema(), properties));

        streamEnv.execute("Sink");
    }

}
