package xc.flink.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import xc.flink.utils.SubmitUtils;

public class KafkaProducerExample {

    public static void main(String[] args) throws Exception {
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String jarFile = SubmitUtils.getJarFile("flink-0.0.1-SNAPSHOT.jar");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("192.168.3.120", 8081, jarFile);


        String brokers = "192.168.3.120:9092";
        String topic = "test-kafka";
        FlinkKafkaProducer011<String> kafkaProducer = new FlinkKafkaProducer011<String>(brokers, topic, new SimpleStringSchema());
        kafkaProducer.setWriteTimestampToKafka(true);

        env.addSource(new RichSourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                String msg = "message from kafka producer";
                sourceContext.collect(msg);
            }

            @Override
            public void cancel() {

            }
        }).addSink(kafkaProducer);


        // trigger program execution
        env.execute("kafka-producer");
    }

}
