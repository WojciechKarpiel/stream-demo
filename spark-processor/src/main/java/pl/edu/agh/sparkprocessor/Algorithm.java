package pl.edu.agh.sparkprocessor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;

import java.util.Map;

public interface Algorithm {
    boolean checkName(String name);

    void run(String broker,
             String resultTopic,
             JavaInputDStream<ConsumerRecord<String, String>> messages,
             Map<String, Object> kafkaParams
    );
}
