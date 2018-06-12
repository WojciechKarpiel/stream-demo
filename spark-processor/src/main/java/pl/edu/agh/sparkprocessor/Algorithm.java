package pl.edu.agh.sparkprocessor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaInputDStream;

public interface Algorithm {
    boolean checkName(String name);

    void run(String broker,
             String resultTopic,
             JavaInputDStream<ConsumerRecord<String, String>> messages,
             Broadcast<KafkaSink> kafkaSink
    );
}
