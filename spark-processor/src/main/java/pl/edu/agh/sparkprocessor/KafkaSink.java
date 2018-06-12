package pl.edu.agh.sparkprocessor;

import io.vavr.Lazy;
import io.vavr.collection.HashMap;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serializable;
import java.util.Map;


// Solution from https://allegro.tech/2015/08/spark-kafka-integration.html
class KafkaSink implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<String, Object> kafkaProperties;

    // transient, because type returned by createProducer is not serializable
    private final transient Lazy<Producer<String, String>> producer = Lazy.of(this::createProducer);


    KafkaSink(Map<String, Object> kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }


    void send(String topic, String value) {
        producer.get().send(new ProducerRecord<>(topic, value));
    }

    private Producer<String, String> createProducer() {
        final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProperties);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaProducer::close));
        return kafkaProducer;
    }
}
