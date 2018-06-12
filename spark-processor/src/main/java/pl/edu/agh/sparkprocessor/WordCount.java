package pl.edu.agh.sparkprocessor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.codehaus.jackson.map.ObjectMapper;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class WordCount implements Algorithm {
    private static final String NAME = "WORD_COUNT";

    @Override
    public boolean checkName(String name) {
        return name.equals(NAME);
    }

    @Override
    public void run(String broker,
                    String resultTopic,
                    JavaInputDStream<ConsumerRecord<String, String>> messages,
                    Broadcast<KafkaSink> kafkaSink
    ) {
        final JavaPairDStream<String, Integer> wordCounts = messages
                .map(ConsumerRecord::value)
                .flatMap(x -> Arrays.asList(x.split("\\s+")).iterator())
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        wordCounts.foreachRDD(rdd -> {
            String resultMessage = new ObjectMapper().writeValueAsString(rdd.collectAsMap());
            System.out.println(resultMessage);
            kafkaSink.getValue().send(resultTopic, resultMessage);
        });
    }
}
