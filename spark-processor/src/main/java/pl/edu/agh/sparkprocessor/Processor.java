package pl.edu.agh.sparkprocessor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.List;
import java.util.Map;

public class Processor {
    private static final String LOG_LEVEL = "WARN";
    private static final Duration BATCH_DURATION = Durations.milliseconds(1);

    private final String broker = getEnvOrExitWithError("BROKER_HOST_PORT");
    private final String sinkTopic = getEnvOrExitWithError("SINK_TOPIC");
    private final String algorithmName = getEnvOrExitWithError("ALGORITHM");
    private final String inputTopic = getEnvOrExitWithError("INPUT_TOPIC");

    private final Algorithm algorithm = chooseAlgorithm(
            algorithmName,
            ImmutableList.of(new WordCount(), new LinearRegression())
    );

    private final JavaStreamingContext javaStreamingContext = setupSpark();
    private final JavaInputDStream<ConsumerRecord<String, String>> inputStream =
            createInputKafkaMessageStream(javaStreamingContext, broker, inputTopic);

    public static void main(String[] args) throws InterruptedException {
        final Processor processor = new Processor();

        final Broadcast<KafkaSink> kafkaSink = processor
                .javaStreamingContext
                .sparkContext()
                .broadcast(new KafkaSink(getOutputKafkaParams(processor.broker)));

        processor.algorithm.run(
                processor.broker,
                processor.sinkTopic,
                processor.inputStream,
                kafkaSink
        );

        processor.javaStreamingContext.start();
        processor.javaStreamingContext.awaitTermination();
    }

    private static String getEnvOrExitWithError(String name) {
        final String env = System.getenv(name);
        if (env != null)
            return env;

        System.err.println("\"" + name + "\" env must be provided");
        System.exit(1);
        return null;
    }

    private static JavaStreamingContext setupSpark() {
        final SparkConf sparkConf = new SparkConf()
                .setAppName("spark-processor")
                .setMaster("local[2]");

        final JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, BATCH_DURATION);
        final JavaSparkContext sc = javaStreamingContext.sparkContext();
        sc.setLogLevel(LOG_LEVEL);

        return javaStreamingContext;
    }

    private static JavaInputDStream<ConsumerRecord<String, String>> createInputKafkaMessageStream(
            JavaStreamingContext javaStreamingContext,
            String broker,
            String inputTopic
    ) {

        final Map<String, Object> kafkaParams = ImmutableMap.<String, Object>builder()
                .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
                .put(ConsumerConfig.GROUP_ID_CONFIG, "blabla")
                .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
                .put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                .build();

        return KafkaUtils.createDirectStream(
                javaStreamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(ImmutableList.of(inputTopic), kafkaParams)
        );
    }

    private static Algorithm chooseAlgorithm(String algorithmName, List<Algorithm> availableAlgorithms) {
        for (Algorithm algorithm : availableAlgorithms) {
            if (algorithm.checkName(algorithmName))
                return algorithm;
        }
        System.err.println("\"" + algorithmName + "\" not implemented");
        System.exit(1);
        return null;
    }

    private static Map<String, Object> getOutputKafkaParams(String broker) {
        return ImmutableMap.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
    }
}
