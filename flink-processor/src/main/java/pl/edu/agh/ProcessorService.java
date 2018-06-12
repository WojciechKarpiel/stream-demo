package pl.edu.agh;

import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Properties;

@Service
public class ProcessorService {

    private final String broker = getEnvOrExitWithError("BROKER_HOST_PORT");
    private final String sinkTopic = getEnvOrExitWithError("SINK_TOPIC");
    private final String algorithmName = getEnvOrExitWithError("ALGORITHM");
    private final String inputTopic = getEnvOrExitWithError("INPUT_TOPIC");



    @PostConstruct
    public void init() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = setInputProperties();

        final Algorithm algorithm = chooseAlgorithm(
                algorithmName,
                ImmutableList.of(new WordsCounter(), new LinearRegression())
        );

        env.addSource(
                new FlinkKafkaConsumer011<>(inputTopic, new SimpleStringSchema(), properties))
                .map(algorithm)
                .addSink(new FlinkKafkaProducer011<>(
                        broker,
                        sinkTopic,
                        new SimpleStringSchema()));

        env.execute("Flink Words Counter");
    }

    private Properties setInputProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", broker);
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", inputTopic);

        return properties;
    }

    private static String getEnvOrExitWithError(String name) {
        final String env = System.getenv(name);
        if (env != null)
            return env;

        System.err.println("\"" + name + "\" env must be provided");
        System.exit(1);
        return null;
    }

    private static Algorithm chooseAlgorithm(String algorithmName, List<Algorithm> availableAlgorithms) {
        for (Algorithm algorithm : availableAlgorithms)
            if (algorithm.checkName(algorithmName))
                return algorithm;

        System.err.println("\"" + algorithmName + "\" not implemented");
        System.exit(1);
        return null;
    }
}
