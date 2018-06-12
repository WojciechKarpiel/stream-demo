package pl.edu.agh.source;

import com.google.common.collect.ImmutableList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;


@SpringBootApplication
@EnableBinding(Source.class)
@EnableScheduling
public class SourceApplication {

    private final Source source;

    private final StreamGenerator generator = chooseGenerator(
            getEnvOrExitWithError("ALGORITHM"),
            ImmutableList.of(new WordCountGenerator(), new LinearRegressionGenerator())
    );

    @Autowired
    public SourceApplication(Source source) {
        this.source = source;
    }

    public static void main(String[] args) {
        SpringApplication.run(SourceApplication.class, args);
    }


    @Scheduled(fixedRate = 1)
    private void send() {
        source.output().send(MessageBuilder.withPayload(generator.generate()).build());
    }

    private static String getEnvOrExitWithError(String name) {
        final String env = System.getenv(name);
        if (env != null)
            return env;

        System.err.println("\"" + name + "\" env must be provided");
        System.exit(1);
        return null;
    }

    private static StreamGenerator chooseGenerator(String generatorName, List<StreamGenerator> availableGenerators) {
        for (StreamGenerator generator : availableGenerators)
            if (generator.checkName(generatorName))
                return generator;

        System.err.println("\"" + generatorName + "\" not implemented");
        System.exit(1);
        return null;
    }
}
