package pl.edu.agh.sink;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import javax.annotation.PostConstruct;
import java.util.Date;

@SpringBootApplication
@EnableBinding({Source.class, Sink.class})
public class SinkApplication {

    private static final long WINDOW_LENGTH_MILLIS = 5 * 1000; // 5 seconds
    @Autowired
    private Source source;
    private long nextWindowEndTime = 0;
    private long receivedMessages = 0;

    public static void main(String[] args) {
        SpringApplication.run(SinkApplication.class, args);
    }

    @PostConstruct
    public void init() throws Exception {
        nextWindowEndTime = new Date().getTime() + WINDOW_LENGTH_MILLIS;
        receivedMessages = 0;
    }

    @StreamListener(Sink.INPUT)
    void receive(Message<String> message) throws Exception {
        System.out.println("Received " + message.getPayload());

        receivedMessages++;

        long timeStamp = new Date().getTime();
        if (timeStamp >= nextWindowEndTime) {
            source.output().send(
                    MessageBuilder
                            .withPayload(new ResultIntel(timeStamp, receivedMessages))
                            .build()
            );
            receivedMessages = 0;
            nextWindowEndTime = timeStamp + WINDOW_LENGTH_MILLIS;
        }
    }

}
