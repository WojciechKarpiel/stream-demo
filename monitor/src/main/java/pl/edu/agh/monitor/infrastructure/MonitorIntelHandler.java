package pl.edu.agh.monitor.infrastructure;

import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.stereotype.Component;
import pl.edu.agh.monitor.application.ResultIntelMessageBridge;
import pl.edu.agh.monitor.domain.ResultIntel;

import java.io.IOException;

@Component
@EnableBinding(Sink.class)
public class MonitorIntelHandler {

    private final ResultIntelMessageBridge bridge;

    public MonitorIntelHandler(ResultIntelMessageBridge bridge) {
        this.bridge = bridge;
    }

    @StreamListener(Sink.INPUT)
    void onResult(ResultIntel resultIntel) throws IOException {
        System.out.println("Monitoring message received: " + resultIntel.getTimestamp());
        bridge.publish(resultIntel);
    }


}

