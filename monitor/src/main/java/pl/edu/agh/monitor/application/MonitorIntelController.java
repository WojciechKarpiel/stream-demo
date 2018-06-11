package pl.edu.agh.monitor.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

@Component
public class MonitorIntelController implements WebSocketHandler {

    private final ObjectMapper objectMapper;

    private final ResultIntelMessageBridge bridge;

    public MonitorIntelController(ResultIntelMessageBridge bridge) {
        this.bridge = bridge;
        this.objectMapper = new ObjectMapper();
    }


    @Override
    public Mono<Void> handle(WebSocketSession session) {
        System.out.println("handle");
        return session.send(
                bridge.streamMessages()
                        .map(comment -> {
                            try {
                                return objectMapper.writeValueAsString(comment);
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException();
                            }
                        })
                        .map(session::textMessage)
        );
    }
}
