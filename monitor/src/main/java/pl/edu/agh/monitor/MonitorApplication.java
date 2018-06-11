package pl.edu.agh.monitor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;
import pl.edu.agh.monitor.application.MonitorIntelController;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class MonitorApplication {

    public static void main(String[] args) {
        SpringApplication.run(MonitorApplication.class, args);
    }


    @Bean
    public HandlerMapping webSocketHandlerMapping(MonitorIntelController monitorIntelController) {
        Map<String, WebSocketHandler> map = new HashMap<>();
        map.put("/result", monitorIntelController);

        SimpleUrlHandlerMapping handlerMapping = new SimpleUrlHandlerMapping();
        handlerMapping.setOrder(10);
        handlerMapping.setUrlMap(map);
        return handlerMapping;
    }

    @Bean
    WebSocketHandlerAdapter handlerAdapter() {
        return new WebSocketHandlerAdapter();
    }


}
