package pl.edu.agh.monitor.application;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

public abstract class ReactiveBridge<T> {

    private final Flux<T> flux;

    private FluxSink<T> fluxSink;


    ReactiveBridge() {
        this.flux = Flux.<T>create(
                emitter -> this.fluxSink = emitter,
                FluxSink.OverflowStrategy.IGNORE)
                .publish()
                .autoConnect();
    }

    public void publish(T message) {
        if (fluxSink != null) {
            fluxSink.next(message);
        }
    }

    Flux<T> streamMessages() {
        return flux;
    }

}
