package hattabi.youness.kafka_demo.handlers;

import hattabi.youness.kafka_demo.events.PageEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

@Component
public class PageEventHandler {

    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return event -> {
            System.out.println("*********************");
            System.out.println(event);
            System.out.println("*********************");
        };
    }
}