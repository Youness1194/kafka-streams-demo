package hattabi.youness.kafka_demo.handlers;

import hattabi.youness.kafka_demo.events.PageEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Supplier;

@Component
public class PageEventHandler {

    private final Random random = new Random();

    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return event -> {
            System.out.println("*********************");
            System.out.println(event);
            System.out.println("*********************");
        };
    }

    @Bean
    public Supplier<PageEvent> pageEventSupplier() {
        return () -> new PageEvent(
                random.nextBoolean() ? "P1" : "P2",
                random.nextBoolean() ? "U1" : "U2",
                new Date(),
                random.nextInt(10000) + 10);
    }
}