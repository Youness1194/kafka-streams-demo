package hattabi.youness.kafka_demo.handlers;

import hattabi.youness.kafka_demo.events.PageEvent;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;

import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.Function;

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

    @Bean
    public Function<KStream<String, PageEvent>, KStream<String, Long>> kStreamFunction() {

        return input -> input

                .filter((key, event) -> event.duration() > 100)
                .map((key, event) -> new KeyValue<>(event.name(), 0L))
                .groupBy(
                        (key, value) -> key,
                        Grouped.with(Serdes.String(), Serdes.Long()))
                .windowedBy(
                        TimeWindows.ofSizeWithNoGrace(
                                Duration.ofSeconds(5)))
                .count(Materialized.as("count-store"))
                .toStream()

                .map((windowedKey, count) -> new KeyValue<>(
                        windowedKey.key(),
                        count));
    }
}