package hattabi.youness.kafka_demo.controllers;

import hattabi.youness.kafka_demo.events.PageEvent;
import reactor.core.publisher.Flux;

import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.MediaType;

import java.time.Instant;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@RestController
public class PageEventController {

    private final StreamBridge streamBridge;
    private final InteractiveQueryService interactiveQueryService;
    private final Random random = new Random();

    public PageEventController(StreamBridge streamBridge, InteractiveQueryService interactiveQueryService) {
        this.streamBridge = streamBridge;
        this.interactiveQueryService = interactiveQueryService;
    }

    @GetMapping("/publish")
    public PageEvent publish(
            @RequestParam String name,
            @RequestParam String topic) {
        PageEvent event = new PageEvent(
                name,
                random.nextBoolean() ? "U1" : "U2",
                new Date(),
                random.nextInt(10000) + 10);

        streamBridge.send(topic, event);

        return event;
    }

    @GetMapping(path = "/analytics", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String, Long>> analytics() {

        return Flux.interval(Duration.ofSeconds(1))
                .map(sequence -> {

                    Map<String, Long> results = new HashMap<>();
                    Map<String, Long> latestWindows = new HashMap<>();

                    ReadOnlyWindowStore<String, Long> windowStore = interactiveQueryService.getQueryableStore(
                            "count-store",
                            QueryableStoreTypes.windowStore());

                    Instant now = Instant.now();
                    Instant from = now.minusSeconds(5);

                    try (KeyValueIterator<Windowed<String>, Long> iterator = windowStore.fetchAll(from, now)) {

                        while (iterator.hasNext()) {
                            var entry = iterator.next();

                            String page = entry.key.key();
                            long windowStart = entry.key.window().start();

                            if (!latestWindows.containsKey(page)
                                    || windowStart > latestWindows.get(page)) {

                                latestWindows.put(page, windowStart);
                                results.put(page, entry.value);
                            }
                        }
                    }

                    return results;
                });
    }
}