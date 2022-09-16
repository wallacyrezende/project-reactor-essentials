package reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

@Slf4j
@DisplayName("Flux tests")
public class FluxTest {

    @BeforeAll
    static void setUp() {
        BlockHound.install();
    }

    @Test
    @DisplayName("Flux subscriber behavior")
    void fluxSubscriber() {
        Flux<String> flux = Flux.just("Wallacy", "Theo", "Batata")
            .log();

        StepVerifier.create(flux)
            .expectNext("Wallacy", "Theo", "Batata")
            .verifyComplete();
    }

    @Test
    @DisplayName("Flux subscriber behavior with numbers")
    void fluxSubscriberNumbers() {
        Flux<Integer> flux = Flux.range(1, 5)
            .log();

        flux.subscribe(i -> log.info("Number {}", i));

        log.info("\n------------------------------------\n");
        StepVerifier.create(flux)
            .expectNext(1, 2, 3, 4, 5)
            .verifyComplete();
    }

    @Test
    @DisplayName("Flux subscriber behavior with list")
    void fluxSubscriberFromList() {
        Flux<Integer> flux = Flux.fromIterable(List.of(1, 2, 3, 4, 5))
            .log();

        flux.subscribe(i -> log.info("Number {}", i));

        log.info("\n------------------------------------\n");
        StepVerifier.create(flux)
            .expectNext(1, 2, 3, 4, 5)
            .verifyComplete();
    }

    @Test
    @DisplayName("Flux subscriber behavior with error")
    void fluxSubscriberNumbersError() {
        Flux<Integer> flux = Flux.range(1, 5)
            .log()
            .map(i -> {
                if (i == 4)
                    throw new IndexOutOfBoundsException("Index error");
                return i;
            });

        flux.subscribe(i -> log.info("Number {}", i),
            Throwable::printStackTrace,
            () -> log.info("DONE!"), subscription -> subscription.request(3));

        log.info("\n------------------------------------\n");
        StepVerifier.create(flux)
            .expectNext(1, 2, 3)
            .expectError(IndexOutOfBoundsException.class)
            .verify();

    }

    @Test
    @DisplayName("Flux subscriber behavior with ugly backpressure")
    void fluxSubscriberNumbersUglyBackpressure() {
        Flux<Integer> flux = Flux.range(1, 10)
            .log();

        flux.subscribe(new Subscriber<Integer>() {

            private int count = 0;
            private Subscription subscription;
            private final int requestCount = 2;

            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;
                subscription.request(2);
            }

            @Override
            public void onNext(Integer integer) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    subscription.request(requestCount);
                }

            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onComplete() {

            }
        });

        log.info("\n------------------------------------\n");
        StepVerifier.create(flux)
            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .verifyComplete();

    }

    @Test
    @DisplayName("Flux subscriber behavior with ugly backpressure and base subscriber")
    void fluxSubscriberNumbersNotSoUglyBackpressure() {
        Flux<Integer> flux = Flux.range(1, 10)
            .log();

        flux.subscribe(new BaseSubscriber<Integer>() {
            private int count = 0;
            private final int requestCount = 2;

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(requestCount);
            }

            @Override
            protected void hookOnNext(Integer value) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    request(requestCount);
                }
            }
        });

        log.info("\n------------------------------------\n");
        StepVerifier.create(flux)
            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .verifyComplete();

    }

    @Test
    @DisplayName("Flux subscriber behavior with numbers")
    void fluxSubscriberPrettyBackpressure() {
        Flux<Integer> flux = Flux.range(1, 10)
            .log()
            .limitRate(3);

        flux.subscribe(i -> log.info("Number {}", i));

        log.info("\n------------------------------------\n");
        StepVerifier.create(flux)
            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .verifyComplete();
    }

    @Test
    @DisplayName("Flux subscriber behavior with interval one")
    void fluxSubscriberIntervalOne() throws InterruptedException {
        Flux<Long> interval = Flux.interval(Duration.ofMillis(100))
            .take(10)
            .log();

        interval.subscribe(i -> log.info("Number {}", i));

        Thread.sleep(3000);
    }

    @Test
    @DisplayName("Flux subscriber behavior with interval two")
    void fluxSubscriberIntervalTwo() {
        StepVerifier.withVirtualTime(this::createInterval)
            .expectSubscription()
            .expectNoEvent(Duration.ofDays(1))
            .thenAwait(Duration.ofDays(1))
            .expectNext(0L)
            .thenAwait(Duration.ofDays(1))
            .expectNext(1L)
            .thenCancel()
            .verify();

    }

    private Flux<Long> createInterval() {
        return Flux.interval(Duration.ofDays(1))
            .log();
    }


    @Test
    @DisplayName("Flux HOT publisher with connectableFlux")
    void connectableFlux() throws InterruptedException {
        ConnectableFlux<Integer> connectableFlux = Flux.range(1, 10)
            .log()
            .delayElements(Duration.ofMillis(100))
            .publish();

//        connectableFlux.connect();
//
//        log.info("Thread sleeping for 300ms");
//        Thread.sleep(300);
//
//        connectableFlux.subscribe(i -> log.info("Sub1 number {}", i));
//
//        log.info("Thread sleeping for 200ms");
//        Thread.sleep(200);
//
//        connectableFlux.subscribe(i -> log.info("Sub1 number {}", i));

        StepVerifier.create(connectableFlux)
            .then(connectableFlux::connect)
            .thenConsumeWhile(i -> i <= 5)
            .expectNext(6, 7, 8, 9, 10)
            .expectComplete()
            .verify();
    }

    @Test
    @DisplayName("Flux HOT publisher with connectableFlux using auto connect")
    void connectableFluxAutoConnect() throws InterruptedException {
        Flux<Integer> flexAutoConnect = Flux.range(1, 5)
            .log()
            .delayElements(Duration.ofMillis(100))
            .publish()
            .autoConnect(2);

        StepVerifier.create(flexAutoConnect)
            .then(flexAutoConnect::subscribe)
            .expectNext(1, 2, 3, 4, 5)
            .expectComplete()
            .verify();
    }
}