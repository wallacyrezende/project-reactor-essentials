package reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

/*
 * Reactive Streams
 * 1. Asynchronous
 * 2. Non-blocking
 * 3. Backpressure
 * Publisher (cold) <- (subscribe) Subscriber
 * Subscription is created
 * Publisher (onSubscribe with the subscription) -> Subscriber
 * Subscription <- (request N) Subscriber
 * Publisher -> (onNext) Subscriber
 * until:
 * 1. Publisher sends all the object requested.
 * 2. Publisher sends all the object it has. (onComplete) subscriber and subscription will be canceled
 * 3. There is an error. (onError) -> subscriber and subscription will be canceled
 */

@Slf4j
@DisplayName("Mono tests")
public class MonoTest {

    @BeforeAll
    static void setUp() {
        BlockHound.install();
    }

    @Test
    @DisplayName("MUST throw exception when block thread")
    void blockHoundWorks() {
        try {
            FutureTask<?> task  = new FutureTask<>( () -> {
                Thread.sleep(0);
                return "";
            });
            Schedulers.parallel().schedule(task);

            task.get(10, TimeUnit.SECONDS);
            Assertions.fail("should fail");
        }
        catch (Exception e) {
            Assertions.assertTrue(e.getCause() instanceof BlockingOperationError);
        }
    }

    @Test
    @DisplayName("MUST log the calls of subscriber")
    void monoSubscriber() {
        String name = "Wallacy Rezende";
        Mono<String> mono = Mono.just(name)
            .log();

        mono.subscribe();

        log.info("\n------------------------------------\n");
        StepVerifier.create(mono)
            .expectNext(name)
            .verifyComplete();
    }

    @Test
    @DisplayName("MUST log the calls of subscriber consumer")
    void monoSubscriberConsumer() {
        String name = "Wallacy Rezende";
        Mono<String> mono = Mono.just(name)
            .log();

        mono.subscribe(s -> log.info("Value {}", s));

        log.info("\n------------------------------------\n");
        StepVerifier.create(mono)
            .expectNext(name)
            .verifyComplete();
    }

    @Test
    @DisplayName("MUST log the calls of subscriber consumer error")
    void monoSubscriberConsumerError() {
        String name = "Wallacy Rezende";
        Mono<String> mono = Mono.just(name)
            .map(s -> {throw new RuntimeException("Testing mono with error");});

        mono.subscribe( s -> log.info("Name {}", s), s -> log.error("Something bad happened"));
        mono.subscribe( s -> log.info("Name {}", s), Throwable::printStackTrace);

        log.info("\n------------------------------------\n");

        StepVerifier.create(mono)
            .expectError(RuntimeException.class)
            .verify();
    }

    @Test
    @DisplayName("MUST log the calls of subscriber consumer complete")
    void monoSubscriberConsumerComplete() {
        String name = "Wallacy Rezende";
        Mono<String> mono = Mono.just(name)
            .log()
            .map(String::toUpperCase);

        mono.subscribe(s -> log.info("Value {}", s), Throwable::printStackTrace, () -> log.info("FINISHED!"));

        log.info("\n------------------------------------\n");
        StepVerifier.create(mono)
            .expectNext(name.toUpperCase())
            .verifyComplete();
    }

    @Test
    @DisplayName("MUST log the calls of subscriber consumer complete subscription")
    void monoSubscriberConsumerCompleteSubscription() {
        String name = "Wallacy Rezende";
        Mono<String> mono = Mono.just(name)
            .log()
            .map(String::toUpperCase);

        mono.subscribe(s -> log.info("Value {}", s),
            Throwable::printStackTrace,
            () -> log.info("FINISHED!")
        , subscription -> subscription.request(5));
//        , Subscription::cancel);

        log.info("\n------------------------------------\n");
        StepVerifier.create(mono)
            .expectNext(name.toUpperCase())
            .verifyComplete();
    }

    @Test
    @DisplayName("MUST log publisher behaviors")
    void monoDoOnMethods() {
        String name = "Wallacy Rezende";
//        Mono<String> mono = Mono.just(name)
        Mono<Object> mono = Mono.just(name)
            .log()
            .map(String::toUpperCase)
            .doOnSubscribe(subscription -> log.info("Subscribed"))
            .doOnRequest(longNumber -> log.info("Request received, starting doing something..."))
            .doOnNext(s -> log.info("Value is here. Executing first doOnNext {}", s))
            .flatMap(s -> Mono.empty())
            .doOnNext(s -> log.info("Value is here. Executing second doOnNext {}", s)) // will not be executed
            .doOnSuccess(s -> log.info("doOnSuccess executed")); // value is null

        mono.subscribe(s -> log.info("Value {}", s), Throwable::printStackTrace, () -> log.info("FINISHED!"));

        log.info("\n------------------------------------\n");
//        StepVerifier.create(mono)
//            .expectNext(name.toUpperCase())
//            .verifyComplete();
    }

    @Test
    @DisplayName("MUST log publisher errors")
    void monoDoOnError() {
        Mono<Object> error = Mono.error(new IllegalArgumentException("Illegal argument exception"))
            .doOnError(e -> MonoTest.log.error("Error message: {}", e.getMessage()))
            .doOnNext(s -> log.info("Executing this doOnNext")) // will not be executed
            .log();

        StepVerifier.create(error)
            .expectError(IllegalArgumentException.class)
            .verify();
    }

    @Test
    @DisplayName("MUST log publisher errors on resume")
    void monoDoOnErrorResume() {
        String name = "Wallacy Rezende";

        Mono<Object> error = Mono.error(new IllegalArgumentException("Illegal argument exception"))
            .doOnError(e -> MonoTest.log.error("Error message: {}", e.getMessage()))
            .onErrorResume(s -> {
                log.info("Inside on error resume");
                return Mono.just(name);
            })
            .doOnError(e -> MonoTest.log.error("Error message: {}", e.getMessage())) // will not be executed
            .log();

        StepVerifier.create(error)
            .expectNext(name)
            .verifyComplete();
    }

    @Test
    @DisplayName("MUST log publisher errors on return")
    void monoDoOnErrorReturn() {
        String name = "Wallacy Rezende";

        Mono<Object> error = Mono.error(new IllegalArgumentException("Illegal argument exception"))
            .onErrorReturn("Empty fallback")
            .onErrorResume(s -> {
                log.info("Inside on error resume");
                return Mono.just(name);
            }) // will not be executed
            .doOnError(e -> MonoTest.log.error("Error message: {}", e.getMessage())) // will not be executed
            .log();

        StepVerifier.create(error)
            .expectNext("Empty fallback")
            .verifyComplete();
    }
}
