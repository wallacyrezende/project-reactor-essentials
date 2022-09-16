package reactive.test;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@DisplayName("Operators Tests")
public class OperatorsTest {

    @BeforeAll
    static void setUp() {
        BlockHound.install(builder ->
            builder.allowBlockingCallsInside("org.slf4j.impl.JDK14LoggerAdapter", "log"));
    }

    @Test
    @DisplayName("Subscribe on simple")
    void subscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
            .map(i -> {
                log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            })
            .subscribeOn(Schedulers.single())
            .map(i -> {
                log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            });

        StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4, 5)
            .verifyComplete();
    }

    @Test
    @DisplayName("Publish on simple")
    void publishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
            .map(i -> {
                log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            })
            .publishOn(Schedulers.boundedElastic())
            .map(i -> {
                log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            });

        StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4, 5)
            .verifyComplete();
    }

    @Test
    @DisplayName("Multiple subscribe on simple")
    void multipleSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
            .subscribeOn(Schedulers.single())
            .map(i -> {
                log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            })
            .subscribeOn(Schedulers.boundedElastic())
            .map(i -> {
                log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            });

        StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4, 5)
            .verifyComplete();
    }

    @Test
    @DisplayName("Multiple publish on simple")
    void multiplePublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
            .publishOn(Schedulers.single())
            .map(i -> {
                log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            })
            .publishOn(Schedulers.boundedElastic())
            .map(i -> {
                log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            });

        StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4, 5)
            .verifyComplete();
    }

    @Test
    @DisplayName("Publish and subscribe on simple")
    void publishAndSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
            .publishOn(Schedulers.single())
            .map(i -> {
                log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            })
            .subscribeOn(Schedulers.boundedElastic())
            .map(i -> {
                log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            });

        StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4, 5)
            .verifyComplete();
    }

    @Test
    @DisplayName("Subscribe and publish on simple")
    void subscribeAndPublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
            .subscribeOn(Schedulers.single())
            .map(i -> {
                log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            })
            .publishOn(Schedulers.boundedElastic())
            .map(i -> {
                log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                return i;
            });

        StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1, 2, 3, 4, 5)
            .verifyComplete();
    }

    @Test
    @DisplayName("Subscribe on IO")
    void subscribeOnIO() {
        Mono<List<String>> content = Mono.fromCallable(() -> Files.readAllLines(Path.of("text-file")))
            .log()
            .subscribeOn(Schedulers.boundedElastic());

//        content.subscribe(s -> log.info("{}", s));

        StepVerifier.create(content)
            .expectSubscription()
            .thenConsumeWhile(c -> {
                Assertions.assertFalse(c.isEmpty());
                log.info("Size {}", c.size());
                return true;
            })
            .verifyComplete();
    }

    @Test
    @DisplayName("Used to set default value when a IO is empty (if/else)")
    void switchIfEmptyOperator() throws InterruptedException {
        Flux<Object> flux = emptyFlux()
            .switchIfEmpty(Flux.just("not empty anymore"))
            .log();

        StepVerifier.create(flux)
            .expectSubscription()
            .expectNext("not empty anymore")
            .expectComplete()
            .verify();
    }

    @Test
    @DisplayName("Using defer operator. Used to get a new value whenever there is a subscription")
    void deferOperator() throws InterruptedException {
        Mono<Long> just = Mono.just(System.currentTimeMillis());
        Mono<Long> defer = Mono.defer(() -> Mono.just(System.currentTimeMillis()));

        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);

        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);

        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);

        AtomicLong atomicLong = new AtomicLong();
        defer.subscribe(atomicLong::set);
        Assertions.assertTrue(atomicLong.get() > 0);
    }

    private Flux<Object> emptyFlux() {
        return Flux.empty();
    }

    @Test
    @DisplayName("Using concat operator. Used to merge content between streams")
    void concatOperator() {
        Flux<String> fluxA = Flux.just("a", "b");
        Flux<String> fluxB = Flux.just("c", "d");

        Flux<String> concatFlux = Flux.concat(fluxA, fluxB).log();
//        Flux<String> fluxConcatWith = fluxA.concatWith(fluxB).log();

        StepVerifier.create(concatFlux)
            .expectSubscription()
            .expectNext("a", "b", "c", "d")
            .expectComplete()
            .verify();
    }

    @Test
    @DisplayName("Testing concat operator with error")
    void concatOperatorError() {
        Flux<String> fluxA = Flux.just("a", "b")
            .map(s -> {
                if (s.equals("b"))
                    throw new IllegalArgumentException();
                return s;
            });

        Flux<String> fluxB = Flux.just("c", "d");

        Flux<String> concatFlux = Flux.concatDelayError(fluxA, fluxB).log();

        StepVerifier.create(concatFlux)
            .expectSubscription()
            .expectNext("a", "c", "d")
            .expectError()
            .verify();
    }

    @Test
    @DisplayName("Using combineLatest operator. Used to merge the last element of each stream")
    void combineLatestOperator() {
        Flux<String> fluxA = Flux.just("a", "b");
        Flux<String> fluxB = Flux.just("c", "d");

        Flux<String> combinedFlux = Flux.combineLatest(fluxA, fluxB, (s1, s2) -> s1.toUpperCase() + s2.toUpperCase())
            .log();

        StepVerifier.create(combinedFlux)
            .expectSubscription()
            .expectNext("BC", "BD")
            .expectComplete()
            .verify();
    }

    @Test
    @DisplayName("Using merge operator. This operator joins streams without waiting for the end of the publication of each of them")
    void mergeOperator() {
        Flux<String> fluxA = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> fluxB = Flux.just("c", "d").delayElements(Duration.ofMillis(300));

//        Flux<String> mergedFlux = Flux.merge(fluxA, fluxB).delayElements(Duration.ofMillis(200))
//            .log();

        Flux<String> mergedWithFlux = fluxA.mergeWith(fluxB)
            .log();

//        mergedFlux.subscribe(log::info);
//        Thread.sleep(1000);

        StepVerifier.create(mergedWithFlux)
            .expectSubscription()
            .expectNext("a", "c", "b", "d")
            .expectComplete()
            .verify();
    }

    @Test
    @DisplayName("Using sequential merge operator. This operator joins streams while maintaining sequential order of the elements")
    void mergeSequentialOperator() {
        Flux<String> fluxA = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> fluxB = Flux.just("c", "d");

        Flux<String> mergeSequential = Flux.mergeSequential(fluxA, fluxB, fluxA)
            .delayElements(Duration.ofMillis(200))
            .log();

        StepVerifier.create(mergeSequential)
            .expectSubscription()
            .expectNext("a", "b", "c", "d", "a", "b")
            .expectComplete()
            .verify();
    }

    @Test
    @DisplayName("Testing sequential merge operator with error")
    void mergeDelayErrorOperator() {
        Flux<String> fluxA = Flux.just("a", "b")
            .map(s -> {
                if (s.equals("b"))
                    throw new IllegalArgumentException("We could do something with this");
                return s;
            })
            .doOnError(e -> log.error("Error: {}", e.getMessage()));

        Flux<String> fluxB = Flux.just("c", "d");

        Flux<String> mergeSequential = Flux.mergeDelayError(1, fluxA, fluxB, fluxA)
            .log();

        StepVerifier.create(mergeSequential)
            .expectSubscription()
            .expectNext("a", "c", "d", "a")
            .expectError()
            .verify();
    }

    @Test
    @DisplayName("Using flatMap operator. This operator flattens the structure of the elements, not ensuring the order of them")
    void flatMapOperator() {
        Flux<String> fluxA = Flux.just("a", "b");

        Flux<String> flatFlux = fluxA.map(String::toUpperCase)
            .flatMap(this::findByName)
            .log();

        StepVerifier.create(flatFlux)
            .expectSubscription()
            .expectNext("nameB1", "nameB2", "nameA1", "nameA2")
            .verifyComplete();
    }

    @Test
    @DisplayName("Using flatMapSequential operator. This operator flattens the structure of the data and ensures the order of them")
    void flatMapSequentialOperator() {
        Flux<String> fluxA = Flux.just("a", "b");

        Flux<String> flatSequentialFlux = fluxA.map(String::toUpperCase)
            .flatMapSequential(this::findByName)
            .log();

        StepVerifier.create(flatSequentialFlux)
            .expectSubscription()
            .expectNext("nameA1", "nameA2", "nameB1", "nameB2")
            .verifyComplete();
    }

    private Flux<String> findByName(String name) {
        return name.equals("A") ? Flux.just("nameA1", "nameA2").delayElements(Duration.ofMillis(100)) : Flux.just("nameB1", "nameB2");
    }

    @Test
    @DisplayName("Using zip operator.")
    void zipOperator() {
        Flux<String> titles = Flux.just("Boruto", "Naruto", "Shippuden");
        Flux<String> studios = Flux.just("Batata Studios", "Utchiha Entertainment", "Madara Studios");
        Flux<Integer> episodes = Flux.just(180, 220, 460);

//        Flux<Tuple3<String, String, Integer>> zipTupleEx = Flux.zip(titles, studios, episodes);
        Flux<Anime> animeFlux = Flux.zip(titles, studios, episodes)
            .flatMap(tuple3 -> Flux.just(new Anime(tuple3.getT1(), tuple3.getT2(), tuple3.getT3())))
            .log();

        StepVerifier.create(animeFlux)
            .expectSubscription()
            .expectNext(
                new Anime("Boruto", "Batata Studios", 180),
                new Anime("Naruto", "Utchiha Entertainment", 220),
                new Anime("Shippuden", "Madara Studios", 460))
            .verifyComplete();
    }

    @Test
    @DisplayName("Using zipWith operator.")
    void zipWithOperator() {
        Flux<String> titles = Flux.just("Boruto", "Naruto", "Shippuden");
        Flux<String> studios = Flux.just("Batata Studios", "Utchiha Entertainment", "Madara Studios");
        Flux<Integer> episodes = Flux.just(180, 220, 460);

        Flux<Anime> animeFlux = titles.zipWith(episodes)
            .flatMap(tuple3 -> Flux.just(new Anime(tuple3.getT1(), tuple3.getT2())))
            .log();

        StepVerifier.create(animeFlux)
            .expectSubscription()
            .expectNext(
                new Anime("Boruto", 180),
                new Anime("Naruto", 220),
                new Anime("Shippuden", 460))
            .verifyComplete();
    }

    @AllArgsConstructor
    @Getter
    @ToString
    @EqualsAndHashCode
    class Anime {
        private String title;
        private String studio;
        private int episodes;

        Anime(String title, int episodes) {
            this.title = title;
            this.episodes = episodes;
        }
    }
}
