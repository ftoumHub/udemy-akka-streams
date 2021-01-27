package libs;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.DelayOverflowStrategy;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import io.vavr.collection.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.junit.AfterClass;
import org.junit.Test;

import java.time.Duration;
import java.util.UUID;

import static io.vavr.API.List;
import static io.vavr.API.println;
import static java.lang.Thread.currentThread;
import static java.time.temporal.ChronoUnit.MILLIS;
import static libs.Await.await;
import static org.assertj.core.api.Assertions.assertThat;

public class FlowsTest {

    private static final ActorSystem system = ActorSystem.create("FlowsTest");
    private static final Materializer mat = ActorMaterializer.create(system);

    @Test
    public void groupedTimeoutBySize() {
        List<List<Integer>> res = List.ofAll(Source.from(List.range(0, 15))
                .via(Flows.groupedTimeout(5, Duration.ofMillis(10)))
                .runWith(Sink.seq(), mat)
                .toCompletableFuture().join());

        assertThat(res).isEqualTo(
                List(List.range(0, 5), List.range(5, 10), List.range(10, 15))
        );
    }

    @Test
    public void groupedTimeoutByTimeout() {
        List<List<Integer>> res = List.ofAll(
                Source.from(List(List.range(0, 5), List.range(5, 10), List.range(10, 15)))
                        .throttle(1, Duration.ofMillis(100))
                        .mapConcat(i -> i)
                        .via(Flows.groupedTimeout(10, Duration.ofMillis(10)))
                        .runWith(Sink.seq(), mat)
                        .toCompletableFuture().join()
        );

        assertThat(res).isEqualTo(
                List(List.range(0, 5), List.range(5, 10), List.range(10, 15))
        );
    }

    @Test
    public void groupedTimeoutByTimeout2() {
        List<List<Integer>> res = List.ofAll(
                Source.from(List.range(0, 50))
                        .throttle(1, Duration.ofMillis(50))
                        .via(Flows.groupedTimeout(30, Duration.ofMillis(100)))
                        .runWith(Sink.seq(), mat)
                        .toCompletableFuture().join()
        );

        assertThat(res).isEqualTo(
                List(List.range(0, 30), List.range(30, 50))
        );
    }

    @Test
    public void groupedTimeoutByTimeoutAndSize() {
        List<List<Integer>> res = List.ofAll(
                Source.single(List.range(0, 5))
                        .concat(Source.single(List.range(5, 10)).delay(Duration.ofMillis(30), DelayOverflowStrategy.backpressure()))
                        .concat(Source.single(List.range(10, 15)).delay(Duration.ofMillis(100), DelayOverflowStrategy.backpressure()))
                        .mapConcat(i -> i)
                        .via(Flows.groupedTimeout(10, Duration.ofMillis(50)))
                        .runWith(Sink.seq(), mat)
                        .toCompletableFuture().join()
        );

        assertThat(res).isEqualTo(
                List(List.range(0, 10), List.range(10, 15))
        );
    }

    @Test
    public void shardFlow() {
        Integer parallelism = 3;

        final Flow<Message, Message, NotUsed> worker = Flow.<Message>create().map(m -> {
            await(100, MILLIS);
            m.setThreadName(currentThread().getName());
            println("Msg with id " + m.getId() + " is handled on thread : " + m.getThreadName());
            return m;
        });

        final Flow<Message, Message, NotUsed> shard = Flows.shard(parallelism, Message::getId, worker);

        final java.util.List<Message> messages = Source.range(0, 9)
                .map(i -> List(
                        new Message(String.valueOf(i), UUID.randomUUID(), ""),
                        new Message(String.valueOf(i), UUID.randomUUID(), ""),
                        new Message(String.valueOf(i), UUID.randomUUID(), "")
                        )
                )
                .mapConcat(elt -> elt)
                .via(shard)
                .runWith(Sink.seq(), mat)
                .toCompletableFuture().join();

                List.ofAll(messages)
                        .groupBy(Message::getId)
                        .forEach(k -> {
                            final Message someMessage = k._2.get(0);
                            k._2.forEach(m -> assertThat(someMessage.threadName.equalsIgnoreCase(m.threadName)));
                        });
    }

    @AfterClass
    public static void afterAll() {
        TestKit.shutdownActorSystem(system);
    }

    @AllArgsConstructor
    @Getter
    @Setter
    @EqualsAndHashCode
    private class Message {
        private String id;
        private UUID value;
        private String threadName;
    }
}
