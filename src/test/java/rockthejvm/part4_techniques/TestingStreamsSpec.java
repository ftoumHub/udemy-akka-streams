package rockthejvm.part4_techniques;

import static io.vavr.API.*;
import static io.vavr.Patterns.$Failure;
import static io.vavr.Patterns.$Success;
import static io.vavr.concurrent.Future.fromCompletableFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static scala.compat.java8.FutureConverters.*;

import java.util.concurrent.CompletionStage;

import akka.Done;
import akka.japi.Pair;
import akka.stream.testkit.TestPublisher;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.TestSubscriber.Probe;
import akka.stream.testkit.javadsl.TestSink;
import akka.stream.testkit.javadsl.TestSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;

public class TestingStreamsSpec {

    static ActorSystem system;
    static Materializer mat;
    static TestKit probe; // acteur de test

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create("TestingAkkaStreams");
        mat = ActorMaterializer.create(system);
    }

    @AfterClass
    public static void tearDown() {
        TestKit.shutdownActorSystem(system);
        system = null;
        mat = null;
    }

    @Test
    public void simpleStream() {
        final Source<Integer, NotUsed> simpleSource = Source.range(0, 10);

        // Ce sink va additionner les valeurs qui lui sont passé.
        // Lorsqu'il est matérialisé, il expose son resultat sous forme d'un CompletionStage de Integer
        final Sink<Integer, CompletionStage<Integer>> simpleSink = Sink.fold(0, Integer::sum);

        int result = simpleSource.log("SimpleSource")
                .toMat(simpleSink, Keep.right()).run(mat)
                .toCompletableFuture().join();
        assertEquals(55, result);
    }

    @Test
    public void integrateWithTestActorsViaMaterializedValues() {

        final Source<Integer, NotUsed> simpleSource = Source.range(0, 10);
        final Sink<Integer, CompletionStage<Integer>> simpleSink = Sink.fold(0, Integer::sum);

        probe = new TestKit(system); // Acteur spécial avec des capacités de test

        final CompletionStage<Integer> future = simpleSource.toMat(simpleSink, Keep.right()).run(mat);

        akka.pattern.Patterns.pipe(toScala(future), system.dispatcher()).to(probe.getRef());
        probe.expectMsg(55);
    }

    @Test
    public void integrateWithATestActorBasedSink() {
        probe = new TestKit(system); // Acteur spécial avec des capacités de test
        final Source<Integer, NotUsed> simpleSource = Source.range(0, 10);
        final Flow<Integer, Integer, NotUsed> flow = // 0, 1, 3, 6, 10, 15
                Flow.<Integer>create().scan(0, Integer::sum);
        final Source<Integer, NotUsed> streamUnderTest = simpleSource.via(flow);

        final Sink<Integer, NotUsed> probeSink = Sink.actorRef(probe.getRef(), "CompletionMessage");
        streamUnderTest
                .log("StreamUnderTest")
                .to(probeSink).run(mat);
        probe.expectMsgAllOf(0, 0, 1, 3, 6, 10, 15, 21, 28, 36, 45, 55);
    }

    @Test
    public void integrateWithStreamsTestKitSink() {
        Source<Integer, NotUsed> sourceUnderTest = Source.range(1, 5).map(__ -> __ * 2);

        Sink<Integer, Probe<Integer>> testSink = TestSink.probe(system);
        Probe<Integer> materializedTestValue = sourceUnderTest.runWith(testSink, mat);

        materializedTestValue
                .request(5) // si on met plus ou moins d'éléments que prévu dans la source ça pète
                .expectNext(2, 4, 6, 8, 10)
                .expectComplete();
    }

    @Test
    public void integrateWithStreamsTestKitSource() {
        Sink<Integer, CompletionStage<Done>> sinkUnderTest = Sink.foreach(i ->
                Match(i).of(
                        Case($(13), __ -> new RuntimeException("bad luck!")),
                        Case($(), __ -> __)
                )
        );

        Source<Integer, TestPublisher.Probe<Integer>> testSource = TestSource.probe(system);
        Pair<TestPublisher.Probe<Integer>, CompletionStage<Done>> materialized = testSource.toMat(sinkUnderTest, Keep.both()).run(mat);

        materialized.first()
                .sendNext(1)
                .sendNext(5)
                .sendNext(13)
                .sendComplete();

        fromCompletableFuture(materialized.second().toCompletableFuture()).onComplete(__ ->
                Match(__).of(
                        Case($Success($()), ___ -> run(() -> fail("the sink under test should have thrown an exception on 13"))),
                        Case($Failure($()), ___  -> __)
                )
        );
    }

    @Test
    public void testFlowsWithATestSourceANDaTestSink() {
        Flow<Integer, Integer, NotUsed> flowUnderTest = Flow.<Integer>create().map(__ -> __ * 2);

        Source<Integer, TestPublisher.Probe<Integer>> testSource = TestSource.probe(system);
        Sink<Integer, TestSubscriber.Probe<Integer>> testSink = TestSink.probe(system);

        Pair<TestPublisher.Probe<Integer>, Probe<Integer>> materialized = testSource.via(flowUnderTest).toMat(testSink, Keep.both()).run(mat);

        materialized.first()
                .sendNext(1)
                .sendNext(5)
                .sendNext(42)
                .sendNext(99)
                .sendComplete();

        materialized.second().request(4); // don't forget this!
        materialized.second()
                .expectNext(2, 10, 84, 198)
                .expectComplete();
    }
}
