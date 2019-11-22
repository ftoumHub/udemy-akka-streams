package rockthejvm.part4_techniques;

import static org.junit.Assert.assertEquals;
import static scala.compat.java8.FutureConverters.*;

import java.util.concurrent.CompletionStage;

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
    static TestKit probe;

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
    public void simpleStream() throws Exception {

        final Source<Integer, NotUsed> simpleSource = Source.range(0, 10);

        // Ce sink va additionner les valeurs qui lui sont passé.
        // Lorsqu'il est matérialisé, il expose son resultat sous forme d'un CompletionStage de Integer
        final Sink<Integer, CompletionStage<Integer>> simpleSink = Sink.fold(0, (agg, next) -> agg + next);

        final CompletionStage<Integer> run = simpleSource.log("SimpleSource")
                .toMat(simpleSink, Keep.right()).run(mat);
        int result = run.toCompletableFuture().join();
        assertEquals(55, result);
    }

    @Test
    public void integrateWithTestActorsViaMaterializedValues() {
        probe = new TestKit(system); // Acteur spécial avec des capacités de test
        final Source<Integer, NotUsed> simpleSource = Source.range(0, 10);
        final Sink<Integer, CompletionStage<Integer>> simpleSink = Sink.fold(0, (agg, next) -> agg + next);

        final CompletionStage<Integer> future = simpleSource.toMat(simpleSink, Keep.right()).run(mat);

        akka.pattern.Patterns.pipe(toScala(future), system.dispatcher()).to(probe.getRef());
        probe.expectMsg(55);
    }

    @Test
    public void integrateWithATestActorBasedSink() {
        probe = new TestKit(system); // Acteur spécial avec des capacités de test
        final Source<Integer, NotUsed> simpleSource = Source.range(0, 10);
        final Flow<Integer, Integer, NotUsed> flow = // 0, 1, 3, 6, 10, 15
                Flow.<Integer>create().scan(0, (agg, next) -> agg + next);
        final Source<Integer, NotUsed> streamUnderTest = simpleSource.via(flow);

        final Sink<Integer, NotUsed> probeSink = Sink.actorRef(probe.getRef(), "CompletionMessage");
        streamUnderTest
                .log("StreamUnderTest")
                .to(probeSink).run(mat);
        probe.expectMsgAllOf(0, 0, 1, 3, 6, 10, 15, 21, 28, 36, 45, 55);
    }
}
