package rockthejvm.part2_primer;

import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static io.vavr.API.println;
import static io.vavr.API.run;
import static io.vavr.Patterns.$Failure;
import static io.vavr.Patterns.$Left;
import static io.vavr.Patterns.$Right;
import static io.vavr.Patterns.$Success;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import akka.stream.javadsl.*;
import io.vavr.API;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import io.vavr.concurrent.Future;

public class MaterializingStreams {

    ActorSystem system;
    ActorMaterializer mat;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        system = ActorSystem.create("FirstPrinciples");
        mat = ActorMaterializer.create(system);
    }

    @Test
    public void firstSource() {
        //final RunnableGraph<NotUsed> simpleGraph = Source.range(1, 10).to(Sink.foreach(i -> println(i)));
        //final NotUsed simpleMaterializedValue = simpleGraph.run(mat);

        final Source<Integer, NotUsed> source = Source.range(1, 10);
        final Sink<Integer, CompletionStage<Integer>> sink = Sink.<Integer>reduce((a, b) -> a + b);
        final CompletionStage<Integer> sumFuture = source.runWith(sink, mat);
        Future.fromCompletableFuture(sumFuture.toCompletableFuture()).onComplete(tryInt ->
            Match(tryInt).of(
                    Case($Success($()), value -> run(() -> println("The sum of all elements is: " + value))),
                    Case($Failure($()),  ex -> run(() -> println("The sum of all elements could not be computed" + ex)))
            )
        );
    }

    @Test
    public void choosingMaterializedValues() {
        final Source<Integer, NotUsed> simpleSource = Source.range(1, 10);
        final Flow<Integer, Integer, NotUsed> simpleFlow = Flow.fromFunction(x -> x + 1);
        final Sink<Integer, CompletionStage<Done>> simpleSink = Sink.foreach(API::println);
        //simpleSource.viaMat(simpleFlow, (sourceMat, flowMat) -> flowMat);
        //simpleSource.viaMat(simpleFlow, Keep.right()); // Keep.left(), Keep.both()...
        RunnableGraph<CompletionStage<Done>> graph = simpleSource.viaMat(simpleFlow, Keep.right()).toMat(simpleSink, Keep.right());
        Future.fromCompletableFuture(graph.run(mat).toCompletableFuture()).onComplete(__ ->
                Match(__).of(
                        Case($Success($()), value -> run(() -> println("Stream processing finished."))),
                        Case($Failure($()),  ex -> run(() -> println("stream processing failed with: " + ex)))
                )
        );
    }

    public void sugars() {
        CompletionStage<Integer> sum = Source.range(1, 10).runWith(Sink.reduce((a, b) -> a + b), mat);
    }
}
