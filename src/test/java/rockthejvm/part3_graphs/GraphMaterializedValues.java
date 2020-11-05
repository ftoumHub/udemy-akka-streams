package rockthejvm.part3_graphs;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.FlowShape;
import akka.stream.SinkShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.*;
import io.vavr.API;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static io.vavr.API.*;
import static io.vavr.Patterns.$Failure;
import static io.vavr.Patterns.$Success;
import static io.vavr.concurrent.Future.fromCompletableFuture;

public class GraphMaterializedValues {

    private static final ActorSystem system = ActorSystem.create("GraphMaterializedValues");
    private static final ActorMaterializer mat = ActorMaterializer.create(system);

    private static final Source<String, NotUsed> wordSource = Source.from(List("Akka", "is", "awesome", "rock", "the", "jvm"));
    private static final Sink<String, CompletionStage<Done>> printer = Sink.foreach(API::println);
    private static final Sink<String, CompletionStage<Integer>> counter = Sink.fold(0, (count, __) -> count + 1);

    public static void main(String[] args) {

        /*
          A composite component (sink)
          - prints out all strings which are lowercase
          - COUNTS the strings that are short (< 5 chars)
        */

        // step 1
        /**final Sink<String, NotUsed> complexWordSink = Sink.fromGraph(
                GraphDSL.create(builder -> {
                    // step 2 - SHAPES
                    final UniformFanOutShape<String, String> broadcast = builder.add(Broadcast.create(2));
                    final FlowShape<String, String> lowerCaseFilter = builder.add(Flow.<String>create().filter(word -> word == word.toLowerCase()));
                    final FlowShape<String, String> shortStringFilter = builder.add(Flow.<String>create().filter(__ -> __.length() < 5));

                    // step 3 - connections
                    builder.from(broadcast.out(0)).via(lowerCaseFilter).to(builder.add(printer));
                    builder.from(broadcast.out(1)).via(shortStringFilter).to(builder.add(counter));

                    // step 4 - the SHAPE
                    return SinkShape.of(broadcast.in());
                })
        );*/

        final Sink<String, CompletionStage<Integer>> complexWordSink = Sink.fromGraph(
                GraphDSL.create(counter, (builder, counterShape) -> {
                    // step 2 - SHAPES
                    final UniformFanOutShape<String, String> broadcast = builder.add(Broadcast.create(2));
                    final FlowShape<String, String> lowerCaseFilter = builder.add(Flow.<String>create().filter(word -> word == word.toLowerCase()));
                    final FlowShape<String, String> shortStringFilter = builder.add(Flow.<String>create().filter(__ -> __.length() < 5));

                    // step 3 - connections
                    builder.from(broadcast.out(0)).via(lowerCaseFilter).to(builder.add(printer));
                    builder.from(broadcast.out(1)).via(shortStringFilter).to(counterShape);

                    // step 4 - the SHAPE
                    return SinkShape.of(broadcast.in());
                })
        );

        final CompletionStage<Integer> shortStringsCountFuture  = wordSource.toMat(complexWordSink, Keep.right()).run(mat);
        fromCompletableFuture(shortStringsCountFuture.toCompletableFuture()).onComplete(result ->
                Match(result).of(
                        Case($Success($()), count -> run(() -> printf("The total number of short strings is: %s", count))),
                        Case($Failure($()), except -> run(() -> printf("The count of short strings failed: %s", except.getMessage())))
                )
        );
    }

    /**
     * Exercise
     */
    private <A, B> Flow<A, B, CompletableFuture<Integer>> enhanceFlow(Flow<A, B, NotUsed> flow) {
        return null;
    }
}