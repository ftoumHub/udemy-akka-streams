package rockthejvm.part3_graphs;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.ClosedShape;
import akka.stream.FanInShape2;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.*;
import io.vavr.API;
import io.vavr.collection.List;
import lombok.AllArgsConstructor;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CompletionStage;

import static io.vavr.API.println;
import static java.time.Duration.ofSeconds;

public class GraphBasics {

    ActorSystem system;
    ActorMaterializer mat;

    @Before
    public void setup() {
        system = ActorSystem.create("GraphBasics");
        mat = ActorMaterializer.create(system);
    }

    @Test
    public void twoInputsOneOutputGraph() {
        // Pour une source d'entier de 1 à 1000, on veut générer une paire d'entier
        final Source<Integer, NotUsed> input = Source.range(1, 1000);

        // On veut exécuter ces 2 flows en parallèle et récupérer un tuple des valeurs générées.
        final Flow<Integer, Integer, NotUsed> incrementer = Flow.<Integer>create().map(x -> x + 1);// hard computation
        final Flow<Integer, Integer, NotUsed> multiplier = Flow.<Integer>create().map(x -> x * 10);// hard computation
        final Sink<Pair<Integer, Integer>, CompletionStage<Done>> output = Sink.foreach(API::println);

        // step 1 - setting up the fundamentals for the graph
        final RunnableGraph<CompletionStage<Done>> graph = RunnableGraph.fromGraph(
                // create() function binds sink, out which is sink's out port and builder DSL
                // we need to reference out's shape in the builder DSL below (in to() function)
                GraphDSL.create(output, // previously created sink (Sink)
                                (builder, out) -> { // variables: builder (GraphDSL.Builder) and out (SinkShape)

                                    // step 2 - add the necessary components of this graph
                                    // fan-out operator : Le composant broadcast à 1 entrée et 1 sorties
                                    final UniformFanOutShape<Integer, Integer> broadcast = builder.add(Broadcast.create(2));
                                    // fan-in operator: Le composant zip à 2 entrées et une sortie
                                    final FanInShape2<Integer, Integer, Pair<Integer, Integer>> zip = builder.add(Zip.create());

                                    // step 3 - on relie les composants entre eux
                                    builder.from(builder.add(input)).viaFanOut(broadcast);
                                    // On alimente chaque flow avec la source d'entier
                                    builder.from(broadcast.out(0)).via(builder.add(incrementer)).toInlet(zip.in0());
                                    builder.from(broadcast.out(1)).via(builder.add(multiplier)).toInlet(zip.in1());

                                    builder.from(zip.out()).to(out);

                                    // step 4 - return a closed shape
                                    return ClosedShape.getInstance();
                                })
        );
        graph.run(mat);
    }

    @Test
    public void oneInputTwoOutputsGraph() {

        @AllArgsConstructor
        class Apple{ Boolean bad; }

        final Source<Apple, NotUsed> apples = Source.from(List.fill(10, () -> new Apple(new Random().nextBoolean())));

        final Sink<Apple, CompletionStage<Done>> badApples = Sink.foreach(p -> println("bad apple"));
        final Sink<Apple, CompletionStage<Done>> goodApples = Sink.foreach(p -> println("good apple"));

        final RunnableGraph<NotUsed> graph = RunnableGraph.fromGraph(
                GraphDSL.create(apples,
                        (builder, sourceShape) -> {
                            final UniformFanOutShape<Apple, Apple> partition = builder.add(Partition.create(2, apple -> apple.bad ? 1 : 0));

                            builder.from(sourceShape)
                                    .toFanOut(partition)
                                    .from(partition.out(0))
                                    .to(builder.add(badApples))
                                    .from(partition.out(1))
                                    .to(builder.add(goodApples));

                            return ClosedShape.getInstance();
                        })
        );
        graph.run(mat);
    }

    /**
     * exercise 1: feed a source into 2 sinks at the same time
     */
    @Test
    public void sourceIntoTwoSinks() {

        final Source<Integer, NotUsed> input = Source.range(1, 1000);

        final Sink<Integer, CompletionStage<Done>> firstSink = Sink.foreach(x -> println("First Sink: "+x));
        final Sink<Integer, CompletionStage<Done>> secondSink = Sink.foreach(x -> println("Second Sink: "+x));

        final RunnableGraph<NotUsed> sourceToTwoSinksGraph = RunnableGraph.fromGraph(
                GraphDSL.create(input,
                        (builder, sourceShape) -> {
                            // step 2 - add the necessary components of this graph
                            // fan-out operator : Le composant broadcast à 1 entrée et 1 sorties
                            final UniformFanOutShape<Integer, Integer> broadcast = builder.add(Broadcast.create(2));

                            // step 3 - on relie les composants entre eux
                            builder.from(sourceShape)
                                    .toFanOut(broadcast)
                                    .from(broadcast.out(0))
                                    .to(builder.add(firstSink))
                                    .from(broadcast.out(1))
                                    .to(builder.add(secondSink));

                            // step 4 - return a closed shape
                            return ClosedShape.getInstance();
                        })
        );
        sourceToTwoSinksGraph.run(mat);

        /**
         * exercise 2: balance
         */
        Source<Integer, NotUsed> fastSource = input.throttle(5, ofSeconds(1));
        Source<Integer, NotUsed> slowSource = input.throttle(2, ofSeconds(1));
    }
}
