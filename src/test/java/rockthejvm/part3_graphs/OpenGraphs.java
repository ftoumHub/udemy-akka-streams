package rockthejvm.part3_graphs;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.*;
import akka.stream.javadsl.*;
import io.vavr.API;

import java.util.concurrent.CompletionStage;

import static io.vavr.API.println;

public class OpenGraphs {

    private static final ActorSystem system = ActorSystem.create("OpenGraphs");
    private static final ActorMaterializer mat = ActorMaterializer.create(system);

    private static final Source<Integer, NotUsed> firstSource = Source.range(1, 10);
    private static final Source<Integer, NotUsed> secondSource = Source.range(42, 1000);

    public static void main(String[] args) {
        //creatingAComplexSource();
        //creatingAComplexSink();
        creatingAComplexFlow();
    }

    private static void reminder() {
        // Jusqu'à présent on a écrit des graphes executables en suivant ce squelette :

        // step 1
        final RunnableGraph<Object> graph = RunnableGraph.fromGraph(
                GraphDSL.create((Graph<Shape, Object>) null,
                    (builder, out) -> {
                        // step 2: declaring components

                        // step 3: tying them together

                        // step 4
                        return ClosedShape.getInstance();
                    }
                )
        );

        graph.run(mat);
    }

    //         +------------------+
    //  S1 *-->|                  |
    //         |      Concat      |----> S1 then S2 = S3 -> On retourne une Source
    //  S2 *-->|                  |
    //         +------------------+
    private static void creatingAComplexSource() {
        /**
         * A composite source that concatenates 2 sources
         * - emits ALL the elements from the first source
         * - then ALL the elements from the second
         */
        // A la place d'utiliser RunnableGraph, on utilise ici "Source.fromGraph".
        // Ceci permet de créer un composant à partir d'un graph.
        final Source<Integer, NotUsed> sourceGraph = Source.fromGraph(
                GraphDSL.create(builder -> {
                    // step 1
                    // On crée une instance de l'opérateur qu'on veut utiliser, ici Concat
                    final UniformFanInShape<Integer, Integer> concat = builder.add(Concat.create(2));

                    // step 2
                    //builder.from(builder.add(firstSource)).toInlet(concat.in(0));
                    //builder.from(builder.add(secondSource)).toInlet(concat.in(1));

                    builder.from(builder.add(firstSource)).toFanIn(concat);
                    builder.from(builder.add(secondSource)).toFanIn(concat);

                    // A la place de ClosedShape on utilise ici SourceShape
                    return SourceShape.of(concat.out());
                })
        );
        sourceGraph.to(Sink.foreach(API::println)).run(mat);
    }

    // A partir d'une source unique on veut alimenter 2 Sink.
    private static void creatingAComplexSink() {
        final Sink<Integer, CompletionStage<Done>> sink1 = Sink.foreach(x -> println("Meaningful thing 1: " + x));
        final Sink<Integer, CompletionStage<Done>> sink2 = Sink.foreach(x -> println("Meaningful thing 2: " + x));

        final Sink<Integer, NotUsed> sinkGraph = Sink.fromGraph(
                GraphDSL.create(builder -> {
                    // On crée une instance de l'opérateur qu'on veut utiliser, ici Concat
                    final UniformFanOutShape<Integer, Integer> broadcast = builder.add(Broadcast.create(2));
                    final SinkShape<Integer> sinkShape1 = builder.add(sink1);
                    final SinkShape<Integer> sinkShape2 = builder.add(sink2);

                    builder.from(broadcast.out(0)).to(sinkShape1);
                    builder.from(broadcast.out(1)).to(sinkShape2);

                    // A la place de ClosedShape on utilise ici SinkShape
                    return SinkShape.of(broadcast.in());
                })
        );
        // on peut maintenant cabler une source à notre sink
        firstSource.to(sinkGraph).run(mat);
    }

    /**
     * Challenge - complex flow?
     * Write your own flow that's composed of two other flows
     * - one that adds 1 to a number
     * - one that does number * 10
     */
    private static void creatingAComplexFlow() {
        final Flow<Integer, Integer, NotUsed> incrementer = Flow.<Integer>create().map(x -> x + 1);
        final Flow<Integer, Integer, NotUsed> multiplier  = Flow.<Integer>create().map(x -> x * 10);

        // step 1
        final Flow<Integer, Integer, NotUsed> flowGraph = Flow.fromGraph(
                GraphDSL.create(builder -> {
                    // step 2
                    final FlowShape<Integer, Integer> incrementerShape = builder.add(incrementer);
                    final FlowShape<Integer, Integer> multiplierShape = builder.add(multiplier);
                    // step 3
                    builder.from(incrementerShape).via(multiplierShape);
                    // step 4
                    return FlowShape.of(incrementerShape.in(), multiplierShape.out());
                })
        );

        firstSource.via(flowGraph)
                .runForeach(API::println, mat)
                .whenComplete((d, t) -> system.terminate());
    }

    /**
     * Exercise: flow from a sink and a source?
     */
    private <A, B> Flow<A, B, NotUsed> fromSinkAndSource(Sink<A, NotUsed> sink, Source<B, NotUsed> source) {
        return Flow.fromGraph(
                GraphDSL.create(builder -> {
                    final SourceShape<B> sourceShape = builder.add(source);
                    final SinkShape<A> sinkShape = builder.add(sink);

                    return FlowShape.of(sinkShape.in(), sourceShape.out());
                })
        );
    }

    //final Flow<String, Integer, NotUsed> f = Flow.fromSinkAndSource(Sink.foreach(API::println), Source.range(1, 10));

    final Flow<String, Integer, NotUsed> f = Flow.fromSinkAndSourceCoupled(Sink.foreach(API::println), Source.range(1, 10));
}