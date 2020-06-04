package rockthejvm.part3_graphs;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.*;
import akka.stream.javadsl.*;
import io.vavr.API;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletionStage;

import static io.vavr.API.println;

public class OpenGraphs {

    ActorSystem system;
    ActorMaterializer mat;

    Source<Integer, NotUsed> firstSource = Source.range(1, 10);
    Source<Integer, NotUsed> secondSource = Source.range(42, 1000);

    @Before
    public void setup() {
        system = ActorSystem.create("OpenGraphs");
        mat = ActorMaterializer.create(system);
    }

    //         +------------------+
    //  S1 *-->|                  |
    //         |      Concat      |----> S1 then S2 = S3 -> On retourne une Source
    //  S2 *-->|                  |
    //         +------------------+
    @Test
    public void creatingAComplexSource() {
        /**
         * A composite source that concatenates 2 sources
         * - emits ALL the elements from the first source
         * - then ALL the elements from the second
         */
        // A la place d'utiliser RunnableGraph, on utilise ici "Source.fromGraph".
        // Ceci permet de créer un composant à partir d'un graph.
        final Source<Integer, NotUsed> sourceGraph = Source.fromGraph(
                GraphDSL.create(builder -> {
                    // On crée une instance de l'opérateur qu'on veut utiliser, ici Concat
                    final UniformFanInShape<Integer, Integer> concat = builder.add(Concat.create(2));

                    builder.from(builder.add(firstSource)).toInlet(concat.in(0));
                    builder.from(builder.add(secondSource)).toInlet(concat.in(1));

                    // A la place de ClosedShape on utilise ici SourceShape
                    return SourceShape.of(concat.out());
                })
        );
        sourceGraph.to(Sink.foreach(API::println)).run(mat);
    }

    // A partir d'une source unique on veut alimenter 2 Sink.
    @Test
    public void creatingAComplexSink() {
        final Sink<Integer, CompletionStage<Done>> sink1 = Sink.foreach(x -> println("Meaningful thing 1: " + x));
        final Sink<Integer, CompletionStage<Done>> sink2 = Sink.foreach(x -> println("Meaningful thing 2 : " + x));

        final Sink<Integer, NotUsed> sinkGraph = Sink.fromGraph(
                GraphDSL.create(builder -> {
                    // On crée une instance de l'opérateur qu'on veut utiliser, ici Concat
                    final UniformFanOutShape<Integer, Integer> broadcast = builder.add(Broadcast.create(2));

                    builder.from(broadcast.out(0)).to(builder.add(sink1));
                    builder.from(broadcast.out(1)).to(builder.add(sink2));

                    // A la place de ClosedShape on utilise ici SourceShape
                    return SinkShape.of(broadcast.in());
                })
        );
        firstSource.to(sinkGraph).run(mat);
    }
}