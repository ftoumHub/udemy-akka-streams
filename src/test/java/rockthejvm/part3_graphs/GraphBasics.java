package rockthejvm.part3_graphs;

import java.util.concurrent.CompletionStage;

import org.junit.Before;
import org.junit.Test;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.ClosedShape;
import akka.stream.FanInShape2;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Broadcast;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Zip;
import io.vavr.API;

public class GraphBasics {

    ActorSystem system;
    ActorMaterializer mat;

    @Before
    public void setup() {
        system = ActorSystem.create("GraphBasics");
        mat = ActorMaterializer.create(system);
    }

    @Test
    public void firstGraph() {
        final Source<Integer, NotUsed> input = Source.range(1, 1000);
        final Flow<Integer, Integer, NotUsed> incrementer = Flow.<Integer>create().map(x -> x + 1);// hard computation
        final Flow<Integer, Integer, NotUsed> multiplier = Flow.<Integer>create().map(x -> x * 10);// hard computation
        final Sink<Pair<Integer, Integer>, CompletionStage<Done>> output = Sink.foreach(API::println);

        // step 1 - setting up the fundamentals for the graph
        final RunnableGraph<CompletionStage<Done>> graph = RunnableGraph.fromGraph(
                // create() function binds sink, out which is sink's out port and builder DSL
                // we need to reference out's shape in the builder DSL below (in to() function)
                GraphDSL.create(output, // previously created sink (Sink)
                                (builder, out) -> { // variables: builder (GraphDSL.Builder) and out (SinkShape)
                                    // fan-out operator
                                    final UniformFanOutShape<Integer, Integer> broadcast = builder.add(Broadcast.create(2));
                                    // fan-in operator: Le composant zip à 2 entrées et une sortie
                                    final FanInShape2<Integer, Integer, Pair<Integer, Integer>> zip = builder.add(Zip.create());

                                    // step 3 - tying up the components
                                    builder.from(builder.add(input)).viaFanOut(broadcast);

                                    builder.from(broadcast.out(0)).via(builder.add(incrementer)).toInlet(zip.in0());
                                    builder.from(broadcast.out(1)).via(builder.add(multiplier)).toInlet(zip.in1());

                                    builder.from(zip.out()).to(out);

                                    return ClosedShape.getInstance();
                                })
        );
        graph.run(mat);
    }
}
