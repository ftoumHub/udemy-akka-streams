package rockthejvm.part3_graphs;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.*;
import akka.stream.javadsl.*;
import io.vavr.API;
import io.vavr.collection.List;
import lombok.AllArgsConstructor;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletionStage;

import static io.vavr.API.println;

public class OpenGraphs {

    ActorSystem system;
    ActorMaterializer mat;

    @Before
    public void setup() {
        system = ActorSystem.create("OpenGraphs");
        mat = ActorMaterializer.create(system);
    }

    @Test
    public void openGraph() {
        /**
         * A composite source that concatenates 2 sources
         * - emits ALL the elements from the first source
         * - then ALL the elements from the second
         */

        Source<Integer, NotUsed> firstSource = Source.range(1, 10);
        Source<Integer, NotUsed> secondSource = Source.range(42, 1000);

        // step 1
        final Source<Integer, NotUsed> sourceGraph = Source.fromGraph(
                GraphDSL.create(builder -> {
                    final UniformFanInShape<Integer, Integer> concat = builder.add(Concat.create(2));

                    builder.from(builder.add(firstSource)).toInlet(concat.in(0));
                    builder.from(builder.add(secondSource)).toInlet(concat.in(1));

                    return SourceShape.of(concat.out());
                })
        );
        sourceGraph.to(Sink.foreach(API::println)).run(mat);
    }
}
