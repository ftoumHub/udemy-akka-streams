package rockthejvm.part3_graphs;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.*;
import akka.stream.javadsl.*;
import io.vavr.API;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.concurrent.CompletionStage;
import java.util.stream.IntStream;

public class GraphEx {

    public static void main(String[] args) {

        ActorSystem system = ActorSystem.create("demo");
        ActorMaterializer materializer = ActorMaterializer.create(system);

        // create a stream for the following graph
        //
        // in -> f1 -> bcast /  f2 \ merge -> f3 -> out
        //                   \  f4 /
        final Sink<Integer, CompletionStage<Done>> aSink = Sink.foreach(API::println);
        final RunnableGraph<CompletionStage<Done>> runnable = RunnableGraph.fromGraph(
                GraphDSL.create(aSink, (builder, out) -> {
                    IntStream stream = IntStream.range(1, 4);
                    Outlet<Integer> in = builder.add(Source.from(() -> stream.iterator())).out();
                    FlowShape<Integer, Integer> f1 = builder.add(Flow.fromFunction(x -> x + 10));
                    FlowShape<Integer, Integer> f2 = builder.add(Flow.<Integer>create().map(x -> x + 10));
                    FlowShape<Integer, Integer> f3 = builder.add(Flow.<Integer>create().map(x -> x + 10));
                    FlowShape<Integer, Integer> f4 = builder.add(Flow.<Integer>create().map(x -> x + 10));
                    UniformFanOutShape<Integer, Integer> bcast = builder.add(Broadcast.create(2));
                    UniformFanInShape<Integer, Integer> merge = builder.add(Merge.create(2));

                    builder.from(in).via(f1).viaFanOut(bcast).via(f2).viaFanIn(merge).via(f3).to(out);
                    builder.from(bcast).via(f4).toFanIn(merge);

                    return ClosedShape.getInstance();
                })
        );

        runnable.run(materializer).whenComplete((d, t) -> system.terminate());
    }
}
