package rockthejvm.part3_graphs;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.*;
import akka.stream.javadsl.*;
import io.vavr.API;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.Date;
import java.util.concurrent.CompletionStage;

import static io.vavr.API.List;
import static io.vavr.API.printf;
import static java.util.Arrays.asList;

/**
 * Un Graphe est dit "uniforme" lorsque ses entrées sont du même type.
 */
public class MoreOpenGraphs {

    private static final ActorSystem system = ActorSystem.create("MoreOpenGraphs");
    private static final ActorMaterializer mat = ActorMaterializer.create(system);

    /**
     Example: Max3 operator
     - 3 inputs of type int
     - return the maximum of the 3
     */
    public static void main(String[] args) {

        final Graph<UniformFanInShape, NotUsed> max3StaticGraph = GraphDSL.create(builder -> {
            // step 2 - define aux SHAPES
            final FanInShape2<Integer, Integer, Integer> max1 = builder.add(ZipWith.create(Math::max));
            final FanInShape2<Integer, Integer, Integer> max2 = builder.add(ZipWith.create(Math::max));
            // step 3
            builder.from(max1.out()).toInlet(max2.in0());

            final Inlet<Integer>[] inlets = asList(max1.in0(), max1.in1(), max2.in1()).toArray(new Inlet[3]);

            return new UniformFanInShape(max2.out(), inlets);
        });

        final Source<Integer, NotUsed> source1 = Source.range(1, 10);
        final Source<Integer, NotUsed> source2 = Source.range(1, 10).map(x -> 5);
        final Source<Integer, NotUsed> source3 = Source.range(10, 1, -1);

        final Sink<Integer, CompletionStage<Done>> sink = Sink.foreach(x -> printf("Max is: %s\n", x));

        // step 1
        final RunnableGraph<CompletionStage<Done>> max3RunnableGraph  = RunnableGraph.fromGraph(
                GraphDSL.create(sink,
                        (builder, out) -> {
                            // step 2: declaring components
                            final UniformFanInShape max3Shape = builder.add(max3StaticGraph);
                            // step 3: tying them together
                            builder.from(builder.add(source1)).viaFanIn(max3Shape);
                            builder.from(builder.add(source2)).viaFanIn(max3Shape);
                            builder.from(builder.add(source3)).viaFanIn(max3Shape);

                            builder.from(max3Shape).to(out);
                            // step 4
                            return ClosedShape.getInstance();
                        }
                )
        );

        //max3RunnableGraph.run(mat).whenComplete((d, t) -> system.terminate());

        /*
          Non-uniform fan out shape
          Processing bank transactions
          Txn suspicious if amount > 10000

          Streams component for txns
          - output1: let the transaction go through
          - output2: suspicious txn ids
        */
        @AllArgsConstructor
        @Getter
        @ToString
        class Transaction{ String id; String source; String recipient; Integer amount; Date date; }

        Source<Transaction, NotUsed> transactionSource = Source.from(List(
                new Transaction("5273890572", "Paul", "Jim", 100, new Date()),
                new Transaction("3578902532", "Daniel", "Jim", 100000, new Date()),
                new Transaction("5489036033", "Jim", "Alice", 7000, new Date())
        ));

        final Sink<Transaction, CompletionStage<Done>> bankProcessor  = Sink.foreach(API::println);
        final Sink<String, CompletionStage<Done>> suspiciousAnalysisService = Sink.foreach(txnId -> printf("Suspicious transaction ID: %s\n",txnId));

        // step 1
        final Graph<FanOutShape2, NotUsed> suspiciousTxnStaticGraph  = GraphDSL.create(builder -> {
            // step 2 - define SHAPES
            final UniformFanOutShape<Transaction, Transaction> broadcast = builder.add(Broadcast.create(2));
            final FlowShape<Transaction, Transaction> suspiciousTxnFilter  = builder.add(Flow.<Transaction>create().filter(txn -> txn.amount > 10000));
            final FlowShape<Transaction, String> txnIdExtractor  = builder.add(Flow.fromFunction(txn -> txn.id));
            // step 3 - tie SHAPES
            builder.from(broadcast.out(0)).via(suspiciousTxnFilter).via(txnIdExtractor);

            return new FanOutShape2(broadcast.in(), broadcast.out(1), txnIdExtractor.out());
        });

        // step 1
        final RunnableGraph<NotUsed> suspiciousTxnRunnableGraph   = RunnableGraph.fromGraph(
                GraphDSL.create(transactionSource,
                        (builder, sourceShape) -> {
                            // step 2: declaring components
                            final FanOutShape2 suspiciousTxnShape  = builder.add(suspiciousTxnStaticGraph);
                            // step 3: tying them together
                            builder.from(sourceShape).toInlet(suspiciousTxnShape.in());
                            builder.from(suspiciousTxnShape.out0()).to(builder.add(bankProcessor));
                            builder.from(suspiciousTxnShape.out1()).to(builder.add(suspiciousAnalysisService));
                            // step 4
                            return ClosedShape.getInstance();
                        }
                )
        );

        suspiciousTxnRunnableGraph.run(mat);
    }

}