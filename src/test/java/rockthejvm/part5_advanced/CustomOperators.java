package rockthejvm.part5_advanced;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.*;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import akka.stream.stage.InHandler;
import akka.stream.stage.OutHandler;
import io.vavr.collection.List;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

import static io.vavr.API.println;

public class CustomOperators {

    static ActorSystem system = ActorSystem.create("CustomOperators");
    static ActorMaterializer materializer = ActorMaterializer.create(system);

    public static void main(String[] args) {

        final Source<Integer, NotUsed> randomGeneratorSource = Source.fromGraph(new RandomNumberGenerator(100));
        //randomGeneratorSource.runWith(Sink.foreach(API::println), materializer);

        final Sink<Integer, NotUsed> batcherSink = Sink.fromGraph(new Batcher(10));
        randomGeneratorSource.to(batcherSink).run(materializer);
    }

    // 1 - a custom source which emits random numbers until canceled

    static class RandomNumberGenerator extends GraphStage</**step 0: define the shape*/SourceShape<Integer>> {

        // step 1: define the ports and the component-specific members
        private final Outlet<Integer> outPort = Outlet.create("randomGenerator");
        private final Random random = new Random();

        private Integer max;

        public RandomNumberGenerator(Integer max) {
            this.max = max;
        }

        // step 2: construct a new shape
        @Override
        public SourceShape<Integer> shape() {
            return new SourceShape(outPort);
        }

        // step 3: create the logic
        @Override
        public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception, Exception {
            // implement my logic here
            return new GraphStageLogic(shape()) {
                {
                    setHandler(outPort, new OutHandler() {
                        // when there is demand from downstream
                        @Override
                        public void onPull() throws Exception, Exception {
                            // emit a new element
                            push(outPort, random.nextInt(max));
                        }
                    });
                }
            };
        }
    }

    // 2 - a custom sink that prints elements in batches of a given size

    static class Batcher extends GraphStage<SinkShape<Integer>> {

        private final Inlet inPort = Inlet.<Integer>create("batcher");

        private Integer batchSize;

        public Batcher(Integer batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public SinkShape<Integer> shape() {
            return new SinkShape(inPort);
        }

        @Override
        public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception, Exception {
            LinkedList batch = new LinkedList<Integer>();
            return new GraphStageLogic(shape()) {
                @Override
                public void preStart() throws Exception, Exception {
                    super.preStart();
                    pull(inPort);
                }

                {
                    setHandler(inPort, new InHandler() {
                        @Override
                        public void onPush() throws Exception, Exception {
                            final Integer nextElement = (Integer) grab(inPort);
                            batch.add(nextElement);

                            // assume some complex computation
                            Thread.sleep(100);
                            List result = List.empty();

                            if (batch.size() >= batchSize) {
                                while (!batch.isEmpty()) {
                                    final Integer next = (Integer) batch.remove();
                                    result = result.prepend(next);
                                }
                                println("New batch: " + result.mkString("[", ",", "]"));
                            }

                            pull(inPort); // send demand upstream
                        }

                        @Override
                        public void onUpstreamFinish() throws Exception, Exception {
                            List result = List.empty();
                            if (!batch.isEmpty()) {
                                while (!batch.isEmpty()) {
                                    final Integer next = (Integer) batch.remove();
                                    result = result.prepend(next);
                                }
                                println("New batch: " + result.mkString("[", ",", "]"));
                                println("Stream finished");
                            }
                        }
                    });
                }
            };
        }
    }
}