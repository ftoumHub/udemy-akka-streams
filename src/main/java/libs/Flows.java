package libs;

import akka.Done;
import akka.NotUsed;
import akka.japi.function.Function2;
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.stream.stage.*;
import akka.util.ByteString;
import io.vavr.collection.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.hashing.MurmurHash3$;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class Flows {

    public static Logger LOGGER = LoggerFactory.getLogger(Flows.class);

    public static Function2<NotUsed, CompletionStage<Done>, NotUsed> logError() {
        return (nu, done) -> {
            done.whenComplete((__, e) -> {
                if (e !=null) {
                    LOGGER.error("Error during resynchro", e);
                }
            });
            return nu;
        };
    }

    public static <A> Flow<A, A, NotUsed> logErrorFlow() {
        return Flow.<A>create().watchTermination(logError());
    }

    public static <A> Flow<A, A, NotUsed> onComplete(Runnable r) {
        return Flow.<A>create()
                .watchTermination((nu, done) -> {
                    done.whenComplete((__, ___) -> {
                        r.run();
                    });
                    return nu;
                });
    }

    public static <T> Flow<T, List<T>, NotUsed> groupFlow(Integer count) {
        return Flow.<T>create().grouped(count).map(List::ofAll);
    }

    public static <T> Flow<T, List<T>, NotUsed> groupOne() {
        return Flow.<T>create().map(List::of);
    }

    public static <In, Out> Flow<In, Out, NotUsed> shard(Integer parallelism, Function<In, String> getId, Flow<In, Out, NotUsed> worker) {
        return Flow.fromGraph(
                GraphDSL.create(
                        b -> {
                            final UniformFanOutShape<In, In> partitionStage = b.add(Partition.create(parallelism, (elt) ->
                                    Math.abs(MurmurHash3$.MODULE$.stringHash(getId.apply(elt))) % parallelism
                            ));
                            UniformFanInShape<Out, Out> merge = b.add(Merge.create(parallelism));

                            for (int i = 0; i < parallelism; i++) {
                                b.from(partitionStage.out(i)).via(b.add(worker.async())).toInlet(merge.in(i));
                            }
                            return FlowShape.of(partitionStage.in(), merge.out());
                        }));
    }

    @SafeVarargs
    public static <In, Out> Flow<In, Out, NotUsed> broadcast(Flow<In, Out, NotUsed>... allWorkers) {
        return Flow.fromGraph(
                GraphDSL.create(
                        b -> {
                            List<Flow<In, Out, NotUsed>> workers = List.of(allWorkers);
                            int parallelism = workers.size();
                            final UniformFanOutShape<In, In> partitionStage = b.add(Broadcast.create(parallelism));
                            UniformFanInShape<Out, Out> merge = b.add(Merge.create(parallelism));

                            workers.zipWithIndex().forEach(t -> {
                                Integer index = t._2;
                                Flow<In, Out, NotUsed> worker = t._1;
                                b.from(partitionStage.out(index)).via(b.add(worker.async())).toInlet(merge.in(index));
                            });
                            return FlowShape.of(partitionStage.in(), merge.out());
                        }));
    }

    public static Flow<ByteString, ByteString, NotUsed> discard() {
        return Flow.<ByteString>create().reduce((a, b) -> ByteString.empty());
    }


    /**
     * Accumule les messages par paquet de `size`. Si `timeOut` est dépassé entre 2 messages, le paquet accumulé est publié.
     * @param size
     * @param timeOut
     * @param <T>
     * @return
     */
    public static <T> Flow<T, List<T>, NotUsed> groupedTimeout(int size, Duration timeOut) {
        return Flow.fromGraph(new GroupedTimeout<T>(size, timeOut));
    }


    private static class GroupedTimeout<T> extends GraphStage<FlowShape<T, List<T>>> {

        public final Inlet<T> in = Inlet.create("GroupedTimeout.in");
        public final Outlet<List<T>> out = Outlet.create("GroupedTimeout.in");

        private final FlowShape<T, List<T>> shape = FlowShape.of(in, out);

        private final Duration timeout;
        private final int maxBatch;

        public GroupedTimeout(int maxBatch, Duration timeout) {
            this.timeout = timeout;
            this.maxBatch = maxBatch;
        }

        @Override
        public FlowShape<T, List<T>> shape() {
            return shape;
        }

        @Override
        public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
            return new TimerGraphStageLogic(shape) {

                private List<T> currentBuffer = List.empty();
                private Long lastIndex = 0L;
                private boolean triggerPush = false;

                @Override
                public void preStart() throws Exception {
                    pull(in);
                }

                { setHandler(
                        in,
                        new AbstractInHandler() {
                            @Override
                            public void onPush() throws Exception {
                                T element = grab(in);
                                cancelTimer(0L);
                                currentBuffer = currentBuffer.append(element);
                                lastIndex = lastIndex + 1;
                                scheduleOnce(0L, timeout);
                                handlePushMessage();
                                pull(in);
                            }

                            @Override
                            public void onUpstreamFinish() throws Exception {
                                if (isAvailable(out)) {
                                    pushMessages();
                                }
                                super.onUpstreamFinish();
                            }
                        });
                    setHandler(
                            out,
                            new AbstractOutHandler() {
                                @Override
                                public void onPull() throws Exception {
                                    handlePushMessage();
                                }
                            });
                }

                @Override
                public void onTimer(Object timerKey) throws Exception {
                    if (timerKey instanceof Long) {
                        triggerPush = true;
                        handlePushMessage();
                    }
                }

                private void handlePushMessage() {
                    if (isAvailable(out) && currentBuffer.length() >= maxBatch) {
                        pushMessages();
                    } else if (isAvailable(out) && triggerPush) {
                        pushMessages();
                    }
                }

                private void pushMessages() {
                    if (currentBuffer.nonEmpty()) {
                        push(out, currentBuffer);
                        currentBuffer = List.empty();
                    }
                    triggerPush = false;
                }
            };
        }
    }


}
