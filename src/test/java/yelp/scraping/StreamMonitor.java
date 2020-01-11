package yelp.scraping;

import akka.NotUsed;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.stream.javadsl.Flow;

import java.util.function.Consumer;

public class StreamMonitor {

    public static class ElementReceived {}

    public static <T> Flow<T, T, NotUsed> monitor(Integer logEvery, Consumer<Integer> logMessage, ActorSystem system) {
        final ActorRef monitorActor = system.actorOf(StreamMonitorActor.props(logEvery, logMessage));

        return Flow.<T>create().map(element -> {
            monitorActor.tell(new ElementReceived(), ActorRef.noSender());
            return element;
        });
    }

    private static class StreamMonitorActor extends AbstractActor {

        private int numberElements = 0;

        private final Integer logEvery;
        private final Consumer<Integer> logMessage;

        public StreamMonitorActor(Integer logEvery, Consumer<Integer> logMessage) {
            this.logEvery = logEvery;
            this.logMessage = logMessage;
        }

        public static Props props(Integer logEvery, Consumer<Integer> logMessage) {
            return Props.create(StreamMonitorActor.class, () -> new StreamMonitorActor(logEvery, logMessage));
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(ElementReceived.class, element -> {
                        numberElements += 1;
                        if (numberElements % logEvery == 0) { logMessage.accept(numberElements); }
                    })
                    .build();
        }
    }
}
