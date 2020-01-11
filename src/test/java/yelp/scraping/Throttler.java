package yelp.scraping;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import io.vavr.collection.Queue;

import java.time.Duration;

import static io.vavr.API.println;

public class Throttler extends AbstractActor {

    // messages received
    public static class WantToPass {}
    public static class RequestLimitExceeded {}

    // messages sent by this actor
    public static class MayPass {}

    // messages sent to itself
    public static class Open {}

    private Queue<ActorRef> waitQueue = Queue.empty();

    public static Props props() {
        return Props.create(Throttler.class);
    }

    @Override
    public Receive createReceive() {
        return open();
    }

    private Receive open() {
        return receiveBuilder()
                .matchEquals(WantToPass.class,
                        __ -> getSender().tell(MayPass.class, getSelf()))
                .matchEquals(RequestLimitExceeded.class,
                        __ -> {
                            println("Request limit exceeded: throttling");
                            getContext().become(closed());
                            scheduleOpen();
                        })
                .matchAny(__ -> println("open state " + __.getClass()))
                .build();
    }

    private Receive closed() {
        return receiveBuilder()
                .matchEquals(WantToPass.class,
                        __ -> {
                            println("Currently throttled: queueing request. Current queue size " + waitQueue.size());
                            waitQueue = waitQueue.enqueue(sender());
                        })
                .matchEquals(Open.class,
                        __ -> {
                            println("Releasing waiting actors");
                            getContext().become(open());
                            releaseWaiting();
                            println("Actors released");
                        })
                .matchAny(__ -> println("closed state"))
                .build();
    }

    private void releaseWaiting() {
        waitQueue.forEach(actorRef -> actorRef.tell(MayPass.class, ActorRef.noSender()));
    }

    private void scheduleOpen() {
        getContext().getSystem()
                .getScheduler()
                .scheduleOnce(
                        Duration.ofMinutes(30),
                        () -> getSelf().tell(Open.class, ActorRef.noSender()),
                        getContext().getSystem().getDispatcher());
    }

}
