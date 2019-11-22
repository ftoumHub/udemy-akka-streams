package rockthejvm.part1_recap;

import static io.vavr.API.println;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

public class AkkaRecap00 {

    public static class SimpleActor extends AbstractActor {

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .matchEquals("change", msg -> getContext().become(anotherHandler()))
                    .matchAny(msg -> println("I received: " + msg))
                    .build();
        }

        private Receive anotherHandler() {
            println("In another handler");
            return receiveBuilder()
                    .matchAny(msg -> println("In another receiveHandler: " + msg))
                    .build();
        }
    }

    /**
     * - messages are sent asynchronously
     * - many actors (in the millions) can share a few dozen threads
     * - each messages is processed/handled ATOMICALLY
     * - no needs for locks
     */
    public static void main(String[] args) {
        // actor encapsulation
        ActorSystem actorSystem = ActorSystem.create("AkkaRecap");
        // #1: you can only instantiate an actor through the actor system
        ActorRef actor = actorSystem.actorOf(Props.create(SimpleActor.class), "SimpleActor");
        // #2: sending messages

        actor.tell("change", actor);
        actor.tell("hello", actor);

        actorSystem.terminate();
    }
}
