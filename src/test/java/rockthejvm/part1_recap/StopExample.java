package rockthejvm.part1_recap;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

/**
 * The following code shows how to kill actors.
 * It is a very violent way; discretion is advised. Normally, if you
 * want to stop an actor gracefully, you use the methods described earlier.
 */
public class StopExample {

    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("StopExample");

        ActorRef sg = system.actorOf(Props.create(Scapegoat.class), "Scapegoat");
        sg.tell("ready?", ActorRef.noSender());

        // stop our crash dummy
        system.stop(sg);
        system.terminate();
    }

    public static class Scapegoat extends AbstractActor {

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, s -> System.out.println("Message Received : " + s))
                    .matchAny(m -> System.out.println("What?"))
                    .build();
        }
    }
}
