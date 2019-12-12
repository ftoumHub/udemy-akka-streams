package rockthejvm.part1_recap;

import akka.actor.*;

import static io.vavr.API.println;

/**
 * Created by Georges on 29/01/2019.
 */
public class ChildMonitoring {

    public static class Child extends AbstractActor {

        static Props props() {
            return Props.create(Child.class, () -> new Child());
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .matchAny(m -> println("Child received a message"))
                    .build();
        }
    }

    public static class Dad extends AbstractActor {

        ActorRef child;

        public Dad() {
            child = context().actorOf(Child.props(), "Son");
            context().watch(child);
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Terminated.class, terminated -> println("This will not end here -_-"))
                    .matchAny(m -> println("Dad received a message"))
                    .build();
        }
    }

    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("ChildMonitoring");

        ActorRef dad = system.actorOf(Props.create(Dad.class), "Dad");
        ActorSelection child = system.actorSelection("/user/Dad/Son");

        child.tell(PoisonPill.getInstance(), ActorRef.noSender());

        println("Revenge!");
        system.terminate();
    }
}
