package rockthejvm.part1_recap;

import akka.actor.*;

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
                    .matchAny(m -> System.out.println("Child received a message"))
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
                    .match(Terminated.class, terminated -> System.out.println("This will not end here -_-"))
                    .matchAny(m -> System.out.println("Dad received a message"))
                    .build();
        }
    }

    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("ChildMonitoring");

        ActorRef dad = system.actorOf(Props.create(Dad.class), "Dad");
        ActorSelection child = system.actorSelection("/user/Dad/Son");

        child.tell(PoisonPill.getInstance(), ActorRef.noSender());

        System.out.println("Revenge!");
        system.terminate();
    }
}
