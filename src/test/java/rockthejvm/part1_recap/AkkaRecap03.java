package rockthejvm.part1_recap;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import java.util.Optional;

/**
 * Cycle de vie d'un acteur.
 */
public class AkkaRecap03 {

    public static void main(String[] args) throws Exception {
        ActorSystem system = ActorSystem.create("LifeCycleSystem");

        ActorRef hulk = system.actorOf(Props.create(Hulk.class), "TheHulk");
        System.out.println("sending Hulk a message");

        hulk.tell("hello Hulk", hulk);
        Thread.sleep(5000);
        System.out.println("making Hulk get angry");
        hulk.tell(GetAngry.class, hulk);
        Thread.sleep(5000);
        System.out.println("stopping Hulk");
        system.stop(hulk);
        System.out.println("shutting down Hulk system");

        system.terminate();
    }

    static public class GetAngry {}

    public static class Hulk extends AbstractActor {
        public Hulk(){
            System.out.println("in the Hulk constructor");
        }

        @Override
        public void preStart() throws Exception {
            System.out.println("in the Hulk preStart");
        }

        @Override
        public void postStop() throws Exception {
            System.out.println("in the Hulk postStop");
        }

        @Override
        public void preRestart(Throwable reason, Optional<Object> message) throws Exception {
            System.out.println("in the Hulk preRestart");
            System.out.println(String.format("preRestart message : %s", message.orElse("")));
            System.out.println(String.format("preRestart reason : %s", reason.getMessage()));
            super.preRestart(reason, message);
        }

        @Override
        public void postRestart(Throwable reason) throws Exception {
            System.out.println("in the Hulk postRestart");
            System.out.println(String.format("postRestart reason : %s", reason.getMessage()));
            super.postRestart(reason);
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .matchEquals(GetAngry.class, m -> { throw new Exception("ROAR !"); })
                    .matchAny(m -> System.out.println("Hulk received a message...")) // cas par défaut
                    .build();
        }
    }
}
