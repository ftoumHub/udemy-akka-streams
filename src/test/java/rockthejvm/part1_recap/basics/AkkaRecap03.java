package rockthejvm.part1_recap.basics;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import java.util.Optional;

import static io.vavr.API.println;

/**
 * Cycle de vie d'un acteur.
 */
public class AkkaRecap03 {

    public static void main(String[] args) throws Exception {
        ActorSystem system = ActorSystem.create("LifeCycleSystem");

        ActorRef hulk = system.actorOf(Props.create(Hulk.class), "TheHulk");
        println("sending Hulk a message");

        hulk.tell("hello Hulk", hulk);
        Thread.sleep(5000);
        println("making Hulk get angry");
        hulk.tell(GetAngry.class, hulk);
        Thread.sleep(5000);
        println("stopping Hulk");
        system.stop(hulk);
        println("shutting down Hulk system");

        system.terminate();
    }

    static public class GetAngry {}

    public static class Hulk extends AbstractActor {
        public Hulk(){
            println("in the Hulk constructor");
        }

        @Override
        public void preStart() throws Exception {
            println("in the Hulk preStart");
        }

        @Override
        public void postStop() throws Exception {
            println("in the Hulk postStop");
        }

        @Override
        public void preRestart(Throwable reason, Optional<Object> message) throws Exception {
            println("in the Hulk preRestart");
            println(String.format("preRestart message : %s", message.orElse("")));
            println(String.format("preRestart reason : %s", reason.getMessage()));
            super.preRestart(reason, message);
        }

        @Override
        public void postRestart(Throwable reason) throws Exception {
            println("in the Hulk postRestart");
            println(String.format("postRestart reason : %s", reason.getMessage()));
            super.postRestart(reason);
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .matchEquals(GetAngry.class, m -> { throw new Exception("ROAR !"); })
                    .matchAny(m -> println("Hulk received a message...")) // cas par défaut
                    .build();
        }
    }
}
