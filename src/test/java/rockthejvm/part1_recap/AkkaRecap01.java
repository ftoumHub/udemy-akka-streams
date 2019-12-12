package rockthejvm.part1_recap;

import akka.actor.*;

import static io.vavr.API.println;

/**
 * Un premier example d'acteur simple.
 *
 * For our first example, we use a multilanguage greeter; that is, we enter a particular language and the
 * program responds by replying “Good day,” in the language specified.
 */
public class AkkaRecap01 {

  public static void main(String[] args) {
    ActorSystem actorSystem = ActorSystem.create("MultilangSystem");

    ActorRef greeter = actorSystem.actorOf(Props.create(GreeterActor.class), "GreeterActor");

    greeter.tell("en", greeter);
    greeter.tell("es", greeter);
    greeter.tell("fr", greeter);
    greeter.tell("de", greeter);
    greeter.tell("pt", greeter);
    greeter.tell("zh-CN", greeter);

    actorSystem.terminate();
  }

  public static class GreeterActor extends AbstractActor {

    @Override
    public Receive createReceive() {
      return receiveBuilder()
              .matchEquals("en", m -> println("GoodDay"))
              .matchEquals("es", m -> println("Buen dia"))
              .matchEquals("fr", m -> println("Bonjour"))
              .matchEquals("de", m -> println("Guten Tag"))
              .matchEquals("pt", m -> println("Bom dia"))
              .matchAny(m -> println(":(")) // cas par défaut
              .build();
    }
  }
}
