package rockthejvm.part1_recap;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.io.Udp;

import static io.vavr.API.println;

/**
 * Communication entre acteurs.
 */
public class AkkaRecap02 {

  public static void main(String[] args) {
    ActorSystem system = ActorSystem.create("CatLifeSystem");

    ActorRef sylvester = system.actorOf(Props.create(Cat.class), "Sylvester");
    ActorRef catsGod = system.actorOf(God.props(sylvester), "CatsGod");
    catsGod.tell(SendANewCat.class, ActorRef.noSender());

    system.terminate();
  }

  static public class SendANewCat {}
  static public class LiveALife {}
  static public class BackToHeaven {}
  static public class LifeSpended {
    static int remaining = 0; // a default value
  }


  public static class God extends AbstractActor {

    static public Props props(ActorRef indulged) {
      return Props.create(God.class, () -> new God(indulged));
    }

    private final ActorRef indulged;

    public God(ActorRef indulged) {
      this.indulged = indulged;
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
              .matchEquals(SendANewCat.class, m -> {
                  println("GOD: Go!, you have seven lives");
                  indulged.tell(LiveALife.class, getSelf());
              })
              .matchEquals(LifeSpended.class, m -> {
                  if (LifeSpended.remaining == 0){
                    println("GOD: Time to Return!");
                    indulged.tell(BackToHeaven.class, getSelf());
                    getContext().stop(getSelf());
                  }
                  else {
                    println("GOD: one live spent, " + LifeSpended.remaining + " remaining.");
                    indulged.tell(LiveALife.class, getSelf());
                  }
              })
              .matchAny(m -> println("GOD: Sorry, I don't understand"))
              .build();
    }
  }

  public static class Cat extends AbstractActor {

    static int lives = 7; // Tous les chats naissent avec 7 vies.

    @Override
    public Receive createReceive() {
      return receiveBuilder()
              .matchEquals(LiveALife.class, m -> {
                  println("CAT: Thanks God, I still have " + lives + " lives");
                  lives -= 1;
                  LifeSpended.remaining = lives;
                  getSender().tell(LifeSpended.class, getSelf());
              })
              .matchEquals(BackToHeaven.class, m -> {
                  if (LifeSpended.remaining == 0){
                      println("CAT: No more lives, going to Heaven");
                      getContext().stop(getSelf());
                  }
              })
              .matchAny(m -> println("CAT: Sorry, I don't understand"))
              .build();
    }
  }
}
