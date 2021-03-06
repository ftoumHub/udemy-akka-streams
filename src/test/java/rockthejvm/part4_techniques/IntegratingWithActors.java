package rockthejvm.part4_techniques;

import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.Timeout;
import io.vavr.API;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static libs.Await.await;

public class IntegratingWithActors {

    public static void main(String[] args) {

        ActorSystem system = ActorSystem.create("IntegratingWithActors");
        ActorMaterializer mat = ActorMaterializer.create(system);

        ActorRef simpleActor = system.actorOf(Props.create(SimpleActor.class), "simpleActor");

        // On définit une source d'entier qu'on va passer à notre simpleActor
        final Source<Integer, NotUsed> numbersSource = Source.range(1, 10);

        // On va utiliser notre acteur sous la forme d'un Flow utilisant la méthode ask pour requêter
        // l'acteur (envoi d'un message et attente d'une réponse sous la forme d'une Future avec timeout)
        // Le facteur de parallélisation définit le nb de messages possibles dans la mailbox avant
        // que l'acteur déclenche le mécanisme de backpressure
        // Etant donnée que les futures peuvent retourner n'importe quel type, on indique le type
        // qui nous intéresse, ici, Integer.
        final Flow<Integer, Integer, NotUsed> actorBasedFlow =
                Flow.<Integer>create().ask(4, simpleActor, Integer.class, new Timeout(2, SECONDS));

        numbersSource.via(actorBasedFlow).to(Sink.ignore()).run(mat);
        // equivalent à :
        //numbersSource.ask(4, simpleActor, Integer.class, new Timeout(2, TimeUnit.SECONDS));

        await(2000, MILLIS);

        system.terminate();
    }

    private static class SimpleActor extends AbstractLoggingActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, s -> {
                        log().info("Just received a string " + s);
                        //sender().tell();
                    })
                    .match(Integer.class, n -> {
                        log().info("Just received an integer " + n);
                        sender().tell(n, self());
                    })
                    .matchAny(API::println)
                    .build();
        }

    }
}
