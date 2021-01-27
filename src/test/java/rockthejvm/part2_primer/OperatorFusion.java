package rockthejvm.part2_primer;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import io.vavr.API;

import java.util.concurrent.CompletionStage;

import static io.vavr.API.println;
import static java.time.temporal.ChronoUnit.SECONDS;
import static libs.Await.await;


public class OperatorFusion {

    public static void main(String[] args) {

        ActorSystem system = ActorSystem.create();
        ActorMaterializer mat = ActorMaterializer.create(system);

        final Source<Integer, NotUsed> simpleSource = Source.range(1, 1000);
        final Flow<Integer, Integer, NotUsed> simpleFlow1 = Flow.<Integer>create().map(__ -> __ + 1);
        final Flow<Integer, Integer, NotUsed> simpleFlow2 = Flow.<Integer>create().map(__ -> __ * 10);
        final Sink<Integer, CompletionStage<Done>> simpleSink = Sink.foreach(API::println);

        // Si on connecte ces composants, on peut obtenir le graphe suivant :
        // Les composants d'akka streams sont basés sur le principe des acteurs
        // par défaut, ces composant vont s'exécuter sur le même acteur
        // il s'agit du principe de fusion des opérateurs/composants
        //simpleSource.via(simpleFlow1).via(simpleFlow2).to(simpleSink).run(mat);

        // La fusion des opérateurs peut être couteuse si les opérations sont complexes

        // complex flows:
        final Flow<Integer, Integer, NotUsed> complexFlow = Flow.<Integer>create().map(x -> {
            await(1, SECONDS);
            println(Thread.currentThread().getName());
            return x + 1;
        });

        final Flow<Integer, Integer, NotUsed> complexFlow2 = Flow.<Integer>create().map(x -> {
            await(1, SECONDS);
            println(Thread.currentThread().getName());
            return x * 10;
        });

        // dans le cas de ce graph, on a un délai de 2 secondes entre chaque elements
        //simpleSource.via(complexFlow).via(complexFlow2).to(simpleSink).run(mat);

        // async boundary
        // dans ce graph, le délai n'est plus que d'une seconde
        simpleSource.via(complexFlow).async() // runs on one actor
                .via(complexFlow2).async() // runs on another actor
                .to(simpleSink) // runs on a third actor
                .run(mat);
    }
}
