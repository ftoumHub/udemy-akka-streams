package rockthejvm.part2_primer;

import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static io.vavr.API.println;
import static io.vavr.API.run;
import static io.vavr.Patterns.$Failure;
import static io.vavr.Patterns.$Left;
import static io.vavr.Patterns.$Right;
import static io.vavr.Patterns.$Success;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import akka.stream.javadsl.*;
import io.vavr.API;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import io.vavr.concurrent.Future;

/**
 * Cette classe permet d'apprendre à "extraire" des valeurs
 * d'un stream en cours d'exécution.
 *
 * On appelle la valeur retournée par l'exécution d'un graphe
 * sa valeur matérialisée.
 *
 * Matérialiser un graphe implique de matérialiser tous les composants de ce graphe.
 */
public class MaterializingStreams {

    ActorSystem system;
    ActorMaterializer mat;

    @Before
    public void setup() {
        system = ActorSystem.create("MaterializingStreams");
        mat = ActorMaterializer.create(system);
    }

    @Test
    public void simpleGraph() {
        final RunnableGraph<NotUsed> simpleGraph = Source.range(1, 10).to(Sink.foreach(i -> println(i)));
        // La valeur retournée par l'appel à la methode est la valeur matérialisée du graphe.
        // Par défaut le graphe retourne la valeur matérialisée de l'élément de gauche
        // cad la valeur de Source.range(1, 10), d'où le type NotUsed.
        final NotUsed simpleMaterializedValue = simpleGraph.run(mat);
    }

    @Test
    public void firstMaterializedValue() {
        final Source<Integer, NotUsed> source = Source.range(1, 10);
        // Une fois que ce sink aura fini, il exposera une future de integer representant la somme des éléments
        final Sink<Integer, CompletionStage<Integer>> sink = Sink.reduce((a, b) -> a + b);
        // En exécutant le graphe, on va avoir accès à la somme des éléments calculés par le sink
        final CompletionStage<Integer> sumFuture = source.runWith(sink, mat);

        Future.fromCompletableFuture(sumFuture.toCompletableFuture()).onComplete(tryInt ->
            Match(tryInt).of(
                    Case($Success($()), sum -> run(() -> println("The sum of all elements is: " + sum))),
                    Case($Failure($()), ex  -> run(() -> println("The sum of all elements could not be computed" + ex)))
            )
        );
    }

    @Test
    public void choosingMaterializedValues() {
        final Source<Integer, NotUsed>              simpleSource = Source.range(1, 10);
        final Flow<Integer, Integer, NotUsed>       simpleFlow = Flow.fromFunction(x -> x + 1);
        final Sink<Integer, CompletionStage<Done>>  simpleSink = Sink.foreach(API::println);

        // En utilisant viaMat à la place de via, on peut utiliser une fonction qui permet de choisir
        // la valeur matérialisée que l'on veut retourner pour ce composant (la nouvelle source créé)
        //simpleSource.viaMat(simpleFlow, (sourceMat, flowMat) -> flowMat);
        // on peut écrire aussi :
        //simpleSource.viaMat(simpleFlow, Keep.right()); // Keep.left(), Keep.both()...

        // En choisissant Keep.right deux fois, le graph va retourner le type de retour de simpleSink
        // cad un CompletionStage de Done
        RunnableGraph<CompletionStage<Done>> graph = simpleSource.viaMat(simpleFlow, Keep.right())
                                                                 .toMat(simpleSink, Keep.right());

        Future.fromCompletableFuture(graph.run(mat).toCompletableFuture()).onComplete(__ ->
                Match(__).of(
                        Case($Success($()), ___ -> run(() -> println("Stream processing finished."))),
                        Case($Failure($()), ex  -> run(() -> println("stream processing failed with: " + ex)))
                )
        );
    }

    public void sugars() {
        // On peut s'épargner d'utiliser le Keep.right() avec runWith qui équivaut à : toMat(sink)(Keep.right).run()
        final CompletionStage<Integer> sum1 = Source.range(1, 10).runWith(Sink.reduce((a, b) -> a + b), mat);
        // Ou encore
        final CompletionStage<Integer> sum2 = Source.range(1, 10).runReduce((a, b) -> a + b, mat);

        // Au lieu d'utiliser la syntaxe: source(..).to(sink...).run()
        // qui par défaut retourne la valeur matérialisée de la source, on peut écrire :
        // sink(..).runWith(source..)s
        // En utilisant runWith, la valeur matérialisée est celle du sink au lieu de la source
        final NotUsed notUsed = Sink.<Integer>foreach(API::println).runWith(Source.single(42), mat);

        final Source<Integer, NotUsed>              simpleSource = Source.range(1, 10);
        final Sink<Integer, CompletionStage<Done>>  simpleSink = Sink.foreach(API::println);
        // On peut également connecter à un flow à une source et un sink de cette façon:
        Flow.<Integer>create().map(x -> 2 * x).runWith(simpleSource, simpleSink, mat);
    }

    /**
     * - return the last element out of a source (use Sink.last)
     * - compute the total word count out of a stream of sentences
     *   - map, fold, reduce
     */
    @Test
    public void exercise() {
        final CompletionStage<Integer> lastElFut1 = Source.range(1, 10).toMat(Sink.last(), Keep.right()).run(mat);
        final Integer lastEl1 = lastElFut1.toCompletableFuture().join();
        println(lastEl1);

        final CompletionStage<Integer> lastElFut2 = Source.range(1, 10).runWith(Sink.last(), mat);
        final Integer lastEl2 = lastElFut2.toCompletableFuture().join();
        println(lastEl2);

        Source.from(List.of(
            "Akka is awesome",
            "I love streams",
            "Materialized values are killing me"
        ));
        
    }
}
