package rockthejvm.part2_primer;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.*;
import io.vavr.API;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static io.vavr.API.*;
import static io.vavr.Patterns.$Failure;
import static io.vavr.Patterns.$Success;
import static io.vavr.concurrent.Future.fromCompletableFuture;
import static java.time.temporal.ChronoUnit.MILLIS;
import static libs.Await.await;

/**
 * <h1>Materializing Streams</h1>
 *
 * <p>L'objectif de cette classe est d'apprendre à "extraire" des valeurs
 * d'un stream en cours d'exécution.</p>
 *
 * <p>Un graphe est statique tant qu'il n'a pas été exécuté via la méthode run</p>
 *
 * <p><code>val graph = source.via(flow).to(sink)</code></p>
 * <p><code>val result = graph.run()</code></p>
 *
 * <p>On appelle la valeur retournée par l'exécution d'un graphe
 * sa valeur matérialisée (result).Matérialiser un graphe implique de matérialiser
 * tous les composants de ce graphe.</p>
 */
public class MaterializingStreams {

    ActorSystem system;
    ActorMaterializer mat;

    @Before
    public void setup() {
        system = ActorSystem.create("MaterializingStreams");
        mat = ActorMaterializer.create(system);
    }

    /**
     * La valeur retournée par l'appel à la methode run est la valeur matérialisée du graphe.
     */
    @Test
    public void simpleGraph() {
        // Par défaut le graphe retourne la valeur matérialisée de l'élément de gauche (à gauche de la méthode to(..)),
        // cad la valeur de la source, ici NotUsed
        final RunnableGraph<NotUsed> leftValGraph = Source.range(1, 10).to(Sink.foreach(API::println));
        // Dans ce cas la valeur matérialisée par ce graphe est NotUsed, équivalent à Void ou Unit en scala
        final NotUsed leftMaterializedValue = leftValGraph.run(mat);

        // On peut également choisir de conserver la valeur matérialisée de droite (à droite de la méthode toMat(...) comme ceci:
        final RunnableGraph<CompletionStage<Done>> rightValGraph = Source.range(1, 10).toMat(Sink.foreach(API::println), Keep.right());
        final CompletionStage<Done> rightMaterializedValue = rightValGraph.run(mat);
    }

    /**
     * Au lieu de NotUsed on peut retourner une valeur suite à l'exécution d'un graphe.
     */
    @Test
    public void firstMaterializedValue() {
        final Source<Integer, NotUsed> source = Source.range(1, 10);
        // Une fois que ce sink aura fini, il exposera une future de integer representant la somme des éléments
        final Sink<Integer, CompletionStage<Integer>> sink = Sink.reduce(Integer::sum);
        // En exécutant le graphe, on va avoir accès à la somme des éléments calculés par le sink
        final CompletionStage<Integer> sumFuture = source.runWith(sink, mat);

        fromCompletableFuture(sumFuture.toCompletableFuture()).onComplete(tryInt ->
            Match(tryInt).of(
                    Case($Success($()), sum -> run(() -> println("The sum of all elements is: " + sum))),
                    Case($Failure($()), ex  -> run(() -> println("The sum of all elements could not be computed: " + ex)))
            )
        );

        await(1500, MILLIS);
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

        fromCompletableFuture(graph.run(mat).toCompletableFuture()).onComplete(__ ->
                Match(__).of(
                        Case($Success($()), ___ -> run(() -> println("Stream processing finished."))),
                        Case($Failure($()), ex  -> run(() -> println("stream processing failed with: " + ex)))
                )
        );

        await(1500, MILLIS);
    }

    public void sugars() {
        // On peut s'épargner d'utiliser le Keep.right() avec runWith qui équivaut à : toMat(sink)(Keep.right).run()
        final CompletionStage<Integer> sum1 = Source.range(1, 10).runWith(Sink.reduce(Integer::sum), mat);
        // Ou encore
        final CompletionStage<Integer> sum2 = Source.range(1, 10).runReduce(Integer::sum, mat);

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


    @Test
    public void exercise() {
        // Pour renvoyer le dernier élément d'une source, on peut utiliser Sink.last()
        final CompletableFuture<Integer> f1 = Source.range(1, 10).toMat(Sink.last(), Keep.right()).run(mat).toCompletableFuture();
        final CompletableFuture<Integer> lastElFut2 = Source.range(1, 10).runWith(Sink.last(), mat).toCompletableFuture();

        println(f1.join());
        println(lastElFut2.join());

        // On veut maintenant compter le nombre de mots dans un flux de phrases.
        // on peut utiliser les opérateurs suivants :
        // - map    : disponible sur un Flow
        // - fold   : disponible sur un Flow et un Sink
        // - reduce : disponible sur un Flow et un Sink
        final Source<String, NotUsed> sentenceSource = Source.from(List.of(
                "Akka is awesome",
                "I love streams",
                "Materialized values are killing me"
        ));

        final Sink<String, CompletionStage<Integer>> wordCountSink =
                Sink.fold(0, (currentWords, newSentence) -> currentWords + newSentence.split(" ").length);

        final CompletableFuture<Integer> g1 = sentenceSource.toMat(wordCountSink, Keep.right()).run(mat).toCompletableFuture();
        println("Nb de mots dans le flux: " + g1.join());

        // ce qui est équivalent à :
        final CompletionStage<Integer> g2 = sentenceSource.runWith(wordCountSink, mat);
        final CompletionStage<Integer> g3 = sentenceSource.runFold(0, (currentWords, newSentence) -> currentWords + newSentence.split(" ").length, mat);

        // Avec un Flow on peut écrire :
        final Flow<String, Integer, NotUsed> wordCountFlow =
                Flow.<String>create().fold(0, (currentWords, newSentence) -> currentWords + newSentence.split(" ").length);

        final CompletableFuture<Integer> g4 = sentenceSource.via(wordCountFlow).toMat(Sink.head(), Keep.right()).run(mat).toCompletableFuture();
        final CompletableFuture<Integer> g5 = sentenceSource.viaMat(wordCountFlow, Keep.left()).toMat(Sink.head(), Keep.right()).run(mat).toCompletableFuture();
        final CompletableFuture<Integer> g6 = sentenceSource.via(wordCountFlow).runWith(Sink.head(), mat).toCompletableFuture();
        final CompletableFuture<Integer> g7 = wordCountFlow.runWith(sentenceSource, Sink.head(), mat).second().toCompletableFuture();
    }

    /**
     * <h1>Recap</h1>
     *
     * <b>Materializing a graph = materializing <i>all</i> components</b>
     * <ul>
     * <li>each component produces a materialized value when run</li>
     * <li>the graph produces a <u>single</u> materialized value</li>
     * <li>our job to choose which one to pick</li>
     *</ul>
     *
     * <b>A component can materialize multiple times</b>
     * <p></p>
     *
     * <h2><font color="red">A materialized value can ben ANYTHING</font></h2>
     */
    public void recap(){}
}
