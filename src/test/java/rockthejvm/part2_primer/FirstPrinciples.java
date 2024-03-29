package rockthejvm.part2_primer;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import io.vavr.API;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import libs.Flows;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static io.vavr.API.List;
import static io.vavr.API.println;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.nonNull;
import static libs.Await.await;

public class FirstPrinciples {

    ActorSystem system;
    ActorMaterializer mat;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        system = ActorSystem.create("FirstPrinciples");
        mat = ActorMaterializer.create(system);
    }

    @Test
    public void firstSource() {
        // Une source qui émet tous les éléments de 1 à 10.
        final Source<Integer, NotUsed> source = Source.range(1, 10);
        // Un sink qui va afficher chaque valeur reçue
        final Sink<Integer, CompletionStage<Done>> sink = Sink.foreach(API::println);
        // L'expression source to sink défini un graphe
        final RunnableGraph<NotUsed> graph = source.to(sink);
        // le graphe ne fait rien tant qu'on appelle pas la méthode run.
        graph.run(mat);

        await(500, MILLIS);
    }

    @Test
    public void sourceFlatMapConcat() {
        // On a 2 sources:
        final Source<Integer, NotUsed> src1 = Source.range(1, 3);
        final Source<Integer, NotUsed> src2 = Source.range(1, 10);

        final Sink<Integer, CompletionStage<Done>> sink = Sink.foreach(API::println);

        // Pour chaque element de src1 on émet src2
        final Source<Integer, NotUsed> flatMappedSources = src1.flatMapConcat(i -> src2);
        flatMappedSources.to(sink).run(mat);

        await(500, MILLIS);
    }

    @Test
    public void flatMapConcatVsFlatMapMerge() {
        //final List<String> alphabet = List("A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z");
        final List<String> alphabet = List("A","B","C","D","E");

        println("========== FlatMapConcat ==========");
        Source.from(alphabet)
                .flatMapConcat(letter -> {
                    println(letter);
                    return Source.range(1, alphabet.indexOf(letter) + 1);
                })
                .to(Sink.foreach(API::println))
                .run(mat);

        await(500, MILLIS);

        // pb, si un des sous stream est infini, on ne passe jamais à l'élément suivant de la première source.
        // Avec flatMapMerge, les sous streams sont exécutés en parallèle avec un facteur de parallélisation.

        println("========== FlatMapMerge ==========");
        Source.from(alphabet)
                .flatMapMerge(2, letter -> {
                    println(letter);
                    return Source.range(1, alphabet.indexOf(letter) + 1);
                })
                .to(Sink.foreach(API::println))
                .run(mat);

        await(500, MILLIS);
    }

    @Test
    public void sourceConcat() {

        final Source<Integer, NotUsed> unDeux = Source.from(List.of(1, 2));
        final Source<Integer, NotUsed> trois = Source.single(3);

        final Sink<Integer, CompletionStage<Done>> sink = Sink.foreach(API::println);

        unDeux.concat(trois)
                .watchTermination((nu, whenDone) ->
                        whenDone.whenComplete((d, e) -> {
                            if (nonNull(e)) e.printStackTrace();
                            else println("concat step done");
                        })
                )
                .map(i -> i * 2)
                .watchTermination((nu, whenDone) ->
                        whenDone.whenComplete((d, e) -> {
                            if (nonNull(e)) e.printStackTrace();
                            else println("map step done");
                        })
                )
                .to(sink)
                .run(mat)
        .whenComplete((d, e) -> {
            if (nonNull(e)) e.printStackTrace();
            else println("End Source");
        });

        await(500, MILLIS);
    }

    @Test
    public void flowsTransformElements() {
        final Source<Integer, NotUsed> source = Source.range(1, 10);

        final Flow<Integer, Integer, NotUsed> flow = Flow.fromFunction(x -> x + 1);

        final Sink<Integer, CompletionStage<Done>> sink = Sink.foreach(API::println);

        // une source connectée à un flow retourne une nouvelle source
        final Source<Integer, NotUsed> sourceWithFlow = source.via(flow);

        final Sink<Integer, NotUsed> flowWithSink = flow.to(sink);

        // equivalent
        sourceWithFlow.to(sink).run(mat);
        source.to(flowWithSink).run(mat);
        source.via(flow).to(sink).run(mat);

        await(1500, MILLIS);
    }

    @Test
    public void nullsAreNotAllowed() {
        thrown.expect(NullPointerException.class);

        final Source<Object, NotUsed> illegalSource = Source.single(null);
        illegalSource.to(Sink.foreach(API::println)).run(mat);
        // use Options instead
    }

    @Test
    public void variousKindOfSources() {
        final Source<Integer, NotUsed> finiteSource = Source.single(1);
        final Source<Integer, NotUsed> anotherFiniteSource = Source.from(List(1, 2, 3));
        final Source<Integer, NotUsed> emptySource = Source.empty();
        // do not confuse an Akka stream with a "collection" Stream
        final Source<Integer, NotUsed> infiniteSource = Source.from(Stream.from(1));
        // On peut aussi créer une source à partir d'autres choses, ex: une future
        final Source<Integer, NotUsed> futureSource = Source.fromCompletionStage(CompletableFuture.completedFuture(42));
    }

    @Test
    public void sinks() {
        final Sink<Object, CompletionStage<Done>> theMostBoringSink = Sink.ignore();
        final Sink<String, CompletionStage<Done>> foreachSink = Sink.foreach(API::println);
        // retrieves head and then closes the stream
        final Sink<Integer, CompletionStage<Integer>> headSink = Sink.head();
        // this sink is able to compute the sum of all the elements that are passed into it.
        final Sink<Integer, CompletionStage<Integer>> foldSink = Sink.fold(0, (a, b) -> a + b);
    }

    @Test
    public void flows() {
        // flows - usually mapped to collection operators
        final Flow<Integer, Integer, NotUsed> mapFlow = Flow.<Integer>create().map(x -> 2 * x);
        final Flow<Integer, Integer, NotUsed> takeFlow = Flow.<Integer>create().take(5);
        // drop, filter
        // NOT have flatMap

        final Source<Integer, NotUsed> source = Source.range(1, 10);
        final Sink<Integer, CompletionStage<Done>> sink = Sink.foreach(API::println);

        // source -> flow -> flow -> ... -> sink
        final RunnableGraph<NotUsed> doubleFlowGraph = source.via(mapFlow).via(takeFlow).to(sink);
        doubleFlowGraph.run(mat);

        // syntactic sugars
        //Source.range(1, 10).via(Flow.<Integer>create().map(x -> 2 * x));
        final Source<Integer, NotUsed> mapSource = Source.range(1, 10).map(x -> 2 * x);

        // run streams directly
        //mapSource.to(Sink.foreach(API::println)).run(mat);
        mapSource.runForeach(API::println, mat);

        // OPERATORS = components
    }

    @Test
    public void groupingFlows() {

        final Source<Integer, NotUsed> source = Source.range(1, 14);

        final Flow<Integer, List<Integer>, NotUsed> groupedIntegers = Flows.groupFlow(4);

        source.via(groupedIntegers).runWith(Sink.foreach(i -> println(i.size())), mat);

        await(500, MILLIS);
    }

    /**
     * Exercise: create a stream that takes the name of persons,
     * then you will keep the first 2 names with length > 5 characters.
     */
    @Test
    public void exercise() {
        final List<String> names = List("Alice", "Bob", "Charlie", "David", "Martin", "AkkaStreams");
        final Source<String, NotUsed> nameSource = Source.from(names);

        final Flow<String, String, NotUsed> longNameFlow = Flow.<String>create().filter(s -> s.length() > 5);
        final Flow<String, String, NotUsed> limitFlow = Flow.<String>create().take(2);
        final Sink<String, CompletionStage<Done>> nameSink = Sink.foreach(API::println);

        //nameSource.via(longNameFlow).via(limitFlow).to(nameSink).run(mat);
        nameSource
                .filter(__ -> __.length() > 5)
                .take(2)
                .runForeach(n -> println(n), mat);

        await(500, MILLIS);
    }
}
