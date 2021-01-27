package rockthejvm.part5_advanced;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.SubSource;
import io.vavr.API;

import static io.vavr.API.List;
import static io.vavr.API.printf;
import static org.apache.commons.lang3.StringUtils.isEmpty;

public class Substreams {

    static ActorSystem system = ActorSystem.create("Substreams");
    static ActorMaterializer mat = ActorMaterializer.create(system);

    public static void main(String[] args) {
        // 1 - grouping a stream by a certain function
        final Source<String, NotUsed> wordsSource = Source.from(List("Akka", "is", "amazing", "learning", "substreams"));
        // la fonction de groupement prend une string et retourne une clé qui sera utilisé afin de décider
        // de quel sous stream cet élément fera partie
        final SubSource<String, NotUsed> groups = wordsSource
                .groupBy(30, word -> isEmpty(word) ? "\0" : word.toLowerCase().charAt(0));

        groups.to(Sink.fold(0, (count, word) -> {
            final int newCount = count + 1;
            printf("I just received %s, count is %s", word, count);
            return newCount;
        })).run(mat);

        // 4 - flattening
        final Source<Integer, NotUsed> simpleSource = Source.range(1, 5);

        simpleSource.flatMapConcat(x -> Source.range(x, 3 * x)).runWith(Sink.foreach(API::println), mat);
        simpleSource.flatMapMerge(2, x -> Source.range(x, 3 * x)).runWith(Sink.foreach(API::println), mat);
    }
}