package yelp.scraping;

import io.vavr.control.Try;
import org.apache.commons.collections4.IteratorUtils;
import play.libs.Json;
import scala.collection.JavaConverters;
import scala.io.Codec;
import scala.io.Source;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static io.vavr.API.println;

public class ExistingPostcodes {

    public static Set<String> load(Path path) {

        return Try.of(() -> Source.fromFile(path.toFile(), Codec.defaultCharsetCodec()))
                .map(__ -> IteratorUtils.toList(JavaConverters.asJavaIterator(__.getLines()
                            .map(Json::parse)
                            .map(json -> json.findPath("postcode").asText())))
                            .stream().collect(Collectors.toSet())
                )
                .recover(throwable -> Collections.emptySet())
                .get();
    }

    public static void main(String[] args) {
        final Path path = Paths.get("postcode_restaurants.json");

        final Set<String> load = ExistingPostcodes.load(path);

        println(load.size());
    }


}
