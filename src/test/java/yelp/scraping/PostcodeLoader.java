package yelp.scraping;

import play.libs.Json;
import scala.collection.JavaConverters;
import scala.io.Source;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static io.vavr.API.println;
import static java.util.stream.Collectors.toList;

public class PostcodeLoader {

    private static final String postcodeUrl = "http://data.scala4datascience.com/restaurants/restaurants.json";

    public static List<String> load(){
        println("==> Calling : " + postcodeUrl);
        return JavaConverters.seqAsJavaList(Source.fromURL(postcodeUrl, StandardCharsets.UTF_8.name()).getLines()
                .map(Json::parse)
                .map(json -> json.findPath("postcode").asText())
                .filter(postcode -> postcode.length() > 1)
                .map(Postcode::normalize)
                .filter(postcode -> postcode.chars().allMatch(Character::isLetterOrDigit))
                .toList())
                .stream()
                .sorted()
                .distinct()
                .collect(toList());
    }

}
