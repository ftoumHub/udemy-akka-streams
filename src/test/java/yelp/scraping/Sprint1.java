package yelp.scraping;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.ActorMaterializerSettings;
import akka.stream.javadsl.Source;
import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.ConfigFactory;
import play.libs.ws.ahc.AhcWSClient;
import play.libs.ws.ahc.AhcWSClientConfigFactory;
import play.libs.ws.ahc.StandaloneAhcWSClient;

import java.io.IOException;
import java.util.List;

import static io.vavr.API.Tuple;
import static io.vavr.API.println;

/**
 * https://pascalbugnion.net/blog/scraping-apis-with-akka-streams.html
 *
 * Sprint 1 – exploring the API
 */
public class Sprint1 {

    // Instantiate an actor system and materializer
    private static final String name = "Sprint1";
    private static final ActorSystem system = ActorSystem.create(name);
    private static final ActorMaterializerSettings settings = ActorMaterializerSettings.create(system);
    private static final ActorMaterializer mat = ActorMaterializer.create(settings, system, name);

    // We need a web service client for querying the API
    private static final AhcWSClient ws = new AhcWSClient(
            StandaloneAhcWSClient.create(
                    AhcWSClientConfigFactory.forConfig(ConfigFactory.load(), system.getClass().getClassLoader()),
                    mat),
            mat);

    /**
     * Conf de AhcWsClient voir : https://www.playframework.com/documentation/2.7.x/JavaWS
     */
    public static void main(String[] args) throws InterruptedException, IOException {

        // Load the list of postcodes to query
        List<String> postcodes = PostcodeLoader.load();
        println("Postcodes loaded : " + postcodes.size());
        //postcodes.forEach(API::println);

        Source.from(postcodes)
                .take(100)
                .mapAsync(
                        8, // Number of concurrent threads to use to query the Yelp API
                        postcode -> YelpApi.fetchPostcode(ws, postcode)
                                .map(response -> Tuple(postcode, response))
                                .toCompletableFuture())
                .filter(postcodeWithResp -> postcodeWithResp._2.getStatus() == 200)
                .map(successfulResp -> {
                    String postcode = successfulResp._1;
                    final List<JsonNode> restaurants = YelpApi.parseSuccessfulResponse(postcode, successfulResp._2);
                    return new PostcodeRestaurants(postcode, restaurants);
                })
                .runForeach(pR -> println(pR.getRestaurants()), mat)
                .whenComplete((d, t) -> println("Done"));

        Thread.sleep(10000); // give the stream time to run

        ws.close();
        system.terminate();
    }
}
