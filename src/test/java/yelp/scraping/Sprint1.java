package yelp.scraping;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.ActorMaterializerSettings;
import akka.stream.javadsl.Source;
import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.ConfigFactory;
import io.vavr.API;
import io.vavr.Tuple;
import play.libs.ws.ahc.AhcWSClient;
import play.libs.ws.ahc.AhcWSClientConfigFactory;
import play.libs.ws.ahc.StandaloneAhcWSClient;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static io.vavr.API.println;

public class Sprint1 {

    /**
     * Conf de AhcWsClient voir : https://www.playframework.com/documentation/2.7.x/JavaWS
     */
    public static void main(String[] args) throws InterruptedException, IOException {
        // Instantiate an actor system and materializer
        String name = "Sprint1";
        ActorSystem system = ActorSystem.create(name);
        ActorMaterializerSettings settings = ActorMaterializerSettings.create(system);
        ActorMaterializer materializer = ActorMaterializer.create(settings, system, name);

        // We need a web service client for querying the API
        AhcWSClient ws = new AhcWSClient(
                StandaloneAhcWSClient.create(
                        AhcWSClientConfigFactory.forConfig(ConfigFactory.load(), system.getClass().getClassLoader()),
                        materializer), materializer
                );

        Integer parallelismLevel = 8; // Number of concurrent threads to use to query the Yelp API

        // Load the list of postcodes to query
        List<String> postcodes = PostcodeLoader.load();
        println("postcodes size : " + postcodes.size());
        postcodes.forEach(API::println);

        Source<PostcodeRestaurants, NotUsed> postcodeResponses =
                Source.from(postcodes).take(100)
                .mapAsync(parallelismLevel,
                        postcode -> YelpApi.fetchPostcode(ws, postcode)
                                .map(response -> Tuple.of(postcode, response)).toCompletableFuture())
                .filter(postcodeWithResp -> postcodeWithResp._2.getStatus() == 200)
                .map(successfulResp -> {
                    String postcode = successfulResp._1;
                    final List<JsonNode> restaurants = YelpApi.parseSuccessfulResponse(postcode, successfulResp._2);
                    return new PostcodeRestaurants(postcode, io.vavr.collection.List.ofAll(restaurants));
                });

        postcodeResponses.runForeach(pR -> println(pR.getRestaurants()), materializer);

        Thread.sleep(10000); // give the stream time to run

        // clean up
        ws.close();
        materializer.shutdown();
        system.terminate();
        //Await.ready(system.terminate(), Duration.ofSeconds(5));
    }
}
