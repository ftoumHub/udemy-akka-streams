package yelp.scraping;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.ActorMaterializerSettings;
import akka.stream.IOResult;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.ConfigFactory;
import play.libs.ws.ahc.AhcWSClient;
import play.libs.ws.ahc.AhcWSClientConfigFactory;
import play.libs.ws.ahc.StandaloneAhcWSClient;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import static io.vavr.API.Set;
import static io.vavr.API.Tuple;
import static io.vavr.API.println;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.time.LocalDateTime.now;

/**
 * Sprint 2 – serializing results
 */
public class Sprint2 {

    // Instantiate an actor system and materializer
    private static final String name = "Sprint2";
    private static final ActorSystem system = ActorSystem.create(name);
    private static final ActorMaterializerSettings settings = ActorMaterializerSettings.create(system);
    private static final ActorMaterializer mat = ActorMaterializer.create(settings, system, name);

    private static final Path outputPath = Paths.get("postcode_restaurants.json");

    private static final Set<StandardOpenOption> options = Set(CREATE, WRITE).toJavaSet();

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

        final Function<PostcodeRestaurants, ObjectNode> serializePostcodeRestaurant = postcodeRestaurants -> {
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectNode objNode = objectMapper.createObjectNode();
            objNode.put("postcode", postcodeRestaurants.getPostcode());
            objNode.put("fetch_time", now().toString());
            objNode.put("data", objectMapper.valueToTree(postcodeRestaurants.getRestaurants().toJavaList()));
            return objNode;
        };

        final Sink<PostcodeRestaurants, CompletionStage<IOResult>> postcodeResponseSerializer =
                Flow.<PostcodeRestaurants>create()
                        .map(serializePostcodeRestaurant::apply)
                        .map(objNode -> ByteString.fromString(objNode.toString() + '\n'))
                        .toMat(FileIO.toPath(outputPath, options), Keep.right());

        List<String> postcodes = PostcodeLoader.load(); // Load the list of postcodes to query
        println("postcodes size : " + postcodes.size());

        Source.from(postcodes)
                .take(100)
                .mapAsync(8,
                        postcode ->
                                YelpApi.fetchPostcode(ws, postcode)
                                        .map(response -> Tuple(postcode, response)).toCompletableFuture())
                .filter(postcodeWithResp -> postcodeWithResp._2.getStatus() == 200)
                .map(successfulResp -> {
                    final List<JsonNode> restaurants = YelpApi.parseSuccessfulResponse(successfulResp._1, successfulResp._2);
                    return new PostcodeRestaurants(successfulResp._1, restaurants);
                })
                .runWith(postcodeResponseSerializer, mat);

        Thread.sleep(10000); // give the stream time to run

        ws.close();
        mat.shutdown();
        system.terminate();
        //Await.ready(system.terminate(), Duration.ofSeconds(5));
    }
}
