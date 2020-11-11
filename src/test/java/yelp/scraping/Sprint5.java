package yelp.scraping;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.ActorMaterializerSettings;
import akka.stream.IOResult;
import akka.stream.javadsl.*;
import akka.util.ByteString;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.ConfigFactory;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import play.libs.ws.WSResponse;
import play.libs.ws.ahc.AhcWSClient;
import play.libs.ws.ahc.AhcWSClientConfigFactory;
import play.libs.ws.ahc.StandaloneAhcWSClient;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.vavr.API.*;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;

/**
 * Sprint 5 – exploring failure responses
 *
 * We now have a program that will serialize responses from the Yelp API. Our program can
 * recover from crashes, since it only tries to query postcodes it has not serialized yet.
 *
 * We have, so far, ignored error responses from Yelp. This can lead to pathological behaviour if
 * we let the program run for a long time: the Yelp API (like most APIs) has a request limit.
 * Exceeding this request limit repeatedly can, presumably, get you banned. There may also be
 * other error responses that we must be aware of. We need to better understand how the Yelp
 * API responds when it does not like our query.
 */
public class Sprint5 {

    private static final String name = "Sprint5";
    private static final ActorSystem system = ActorSystem.create(name);
    private static final ActorMaterializerSettings settings = ActorMaterializerSettings.create(system);
    private static final ActorMaterializer mat = ActorMaterializer.create(settings, system, name);

    private static final Path outputPath = Paths.get("postcode_restaurants.json");
    private static final Integer parallelismLevel = 8; // Number of concurrent threads to use to query the Yelp API
    private static long maxErrors = 10; // Stop the stream after seeing this many error codes

    private static final Set<StandardOpenOption> options = Set(CREATE, WRITE, APPEND).toJavaSet();

    /**
     * Conf de AhcWsClient voir : https://www.playframework.com/documentation/2.7.x/JavaWS
     */
    public static void main(String[] args) throws IOException {

        // We need a web service client for querying the API
        AhcWSClient ws = new AhcWSClient(
                StandaloneAhcWSClient.create(
                        AhcWSClientConfigFactory.forConfig(ConfigFactory.load(), system.getClass().getClassLoader()),
                        mat),
                mat);

        Function<PostcodeRestaurants, ObjectNode> serializePostcodeRestaurant = postcodeRestaurants -> {
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectNode objectNode = objectMapper.createObjectNode();
            objectNode.put("postcode", postcodeRestaurants.getPostcode());
            objectNode.put("fetch_time", LocalDateTime.now().toString());
            objectNode.put("data", objectMapper.valueToTree(postcodeRestaurants.getRestaurants().toJavaList()));

            return objectNode;
        };

        final Sink<PostcodeRestaurants, CompletionStage<IOResult>> postcodeResponseSerializer =
                Flow.<PostcodeRestaurants>create()
                        .map(serializePostcodeRestaurant::apply)
                        .map(objNode -> ByteString.fromString(objNode.toString() + '\n'))
                        .toMat(FileIO.toPath(outputPath, options), Keep.right());

        final Flow<Tuple2<String, WSResponse>, Tuple2<String, WSResponse>, NotUsed> errorLimiter =
                Flow.<Tuple2<String, WSResponse>>create().limitWeighted(maxErrors,
                        postcodeWithResponse -> postcodeWithResponse._2.getStatus() == 200 ? Long.valueOf(0L) : Long.valueOf(1L));

        final Flow<Tuple2<String, WSResponse>, Tuple2<String, WSResponse>, NotUsed> errorLogger =
                Flow.<Tuple2<String, WSResponse>>create().map(postcodeWithResponse -> {
                    if (postcodeWithResponse._2.getStatus() != 200) {
                        println("Non 200 response for postcode " + postcodeWithResponse._1 + ": [status: " + postcodeWithResponse._2.getStatus()
                                + ", body: " + postcodeWithResponse._2.getBody());
                    }
                    return postcodeWithResponse;
                });

        // Load the list of postcodes to query
        List<String> allPostcodes = PostcodeLoader.load();
        printf("Found %s unique postcodes.\n", allPostcodes.size());

        // Load the list of postcodes we have already processed
        Set<String> donePostcodes = ExistingPostcodes.load(outputPath);
        printf("Found %s already processed.\n", donePostcodes.size());

        // Filter the list of postcodes
        List<String> remainingPostcodes = allPostcodes.stream().filter(not(donePostcodes::contains)).collect(toList());
        printf("There are %s still to do.\n", remainingPostcodes.size());

        // Use `remainingPostcodes` in our stream
        final CompletableFuture<IOResult> ioResultCompletableFuture =
                Source.from(remainingPostcodes)
                        .take(100)
                        .mapAsync(parallelismLevel,
                                postcode -> YelpApi.fetchPostcode(ws, postcode)
                                        .map(response -> Tuple(postcode, response))
                                        .toCompletableFuture())
                        .via(StreamMonitor.monitor(5, count -> printf("Processed %s restaurants\n", count), system))
                        .via(errorLogger)
                        .via(errorLimiter)
                        .filter(postcodeWithResp -> postcodeWithResp._2.getStatus() == 200)
                        .map(successfulResp -> {
                            final List<JsonNode> restaurants = YelpApi.parseSuccessfulResponse(successfulResp._1, successfulResp._2);
                            return new PostcodeRestaurants(successfulResp._1, restaurants);
                        })
                        .runWith(postcodeResponseSerializer, mat)
                        .toCompletableFuture();

        ioResultCompletableFuture.completeOnTimeout(ioResultCompletableFuture.join(), 10, MINUTES);

        // clean up
        ws.close();
        mat.shutdown();
        system.terminate();
        //Await.ready(system.terminate(), Duration.ofSeconds(5));
    }

    public static <R> Predicate<R> not(Predicate<R> predicate) {
        return predicate.negate();
    }
}
