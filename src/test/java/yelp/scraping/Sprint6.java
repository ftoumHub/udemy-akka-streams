package yelp.scraping;

import akka.NotUsed;
import akka.actor.ActorRef;
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
import io.vavr.Tuple2;
import play.libs.ws.WSResponse;
import play.libs.ws.ahc.AhcWSClient;
import play.libs.ws.ahc.AhcWSClientConfigFactory;
import play.libs.ws.ahc.StandaloneAhcWSClient;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Predicate;

import static akka.pattern.PatternsCS.ask;
import static io.vavr.API.Set;
import static io.vavr.API.Tuple;
import static io.vavr.API.printf;
import static io.vavr.API.println;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;

/**
 * Sprint 6 – throttling to avoid rate limits
 * <p>
 * To query the API for every postcode in our dataset, we could just run the program we have
 * built so far several times, letting it crash every time it exceeds the rate limit. We would want
 * to wait between restarts for our request quota to regenerate. However, we can do better: we
 * can throttle the stream when we start receiving 429 responses.
 */
public class Sprint6 {

    // Instantiate an actor system and materializer
    private static final String name = "Sprint6";
    private static final ActorSystem system = ActorSystem.create(name);
    private static final ActorMaterializerSettings settings = ActorMaterializerSettings.create(system);
    private static final ActorMaterializer mat = ActorMaterializer.create(settings, system, name);

    private static final Path outputPath = Paths.get("postcode_restaurants.json");
    private static final Integer parallelismLevel = 2; // Number of concurrent threads to use to query the Yelp API
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
                        postcodeWithResponse ->
                                (postcodeWithResponse._2.getStatus() == 200
                                 || postcodeWithResponse._2.getStatus() == 429)
                                        ? 0L : 1L);

        final Flow<Tuple2<String, WSResponse>, Tuple2<String, WSResponse>, NotUsed> errorLogger =
                Flow.<Tuple2<String, WSResponse>>create().map(postcodeWithResponse -> {
                    if (postcodeWithResponse._2.getStatus() != 200) {
                        println("Non 200 response for postcode " + postcodeWithResponse._1
                                + ": [status: " + postcodeWithResponse._2.getStatus()
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

        final ActorRef throttler = system.actorOf(Throttler.props());

        // Use `remainingPostcodes` in our stream
        final CompletableFuture<IOResult> ioResultCompletableFuture =
                Source.from(remainingPostcodes)
                        .take(30000)
                        .mapAsync(parallelismLevel,
                                postcode -> {
                                    ask(throttler, Throttler.WantToPass.class, Duration.ofHours(2));
                                    return YelpApi.fetchPostcode(ws, postcode)
                                            .map(response -> Tuple(postcode, response))
                                            .toCompletableFuture();
                                }
                        )
                        .via(throttlerNotifier(throttler))
                        .via(StreamMonitor.monitor(5, count -> printf("Processed %s restaurants\n", count), system))
                        .via(errorLogger)
                        .via(errorLimiter)
                        .filter(postcodeWithResp -> postcodeWithResp._2.getStatus() == 200)
                        .map(successfulResp -> {
                            final List<JsonNode> restaurants = YelpApi.parseSuccessfulResponse(successfulResp._1, successfulResp._2);
                            return new PostcodeRestaurants(successfulResp._1, restaurants);
                        })
                        .runWith(postcodeResponseSerializer, mat).toCompletableFuture();

        ioResultCompletableFuture.completeOnTimeout(ioResultCompletableFuture.join(), 10, MINUTES);

        // clean up
        ws.close();
        mat.shutdown();
        system.terminate();
        //Await.ready(system.terminate(), Duration.ofSeconds(5));
    }

    private static Flow<Tuple2<String, WSResponse>, Tuple2<String, WSResponse>, NotUsed> throttlerNotifier(ActorRef throttler) {
        return Flow.<Tuple2<String, WSResponse>>create().map(postcodeWithResponse -> {
            if (postcodeWithResponse._2.getStatus() == 429) {
                throttler.tell(Throttler.RequestLimitExceeded.class, ActorRef.noSender());
            }
            return postcodeWithResponse;
        });
    }

    public static <R> Predicate<R> not(Predicate<R> predicate) {
        return predicate.negate();
    }
}
