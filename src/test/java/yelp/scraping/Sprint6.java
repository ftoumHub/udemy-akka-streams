package yelp.scraping;

import akka.NotUsed;
import akka.actor.ActorRef;
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
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static akka.pattern.PatternsCS.ask;
import static io.vavr.API.*;

public class Sprint6 {

    private static final Path outputPath = Paths.get("postcode_restaurants.json");
    private static final Integer parallelismLevel = 2; // Number of concurrent threads to use to query the Yelp API
    private static long maxErrors = 10; // Stop the stream after seeing this many error codes

    /**
     * Conf de AhcWsClient voir : https://www.playframework.com/documentation/2.7.x/JavaWS
     */
    public static void main(String[] args) throws IOException {
        // Instantiate an actor system and materializer
        String name = "Sprint6";
        ActorSystem system = ActorSystem.create(name);
        ActorMaterializerSettings settings = ActorMaterializerSettings.create(system);
        ActorMaterializer materializer = ActorMaterializer.create(settings, system, name);

        // We need a web service client for querying the API
        AhcWSClient ws = new AhcWSClient(
                StandaloneAhcWSClient.create(
                        AhcWSClientConfigFactory.forConfig(ConfigFactory.load(), system.getClass().getClassLoader()),
                        materializer), materializer);

        Function<PostcodeRestaurants, ObjectNode> serializePostcodeRestaurant = postcodeRestaurants -> {
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectNode objectNode = objectMapper.createObjectNode();
            objectNode.put("postcode", postcodeRestaurants.getPostcode());
            objectNode.put("fetch_time", LocalDateTime.now().toString());
            objectNode.put("data", objectMapper.valueToTree(postcodeRestaurants.getRestaurants().toJavaList()));

            return objectNode;
        };

        final Set<StandardOpenOption> outputOpenOptions =
                Set(StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND).toJavaSet();

        final Sink<PostcodeRestaurants, CompletionStage<IOResult>> postcodeResponseSerializer =
                Flow.<PostcodeRestaurants>create()
                        .map(p -> serializePostcodeRestaurant.apply(p))
                        .map(objNode -> ByteString.fromString(objNode.toString() + '\n'))
                        .toMat(FileIO.toPath(outputPath, outputOpenOptions), Keep.right());

        final Flow<Tuple2<String, WSResponse>, Tuple2<String, WSResponse>, NotUsed> errorLimiter =
                Flow.<Tuple2<String, WSResponse>>create().limitWeighted(maxErrors,
                        postcodeWithResponse ->
                                (postcodeWithResponse._2.getStatus() == 200 || postcodeWithResponse._2.getStatus() == 429) ? 0L : 1L);

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
        println(String.format("Found %s unique postcodes.", allPostcodes.size()));

        // Load the list of postcodes we have already processed
        Set<String> donePostcodes = ExistingPostcodes.load(outputPath);
        println(String.format("Found %s already processed.", donePostcodes.size()));

        // Filter the list of postcodes
        List<String> remainingPostcodes = allPostcodes.stream().filter(not(p -> donePostcodes.contains(p))).collect(Collectors.toList());
        println(String.format("There are %s still to do.", remainingPostcodes.size()));

        final ActorRef throttler = system.actorOf(Throttler.props());

        // Use `remainingPostcodes` in our stream
        Source<PostcodeRestaurants, NotUsed> postcodeResponses =
                Source.from(remainingPostcodes).take(30000)
                .mapAsync(parallelismLevel, postcode -> {
                        ask(throttler, Throttler.WantToPass.class, Duration.ofHours(2));
                        return YelpApi.fetchPostcode(ws, postcode).map(response -> Tuple.of(postcode, response)).toCompletableFuture();
                    }
                )
                .via(throttlerNotifier(throttler))
                .via(StreamMonitor.monitor(100, count -> println(String.format("Processed %s restaurants", count)), system))
                .via(errorLogger)
                //.via(errorLimiter)
                .filter(postcodeWithResp -> postcodeWithResp._2.getStatus() == 200)
                .map(successfulResp -> {
                    final List<JsonNode> restaurants = YelpApi.parseSuccessfulResponse(successfulResp._1, successfulResp._2);
                    return new PostcodeRestaurants(successfulResp._1, io.vavr.collection.List.ofAll(restaurants));
                });

        final CompletableFuture<IOResult> ioResultCompletableFuture = postcodeResponses.runWith(postcodeResponseSerializer, materializer).toCompletableFuture();

        ioResultCompletableFuture.completeOnTimeout(ioResultCompletableFuture.join(), 10, TimeUnit.MINUTES);

        // clean up
        ws.close();
        materializer.shutdown();
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
