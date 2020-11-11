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
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.vavr.API.Set;
import static io.vavr.API.Tuple;
import static io.vavr.API.printf;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

/**
 * Sprint 4 - Measuring Flow
 * <p>
 * We now serialize successful responses, and program crashes do not result in significant loss of work.
 * We are almost ready to run our code on a larger sample of the postcodes. One thing missing is the ability
 * to measure the progress of our program. Currently, the only way of doing this is to count the number of
 * lines in the output file. In this sprint, we integrate flow measurement into our pipeline. We will print
 * a message for every 1000 postcodes that we process.
 * <p>
 * We want to build a pipeline stage that counts the number of postcodes that flow through it and logs a
 * message every 1000 elements. Since several components of our pipeline are asynchronous, we cannot just
 * increment a variable outside the pipeline, since that might be subject to race conditions. One possible
 * solution would be to use a Java atomic integer. We will use an actor instead. The actor just counts the
 * number of messages it receives and logs a message for every 1000 messages. Since the actor processes
 * messages linearly, we avoid race conditions.
 */
public class Sprint4 {

    // Instantiate an actor system and materializer
    private static final String name = "Sprint4";
    private static final ActorSystem system = ActorSystem.create(name);
    private static final ActorMaterializerSettings settings = ActorMaterializerSettings.create(system);
    private static final ActorMaterializer mat = ActorMaterializer.create(settings, system, name);

    // We need a web service client for querying the API
    private static final AhcWSClient ws = new AhcWSClient(
            StandaloneAhcWSClient.create(
                    AhcWSClientConfigFactory.forConfig(ConfigFactory.load(), system.getClass().getClassLoader()),
                    mat),
            mat);

    private static final Set<StandardOpenOption> options = Set(CREATE, WRITE, APPEND).toJavaSet();

    /**
     * Conf de AhcWsClient voir : https://www.playframework.com/documentation/2.7.x/JavaWS
     */
    public static void main(String[] args) throws InterruptedException, IOException {

        final Path outputPath = Paths.get("postcode_restaurants.json");
        Integer parallelismLevel = 2; // Number of concurrent threads to use to query the Yelp API

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
                Source.from(remainingPostcodes).take(30)
                        .mapAsync(
                                parallelismLevel,
                                postcode ->
                                        YelpApi.fetchPostcode(ws, postcode)
                                                .map(response -> Tuple(postcode, response))
                                                .toCompletableFuture())
                        .via(StreamMonitor.monitor(5, count -> printf("Processed %s restaurants\n", count), system))
                        .filter(postcodeWithResp -> postcodeWithResp._2.getStatus() == 200)
                        .map(successfulResp -> {
                            final List<JsonNode> restaurants = YelpApi.parseSuccessfulResponse(successfulResp._1, successfulResp._2);
                            return new PostcodeRestaurants(successfulResp._1, restaurants);
                        })
                        .runWith(postcodeResponseSerializer, mat)
                        .toCompletableFuture();

        ioResultCompletableFuture.completeOnTimeout(ioResultCompletableFuture.join(), 600, SECONDS);

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
