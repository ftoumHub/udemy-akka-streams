package yelp.scraping;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.ActorMaterializerSettings;
import akka.stream.Graph;
import akka.stream.IOResult;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Partition;
import akka.stream.javadsl.Sink;
import akka.util.ByteString;
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
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import static akka.pattern.PatternsCS.ask;
import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static io.vavr.API.Set;
import static io.vavr.API.Tuple;
import static io.vavr.API.printf;
import static io.vavr.API.println;
import static io.vavr.Predicates.is;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Sprint 7 – requeuing failed postcodes
 */
public class Sprint7 {

    // Instantiate an actor system and materializer
    private static final String name = "Sprint7";
    private static final ActorSystem system = ActorSystem.create(name);
    private static final ActorMaterializerSettings settings = ActorMaterializerSettings.create(system);
    private static final ActorMaterializer mat = ActorMaterializer.create(settings, system, name);

    private static final Path outputPath = Paths.get("postcode_restaurants.json");
    private static final Integer parallelismLevel = 2; // Number of concurrent threads to use to query the Yelp API

    private static final Set<StandardOpenOption> options = Set(CREATE, WRITE, APPEND).toJavaSet();

    /**
     * Conf de AhcWsClient voir : https://www.playframework.com/documentation/2.7.x/JavaWS
     */
    public static void main(String[] args) throws IOException {

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

        final Flow<Tuple2<String, WSResponse>, Tuple2<String, WSResponse>, NotUsed> errorLogger =
                Flow.<Tuple2<String, WSResponse>>create().map(postcodeWithResponse -> {
                    String postcode = postcodeWithResponse._1;
                    WSResponse response = postcodeWithResponse._2;
                    if (response.getStatus() != 200) {
                        println("Non 200 response for postcode " + postcode + ": [status: " + response.getStatus() + ", body: " + response.getBody());
                    }
                    return postcodeWithResponse;
                });

        final Graph<UniformFanOutShape<Tuple2<String, WSResponse>, Tuple2<String, WSResponse>>, NotUsed> responsePartitioner =
                Partition.create(2, s -> Match(Integer.valueOf(s._2.getStatus())).of(
                        Case($(is(200)), 0),
                        Case($(is(429)), 2),
                        Case($(), __ -> {
                            // other response statuses should have been filtered out already
                            printf("Unexpected status code %s", __);
                            throw new IllegalStateException("Unexpected status code " + __);
                        })));
    }

    /**
     * Our new apiQuerier works as follows: prior to making a request, we send a message to the
     * throttler. If the throttler is open, it will reply straightaway, letting us hit the Yelp API. If the
     * throttler is closed, it will not reply immediately. Instead, it will queue our query until it is
     * scheduled to open again. When the throttler opens, it replies to all queued queries, letting us
     * hit the API.
     */
    private Flow<String, Tuple2<String, WSResponse>, NotUsed> apiQuerier(AhcWSClient ws, Integer parallelismLevel, ActorRef throttler) {
        return Flow.<String>create().mapAsync(
                parallelismLevel,
                postcode -> {
                    ask(throttler, Throttler.WantToPass.class, Duration.ofHours(2));
                    return YelpApi.fetchPostcode(ws, postcode)
                            .map(response -> Tuple(postcode, response))
                            .toCompletableFuture();
                });
    }

}
