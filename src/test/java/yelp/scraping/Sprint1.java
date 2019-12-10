package yelp.scraping;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.ActorMaterializerSettings;
import akka.stream.javadsl.Source;
import com.typesafe.config.ConfigFactory;
import io.vavr.API;
import play.libs.ws.ahc.AhcWSClient;
import play.libs.ws.ahc.AhcWSClientConfigFactory;
import play.libs.ws.ahc.StandaloneAhcWSClient;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;

import static io.vavr.API.println;

public class Sprint1 {

    /**
     * Conf de AhcWsClient voir : https://www.playframework.com/documentation/2.7.x/JavaWS
     */
    public static void main(String[] args) {
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

        Seq<String> postcodesSeq = PostcodeLoader.load(); // Load the list of postcodes to query
        List<String> postcodes = JavaConverters.seqAsJavaList(postcodesSeq);
                postcodes.stream().forEach(API::println);

        Source<PostcodeRestaurants, NotUsed> postcodeResponses = Source.from(postcodes).take(100)
                .mapAsync(parallelismLevel,
                        postcode -> YelpApi.fetchPostcode(ws, postcode).whenComplete( (ok, e) -> {
                            if (e != null) {
                                e.printStackTrace();
                            }
                        }))
                .filter(p -> null != p.getRestaurants() && p.getRestaurants().size() > 0)
                .map(p -> p);
        postcodeResponses.runForeach(pR -> println(pR), materializer);
    }
}
