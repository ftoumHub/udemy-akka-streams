package yelp.scraping;

import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.concurrent.Future;
import io.vavr.control.Option;
import org.apache.commons.collections4.IteratorUtils;
import play.libs.ws.WSResponse;
import play.libs.ws.ahc.AhcCurlRequestLogger;
import play.libs.ws.ahc.AhcWSClient;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static io.vavr.API.*;
import static io.vavr.Patterns.$None;
import static io.vavr.Patterns.$Some;


public class YelpApi {

    private static final String token = Option.of(System.getenv("YELP_TOKEN"))
                                .getOrElseThrow(() -> new IllegalStateException("Missing YELP_TOKEN environment variable"));

    private static final String url = "https://api.yelp.com/v3/businesses/search";

    public static Future<WSResponse> fetchPostcode(AhcWSClient wsClient, String postcode) {
        //println("--> fetching : " + postcode);
        final CompletionStage<WSResponse> wsResponseCompletionStage = getWsResponse(wsClient, postcode);
        return Future.fromCompletableFuture(wsResponseCompletionStage.toCompletableFuture());
    }

    private static CompletionStage<WSResponse> getWsResponse(AhcWSClient wsClient, String postcode) {
        return wsClient.url(url)
                //.setRequestFilter(new AhcCurlRequestLogger())
                .addQueryParameter("location", postcode)
                .addQueryParameter("limit", "50")
                .addQueryParameter("sort_by", "distance")
                .addHeader("Authorization", "Bearer " + token)
                .get();
    }


    public static List<JsonNode> parseSuccessfulResponse(String postcode, WSResponse response) {
        final List<JsonNode> businesses = IteratorUtils.toList(response.asJson().findPath("businesses").elements());

        return businesses.stream().filter(
                business -> {
                    Option<String> businessPostcodeMaybe = Option.of(business.findPath("location").findPath("zip_code").asText());

                    return Match(businessPostcodeMaybe).of(
                            Case($Some($()), Postcode.normalize(businessPostcodeMaybe.get()).equalsIgnoreCase(Postcode.normalize(postcode))),
                            Case($None(),    false));
                }
        ).collect(Collectors.toList());
    }
}
