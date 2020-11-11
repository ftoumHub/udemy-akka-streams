package yelp.scraping;

import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.concurrent.Future;
import io.vavr.control.Option;
import org.apache.commons.collections4.IteratorUtils;
import play.libs.ws.WSResponse;
import play.libs.ws.ahc.AhcWSClient;

import java.util.List;
import java.util.concurrent.CompletionStage;

import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static io.vavr.API.Option;
import static io.vavr.Patterns.$None;
import static io.vavr.Patterns.$Some;
import static io.vavr.concurrent.Future.fromCompletableFuture;
import static java.util.stream.Collectors.toList;
import static yelp.scraping.Postcode.normalize;

public class YelpApi {

    private static final String token = Option(System.getenv("YELP_TOKEN"))
                                .getOrElseThrow(() -> new IllegalStateException("Missing YELP_TOKEN environment variable"));

    private static final String url = "https://api.yelp.com/v3/businesses/search";

    public static Future<WSResponse> fetchPostcode(AhcWSClient wsClient, String postcode) {
        return fromCompletableFuture(getWsResponse(wsClient, postcode).toCompletableFuture());
    }

    private static CompletionStage<WSResponse> getWsResponse(AhcWSClient wsClient, String postcode) {
        //println("==> YelpApi.getWsResponse");
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
                    Option<String> businessPostcode = Option(business.findPath("location").findPath("zip_code").asText());

                    final boolean retVal = normalize(businessPostcode.get()).equalsIgnoreCase(normalize(postcode));
                    return Match(businessPostcode).of(
                            Case($Some($()), retVal),
                            Case($None(),    false));
                }
        ).collect(toList());
    }
}
