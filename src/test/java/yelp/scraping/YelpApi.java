package yelp.scraping;

import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.control.Option;
import play.libs.ws.WSResponse;
import play.libs.ws.ahc.AhcCurlRequestLogger;
import play.libs.ws.ahc.AhcWSClient;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;


public class YelpApi {

    private static final String token = Option.of(System.getenv("YELP_TOKEN"))
                                .getOrElseThrow(() -> new IllegalStateException("Missing YELP_TOKEN environment variable"));

    private static final String url = "https://api.yelp.com/v3/businesses/search";

    public static CompletionStage<PostcodeRestaurants> fetchPostcode(AhcWSClient wsClient, String postcode) {

        return wsClient.url(url)
                .setRequestFilter(new AhcCurlRequestLogger())
                .addHeader("Authorization", "Bearer "+token)
                .addQueryParameter("location", postcode)
                .addQueryParameter("limit", "5")
                .addQueryParameter("sort_by", "distance")
                .get()
                .thenCompose(wsResponse -> {
                    if (wsResponse.getStatus() == 200) {
                        //println("OK " + wsResponse.getBody());
                        //println(wsResponse.asJson());
                        List<String> locations = parseSuccessfulResponse(postcode, wsResponse);
                        return CompletableFuture.completedFuture(new PostcodeRestaurants(postcode, locations));
                    }else{
                        //println("Erreur : " + wsResponse.getBody());

                        return CompletableFuture.completedFuture(new PostcodeRestaurants("", null));
                    }
                });
    }


    public static List<String> parseSuccessfulResponse(String postcode, WSResponse response) {
        Iterator<JsonNode> businesses = response.asJson().findPath("businesses").elements();
        List<String> locations = new ArrayList<>();
        while(businesses.hasNext()) {
            JsonNode business = businesses.next();
            String location = business.findPath("location").toString();
            String zip_code = Postcode.normalize(business.findPath("zip_code").asText());
            //println(location);
            //println(zip_code);
            if (postcode.equals(zip_code)) {
                locations.add(location);
            }
        }
        return locations;
    }
}
