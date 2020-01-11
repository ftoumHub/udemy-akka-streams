package yelp.scraping;

import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.collection.List;

public class PostcodeRestaurants {

    private String postcode;
    private List<JsonNode> restaurants;

    public PostcodeRestaurants(String postcode, List<JsonNode> restaurants) {
        this.postcode = postcode;
        this.restaurants = restaurants;
    }

    public String getPostcode() {
        return postcode;
    }

    public void setPostcode(String postcode) {
        this.postcode = postcode;
    }

    public List<JsonNode> getRestaurants() {
        return restaurants;
    }

    public void setRestaurants(List<JsonNode> restaurants) {
        this.restaurants = restaurants;
    }

    @Override
    public String toString() {
        return "PostcodeRestaurants{ restaurants=" + restaurants + '}';
    }
}
