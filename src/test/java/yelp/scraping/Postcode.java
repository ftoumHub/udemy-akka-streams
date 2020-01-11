package yelp.scraping;

public class Postcode {

    public static String normalize(String postcode) {
        return postcode.toLowerCase().replace(" ", "");
    }
}
