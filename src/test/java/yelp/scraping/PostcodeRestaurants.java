package yelp.scraping;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@AllArgsConstructor
@Getter
@Setter
@ToString
public class PostcodeRestaurants {

    private String postcode;
    private List<String> restaurants;
}
