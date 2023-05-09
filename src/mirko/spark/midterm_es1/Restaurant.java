package mirko.spark.midterm_es1;

import java.io.Serializable;

public class Restaurant implements Serializable {
    private String restaurant_name;
    private String main_cuisine_type;
    private String city;

    public String getRestaurant_name() {
        return this.restaurant_name;
    }

    public void setRestaurant_name(String restaurantName) {
        this.restaurant_name = restaurantName;
    }

    public String getMain_cuisine_type() {
        return this.main_cuisine_type;
    }

    public void setMain_cuisine_type(String mainCuisineType) {
        this.main_cuisine_type = mainCuisineType;
    }

    public String getCity() {
        return this.city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}
