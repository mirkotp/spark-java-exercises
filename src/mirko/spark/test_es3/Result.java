package mirko.spark.test_es3;

import java.io.Serializable;

public class Result implements Serializable {
    private String location;
    private double total_cons;

    public String getLocation() {
        return this.location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public double getTotal_cons() {
        return this.total_cons;
    }

    public void setTotal_cons(double total_cons) {
		this.total_cons = total_cons;
	}
}