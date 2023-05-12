package mirko.spark.sensors;

import java.io.Serializable;
import java.util.Date;

public class Sample implements Serializable {
    private int sensorId;
    private Date date;
    private float temp;

    public int getSensorId() {
        return this.sensorId;
    }

    public void setSensorId(int sensorId) {
        this.sensorId = sensorId;
    }

    public Date getDate() {
        return this.date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public float getTemp() {
        return this.temp;
    }

    public void setTemp(float temp) {
        this.temp = temp;
    }
}
