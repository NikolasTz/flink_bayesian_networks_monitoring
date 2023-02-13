package datatypes.input;

import java.io.Serializable;
import java.sql.Timestamp;

public class Input implements Serializable {

    private String value;
    private String key;
    private long timestamp;

    // Constructors
    public Input(){}
    public Input(String value,String key,long timestamp){
        this.value = value;
        this.key = key;
        this.timestamp = timestamp;
    }

    // Getters
    public String getValue() { return value; }
    public String getKey() { return key; }
    public long getTimestamp() { return timestamp; }

    // Setters
    public void setValue(String value) { this.value = value; }
    public void setKey(String key) { this.key = key; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }


    // Methods

    @Override
    public String toString(){

        return  "Input => "
                + " value : " + this.value
                + " , key : " + this.key
                + " , timestamp : " + new Timestamp(this.timestamp);

    }

}
