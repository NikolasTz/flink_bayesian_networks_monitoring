package datatypes.counter;

import java.io.Serializable;
import java.util.HashMap;

public class CoordinatorCounter implements Serializable {

    // Last sent values
    // Key : Each position correspond to each worker
    // Value : Correspond to last sent value (ni_dash)
    HashMap<String,Long> lastSentValues;

    // Last sent updates
    // Key : Each position correspond to each worker
    // Value : Correspond to last sent update (ni')
    HashMap<String,Long> lastSentUpdates;


    // Constructors
    public CoordinatorCounter(){}

    // Getters
    public HashMap<String, Long> getLastSentValues() { return lastSentValues; }
    public HashMap<String, Long> getLastSentUpdates() { return lastSentUpdates; }

    public Long getLastSentValues(String key) { return lastSentValues.get(key); }
    public Long getLastSentUpdates(String key) { return lastSentUpdates.get(key); }


    // Setters
    public void setLastSentValues(HashMap<String, Long> lastSentValues) { this.lastSentValues = lastSentValues; }
    public void setLastSentUpdates(HashMap<String, Long> lastSentUpdates) { this.lastSentUpdates = lastSentUpdates; }

    public void setLastSentValues(String key,Long lastSentValue) { this.lastSentValues.put(key,lastSentValue); }
    public void setLastSentUpdates(String key,Long lastSentUpdate) { this.lastSentUpdates.put(key,lastSentUpdate); }


    // Methods

    @Override
    public String toString(){

        return " , last sent values : " + this.lastSentValues
               + " , last sent updates : " + this.lastSentUpdates;
    }

}
