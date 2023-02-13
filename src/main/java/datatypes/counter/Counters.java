package datatypes.counter;

import java.io.Serializable;
import java.util.LinkedHashMap;


/**
 * This class act as Wrapper class of HashMap<Long,Counter>.
 * Key is the hash-coded value of counter.
 * Key follow the format of nodeNames,nodeValues,nodeParentsNames,nodeParentsValues for BN
 * and the format of index,attr_value,class_value for NBC
 */
public class Counters implements Serializable {

    // This class act as Wrapper class of HashMap<Long,Counter>.
    // Key is the hash-coded value of counter.
    // Key follow the format of  <nodeNames,nodeValues,nodeParentsNames,nodeParentsValues> for BN
    // and the format of <index,attr_value,class_value> for NBC
    private LinkedHashMap<Long, Counter> counters;

    // Constructor
    public Counters(){ counters = new LinkedHashMap<>(); }

    // Getters
    public LinkedHashMap<Long, Counter> getCounters() { return counters; }

    // Setters
    public void setCounters(LinkedHashMap<Long, Counter> counters) { this.counters = counters; }

}
