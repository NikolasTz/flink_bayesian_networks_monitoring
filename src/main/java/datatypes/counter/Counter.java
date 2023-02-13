package datatypes.counter;

import java.util.ArrayList;
import java.util.List;

import static utils.Util.convertToString;
import static utils.Util.equalsList;

public class Counter extends RandomizedCounter {

    private ArrayList<String> nodeName; // Names of node
    private ArrayList<String> nodeValue; // Values of node
    private boolean actualNode; // Indicates if counter correspond to actual or parent counter
    private ArrayList<String> nodeParents; // Names of parents node
    private ArrayList<String> nodeParentValues; // Values of parents node
    private double trueParameter; // The true parameter of node

    private long counterKey; // The key of counter

    // Constructors
    public Counter(){}

    // Getters
    public ArrayList<String> getNodeName() { return nodeName; }
    public ArrayList<String> getNodeValue() { return nodeValue; }
    public boolean isActualNode() { return actualNode; }
    public ArrayList<String> getNodeParents() { return nodeParents; }
    public ArrayList<String> getNodeParentValues() { return nodeParentValues;  }
    public double getTrueParameter() { return trueParameter; }
    public long getCounterKey() { return counterKey; }

    // Setters
    public void setNodeName(ArrayList<String> nodeName) { this.nodeName = nodeName; }
    public void setNodeValue(ArrayList<String> nodeValue) { this.nodeValue = nodeValue; }
    public void setActualNode(boolean actual_node) { this.actualNode = actual_node; }
    public void setNodeParents(ArrayList<String> nodeParents) { this.nodeParents = nodeParents; }
    public void setNodeParentValues(ArrayList<String> nodeParentValues) { this.nodeParentValues = nodeParentValues; }
    public void setTrueParameter(double trueParameter) { this.trueParameter = trueParameter; }
    public void setCounterKey(long counterKey) { this.counterKey = counterKey; }

    // Methods

    @Override
    public String toString(){
        return "Counter => name : " + this.nodeName
                + " , value : " + this.nodeValue
                + " , actualNode : " + this.actualNode
                + " , trueParameter : " + this.trueParameter
                + " , nodeParents : " + this.nodeParents
                + " , nodeParentValues : " + this.nodeParentValues
                + " , counter_value : " + ( this.nodeParentValues == null ? this.nodeValue : this.nodeValue.toString().concat(this.nodeParentValues.toString()))
                + super.toString();
    }

    public String getParameter(){
        return "Counter => name : " + this.nodeName
                + " , trueParameter : " + this.trueParameter
                + " , actualNode : " + this.actualNode
                + " , nodeParents : " + this.nodeParents
                + " , counter_value : " + ( this.nodeParentValues == null ? this.nodeValue : this.nodeValue.toString().concat(this.nodeParentValues.toString()));
    }

    // Concatenate the name of node and name of parents node
    public String concatParNames(){

        // Get the name
        String name;

        // Get the node name
        ArrayList<String> names = new ArrayList<>(this.getNodeName());

        // Append the node parents names if exists
        if( this.getNodeParents() != null ){ names.addAll(this.getNodeParents()); }

        name = convertToString(names).replace(" ","");
        return name;

    }

    // Concatenate the name of node and values of node
    public String concatValNames(){

        // Get the name
        String name;

        // Get the node name
        ArrayList<String> names = new ArrayList<>(this.getNodeName());

        // Get the values
        names.addAll(this.getNodeValue());

        name = convertToString(names).replace(" ","");
        return name;

    }

    // Find the references/count of orphan node
    public static int findRefOrphanCounter(List<Counter> counters, Counter refCounter){

        int ref=0;

        for(Counter counter : counters){
            if(counter.isActualNode() && counter.getNodeParents() == null && counter.getNodeName().equals(refCounter.getNodeName())) ref++;
        }

        return ref;
    }

    // Find the cardinality of actual node
    public static int findCardinality(List<Counter> counters,Counter searchCounter){

        // Check if searchCounter is orphan node
        if(searchCounter.getNodeParents() == null && searchCounter.getNodeParentValues() == null){
            // Orphan node
            return findRefOrphanCounter(counters,searchCounter);
        }

        List<String> keys = new ArrayList<>();

        // Key of search counter
        String searchKey = searchCounter.concatValNames();

        // Add to list
        keys.add(searchKey);

        // Key
        String key;

        for(Counter counter : counters){

            // Get the key
            key = counter.concatValNames();

            // Check the counter
            if( counter.isActualNode()
                && equalsList(counter.getNodeName(),searchCounter.getNodeName())
                && equalsList(counter.getNodeParents(),searchCounter.getNodeParents())
                && !keys.contains(key) ){
                keys.add(key);
            }
        }

        return keys.size();
    }

}
