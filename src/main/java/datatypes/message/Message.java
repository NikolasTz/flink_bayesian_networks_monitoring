package datatypes.message;

import datatypes.counter.Counter;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;

import static utils.Util.getSizeList;
import static config.InternalConfig.TypeMessage;

public class Message implements Serializable {

    private String worker; // Worker who sent the message
    private TypeMessage typeMessage; // Type of message

    private ArrayList<String> nodeName; // Names of node
    private ArrayList<String> nodeValue; // Values of node
    private boolean actualNode; // Indicates if counter correspond to actual or parent counter
    private ArrayList<String> nodeParents; // Names of parents node
    private ArrayList<String> nodeParentValues; // Values of parents node

    // One message for all type of counters
    private long value;
    private long lastBroadcastValue;
    private long timestamp;

    // Constructor
    public Message(){}
    public Message(String worker, Counter counter, TypeMessage typeMessage, long value, long lastBroadcastValue){

        this.worker = worker;
        this.typeMessage = typeMessage;

        this.nodeName = counter.getNodeName();
        this.nodeValue = counter.getNodeValue();
        this.actualNode = counter.isActualNode();
        this.nodeParents = counter.getNodeParents();
        this.nodeParentValues = counter.getNodeParentValues();

        this.value = value;
        this.lastBroadcastValue = lastBroadcastValue;
    }

    // Getters
    public String getWorker() { return worker; }
    public TypeMessage getTypeMessage() { return typeMessage; }
    public ArrayList<String> getNodeName() { return nodeName; }
    public ArrayList<String> getNodeValue() { return nodeValue; }
    public boolean isActualNode() { return actualNode; }
    public ArrayList<String> getNodeParents() { return nodeParents; }
    public ArrayList<String> getNodeParentValues() { return nodeParentValues; }

    public long getValue() { return value; }
    public long getLastBroadcastValue() { return lastBroadcastValue; }
    public long getTimestamp() { return timestamp; }

    // Setters
    public void setWorker(String worker) { this.worker = worker; }
    public void setTypeMessage(TypeMessage typeMessage) { this.typeMessage = typeMessage; }
    public void setNodeName(ArrayList<String> nodeName) { this.nodeName = nodeName; }
    public void setNodeValue(ArrayList<String> nodeValue) { this.nodeValue = nodeValue; }
    public void setActualNode(boolean actualNode) { this.actualNode = actualNode; }
    public void setNodeParents(ArrayList<String> nodeParents) { this.nodeParents = nodeParents; }
    public void setNodeParentValues(ArrayList<String> nodeParentValues) { this.nodeParentValues = nodeParentValues; }

    public void setValue(long value) { this.value = value; }
    public void setLastBroadcastValue(long lastBroadcastValue) { this.lastBroadcastValue = lastBroadcastValue; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }


    // Methods

    @Override
    public String toString(){
        return "Message => worker : " + this.worker
                + " , typeMessage : " + this.typeMessage
                + " , name : " + this.nodeName
                + " , value : " + this.nodeValue
                + " , actualNode : " + this.actualNode
                + " , nodeParents : " + this.nodeParents
                + " , nodeParentValues : " + this.nodeParentValues
                + " , counter_value : " + ( this.nodeParentValues == null ? this.nodeValue : this.nodeValue.toString().concat(this.nodeParentValues.toString()))
                + " , value : " + this.value
                + " , lastBroadcastValue : " + this.lastBroadcastValue
                + " , message timestamp : " + new Timestamp(this.timestamp);
    }

    // Get the size of message in bytes
    public int size() throws ClassNotFoundException {

        int size;

        size =  this.getWorker().length()*Character.BYTES
                + this.getTypeMessage().toString().length()*Character.BYTES
                + getSizeList(getNodeName())
                + getSizeList(getNodeValue())
                + getSizeList(getNodeParents())
                + getSizeList(getNodeParentValues())
                + Long.BYTES*3;

        return size;
    }

    // Get the overhead of message in bytes
    public int overhead() throws ClassNotFoundException {
        return this.getWorker().length()*Character.BYTES
                + this.getTypeMessage().toString().length()*Character.BYTES
                + getSizeList(getNodeName())
                + getSizeList(getNodeValue())
                + getSizeList(getNodeParents())
                + getSizeList(getNodeParentValues())
                + Long.BYTES;
    }

}
