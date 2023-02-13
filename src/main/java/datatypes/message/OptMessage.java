package datatypes.message;


import config.InternalConfig.TypeMessage;
import datatypes.counter.Counter;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * This class constitutes an optimized version of Message class
 * The optimization is regarding to the size of message
 */
public class OptMessage implements Serializable {

    private String worker; // Worker who sent the message
    private TypeMessage typeMessage; // Type of message
    private long keyMessage; // Hashed key

    // One message for all type of counters
    private long value;
    private long lastBroadcastValue;
    private long timestamp;


    // Constructor
    public OptMessage(){}
    public OptMessage(String worker, Counter counter, TypeMessage typeMessage, long value, long lastBroadcastValue){

        this.worker = worker;
        this.typeMessage = typeMessage;
        this.keyMessage = counter.getCounterKey();
        this.value = value;
        this.lastBroadcastValue = lastBroadcastValue;
    }
    public OptMessage(String worker, long keyMessage, TypeMessage typeMessage, long value, long lastBroadcastValue){

        this.worker = worker;
        this.typeMessage = typeMessage;
        this.keyMessage = keyMessage;
        this.value = value;
        this.lastBroadcastValue = lastBroadcastValue;
    }

    // Getters
    public String getWorker() { return worker; }
    public TypeMessage getTypeMessage() { return typeMessage; }
    public long getKeyMessage() { return keyMessage; }

    public long getValue() { return value; }
    public long getLastBroadcastValue() { return lastBroadcastValue; }
    public long getTimestamp() { return timestamp; }

    // Setters
    public void setWorker(String worker) { this.worker = worker; }
    public void setTypeMessage(TypeMessage typeMessage) { this.typeMessage = typeMessage; }
    public void setKeyMessage(long keyMessage) { this.keyMessage = keyMessage; }

    public void setValue(long value) { this.value = value; }
    public void setLastBroadcastValue(long lastBroadcastValue) { this.lastBroadcastValue = lastBroadcastValue; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }


    // Methods

    @Override
    public String toString(){
        return "Message => worker : " + this.worker
                + " , typeMessage : " + this.typeMessage
                + " , keyMessage : " + this.keyMessage
                + " , value : " + this.value
                + " , lastBroadcastValue : " + this.lastBroadcastValue
                + " , message timestamp : " + new Timestamp(this.timestamp);
    }

    // Get the size of message in bytes
    public int size() throws ClassNotFoundException {

        int size;

        size =  this.getWorker().length()*Character.BYTES
                + this.getTypeMessage().toString().length()*Character.BYTES
                + Long.BYTES*4;

        return size;
    }

}
