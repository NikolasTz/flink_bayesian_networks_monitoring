package datatypes.message;


import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Arrays;

import static config.InternalConfig.TypeMessage;

public class MessageFGM implements Serializable {

    private String worker; // Worker who sent the message
    private TypeMessage typeMessage; // Type of message

    private double[][] vector; // Vector correspond to drift vector Xi or estimated vector E based on type of message
    private double value ; // The value of message
    private long timestamp; // The timestamp of message

    // Constructors
    public MessageFGM(){}
    public MessageFGM(String worker,TypeMessage typeMessage,long timestamp,double[][] vector){
        this.worker = worker;
        this.typeMessage = typeMessage;
        this.timestamp = timestamp;
        initializeVector(vector);
    }
    public MessageFGM(String worker,TypeMessage typeMessage,long timestamp,double value){
        this.worker = worker;
        this.typeMessage = typeMessage;
        this.timestamp = timestamp;
        this.value = value;
        this.vector = null;
    }

    // Getters
    public String getWorker() { return worker; }
    public TypeMessage getTypeMessage() { return typeMessage; }
    public long getTimestamp() { return timestamp; }
    public double[][] getVector() { return vector; }
    public double getValue() { return value; }

    // Setters
    public void setWorker(String worker) { this.worker = worker; }
    public void setTypeMessage(TypeMessage typeMessage) { this.typeMessage = typeMessage; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    public void setVector(double[][] vector) { this.vector = vector; }
    public void setValue(double value) { this.value = value; }

    // Methods

    public void initializeVector(double[][] vector){

        // The new vector
        this.vector = new double[vector.length][vector[0].length];

        // Copy the values
        for(int i=0;i<vector.length;i++){
            System.arraycopy(vector[i], 0, this.vector[i], 0, vector[0].length);
        }
    }

    @Override
    public String toString(){
        return "Message => worker : " + this.worker
                + " , typeMessage : " + this.typeMessage
                + " , value : " + this.value
                + " , vector : " + Arrays.deepToString(this.vector)
                + " , timestamp : " + new Timestamp(this.timestamp);
    }

    // Get the size of message in bytes
    public int size(){

        int size;
        size =  this.getWorker().length()*Character.BYTES
                + this.getTypeMessage().toString().length()*Character.BYTES
                + 2*Long.BYTES
                + ((this.vector == null) ? 0 : this.vector.length*this.vector[0].length*Double.BYTES);

        return size;
    }

    // Get the overhead of message in bytes
    public int overhead(){
        return  this.getWorker().length()*Character.BYTES
                + this.getTypeMessage().toString().length()*Character.BYTES
                + Long.BYTES;
    }

    /* Testing purposes
    // List<Counter> counters;
    // public List<Counter> getCounters() { return counters; }
    // public void setCounters(List<Counter> counters) { this.counters = counters; }
    // Constructor
    public MessageFGM(String worker,TypeMessage typeMessage,long timestamp,double value,List<Counter> counters){
        this.worker = worker;
        this.typeMessage = typeMessage;
        this.timestamp = timestamp;
        this.value = value;
        this.counters = counters;
        this.vector = null;
    }*/
}
