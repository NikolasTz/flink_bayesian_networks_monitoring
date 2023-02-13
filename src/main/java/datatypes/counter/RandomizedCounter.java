package datatypes.counter;

import java.sql.Timestamp;

public class RandomizedCounter extends CoordinatorCounter {

    private long counter; // ni => local counter , increment whenever receive a new element
    private long counterDoubles; // Used to check when the local counter doubles
    private long lastBroadcastValue; // n_dash => last broadcast value from coordinator for this counter
    private long lastSentValue; // ni_dash => last sent value from worker for this counter
    private long lastTs; // Last timestamp received from input source
    private long numMessages; // Number of messages sent to coordinator for this counter
    private boolean sync; // Sync
    private double prob; // Probability
    private double epsilon; // Epsilon
    private double delta; // Delta
    private double resConstant; // Rescaling constant

    // Error >= c*epsilon*n , where n = last broadcast value and c >= 1
    // prob = (sqrt(numOfSites) / epsilon*lastBroadcastValue) * resConstant
    // 0 < epsilon < 1 and 0 < delta <= 1
    // With delta = 1 , error = 2*epsilon*n with prob = 3/4

    // Constructor
    public RandomizedCounter(){}

    // Getters
    public long getCounter() { return counter; }
    public long getCounterDoubles() { return counterDoubles; }
    public long getLastBroadcastValue() { return lastBroadcastValue; }
    public long getLastSentValue() { return lastSentValue; }
    public long getLastTs() { return lastTs; }
    public long getNumMessages() { return numMessages; }
    public boolean isSync() { return sync; }
    public double getProb() { return prob; }
    public double getEpsilon() { return epsilon; }
    public double getDelta() { return delta; }
    public double getResConstant() { return resConstant; }

    // Setters
    public void setCounter(long counter) { this.counter = counter; }
    public void setCounterDoubles(long counterDoubles) { this.counterDoubles = counterDoubles; }
    public void setLastBroadcastValue(long lastBroadcastValue) { this.lastBroadcastValue = lastBroadcastValue; }
    public void setLastSentValue(long lastSentValue) { this.lastSentValue = lastSentValue; }
    public void setLastTs(long lastTs) { this.lastTs = lastTs; }
    public void setNumMessages(long numMessages) { this.numMessages = numMessages; }
    public void setSync(boolean sync) { this.sync = sync; }
    public void setProb(double prob) { this.prob = prob; }
    public void setEpsilon(double epsilon) { this.epsilon = epsilon; }
    public void setDelta(double delta) { this.delta = delta; }
    public void setResConstant(double resConstant) { this.resConstant = resConstant; }

    // Methods

    @Override
    public String toString(){
        return  " , counter : " + this.counter
                + " , counterDoubles : " + this.counterDoubles
                + " , lastBroadcastValue : " + this.lastBroadcastValue
                + " , lastSentValue : " + this.lastSentValue
                + " , lastTs : " + new Timestamp(this.lastTs)
                + " , numMessages : " + this.numMessages
                + " , prob : " + this.prob
                + " , epsilon : " + this.epsilon
                + " , delta : " + this.delta
                + " , resConstant : " + this.resConstant
                + " , sync : " + this.sync
                + super.toString();
    }

    // Get randomized counter as string
    public String getRandomizedCounter(){ return this.toString(); }

}
