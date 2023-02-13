package state;

import com.fasterxml.jackson.databind.ObjectMapper;
import config.CBNConfig;
import datatypes.counter.Counter;
import datatypes.NodeBN_schema;
import datatypes.counter.Counters;
import datatypes.input.Input;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static config.InternalConfig.ErrorSetup.NON_UNIFORM;
import static config.InternalConfig.TypeCounter.*;
import static config.InternalConfig.SetupCounter.*;
import static config.InternalConfig.TypeNetwork.*;
import static state.State.TypeNode.COORDINATOR;
import static job.JobKafka.coordinator_stats;
import static worker.WorkerFunction.getKey;
import static worker.WorkerFunction.getKeyNBC;

public class CoordinatorState extends State {

    // Last sent values(CoordinatorCounter class)
    // ni_dash => A simple counter which correspond to last sent value from worker
    // ni_dash => counter.getLastSentValues(worker)

    // Last sent updates(CoordinatorCounter class)
    // ni' => Correspond to the last sent update(whenever the local counter doubles) from specific worker
    // ni' =>  counter.getLastSentUpdates(worker)
    // n' => The sum of ni'

    // Probability
    // Probability => Correspond to the probability of counter,changes when new round begins
    // Probability => counter.getProb()

    // Last broadcast value
    // n_dash => Correspond to the sum of ni' , last broadcast value
    // n_dash => counter.getLastBroadcastValue()

    // Estimator of counter
    // ni_hat => Estimator of counter for specific worker
    // n_hat => The sum of ni_hat

    // Number of round(testing purposes)
    // counter.getNumMessages() , act as number of round for each randomized counter
    // counter.getCounterDoubles() , act as number of round for each deterministic counter


    private final String uid = UUID.randomUUID().toString(); // Unique id for coordinator
    private transient ValueState<Counters> coordinatorState; // Coordinator state
    private transient ValueState<Long> numMessagesSent; // Number of messages sent from coordinator
    private transient ValueState<Long> overheadMessages; // Overhead of messages
    private transient ValueState<Integer> endOfStreamCounter; // Counter of the END_OF_STREAM messages
    private transient ValueState<NodeBN_schema[]> nodes; // The nodes of BN or NBC

    private transient ValueState<Long> numMessagesSentWorkers; // Number of messages sent from Workers
    private transient ValueState<Integer> endOfWorkerCounter; // Counter of the END_OF_WORKER messages

    // Constructors
    public CoordinatorState(RuntimeContext ctx, CBNConfig config){

        // Initialize state
        super(config);

        // Initialize coordinator state
        this.coordinatorState = ctx.getState(new ValueStateDescriptor<>("coordinatorState"+uid,Types.GENERIC(Counters.class)));
        this.numMessagesSent = ctx.getState(new ValueStateDescriptor<>("numMessagesSent"+uid,Long.class));
        this.overheadMessages = ctx.getState(new ValueStateDescriptor<>("overheadMessages"+uid,Long.class));
        this.endOfStreamCounter = ctx.getState(new ValueStateDescriptor<>("endOfStreamCounter",Integer.class));
        this.numMessagesSentWorkers = ctx.getState(new ValueStateDescriptor<>("numMessagesSentWorkers"+uid,Long.class));
        this.nodes = ctx.getState(new ValueStateDescriptor<>("nodes"+uid,Types.OBJECT_ARRAY(TypeInformation.of(NodeBN_schema.class))));
        this.endOfWorkerCounter = ctx.getState(new ValueStateDescriptor<>("endOfWorkerCounter",Integer.class));
    }

    // Getters
    public Counters getCoordinatorState() throws IOException { return coordinatorState.value(); }
    public Long getNumMessagesSent() throws IOException { return numMessagesSent.value(); }
    public Long getOverheadMessages() throws IOException { return overheadMessages.value(); }
    public Integer getEndOfStreamCounter() throws IOException { return endOfStreamCounter.value(); }
    public Integer getEndOfWorkerCounter() throws IOException { return endOfWorkerCounter.value(); }
    public Long getNumMessagesSentWorkers() throws IOException { return numMessagesSentWorkers.value(); }
    public NodeBN_schema[] getNodes() throws IOException { return nodes.value(); }

    public Counter getCounter(long key) throws IOException { return coordinatorState.value().getCounters().get(key); }
    public LinkedHashMap<Long, Counter> getCounters() throws IOException { return coordinatorState.value().getCounters(); }

    // Setters
    public void setCoordinatorState(Counters counters) throws Exception { this.coordinatorState.update(counters); }
    public void setNumMessagesSent(Long numMessagesSent) throws IOException { this.numMessagesSent.update(numMessagesSent); }
    public void setOverheadMessages(Long overheadMessages) throws IOException { this.overheadMessages.update(overheadMessages); }
    public void setEndOfStreamCounter(Integer endOfStreamCounter) throws IOException { this.endOfStreamCounter.update(endOfStreamCounter); }
    public void setEndOfWorkerCounter(Integer endOfWorkerCounter) throws IOException { this.endOfWorkerCounter.update(endOfWorkerCounter); }
    public void setNumMessagesSentWorkers(Long numMessagesSentWorkers) throws IOException { this.numMessagesSentWorkers.update(numMessagesSentWorkers); }
    public void setNodes(NodeBN_schema[] nodes) throws IOException { this.nodes.update(nodes); }

    // Methods
    public void updateCoordinatorState() throws Exception { this.setCoordinatorState(this.getCoordinatorState()); }
    public NodeBN_schema getNode(int index) throws IOException { return nodes.value()[index]; }

    // Initialize the state of coordinator
    public void initializeCoordinatorState(String input) throws Exception {

        ObjectMapper objectMapper = new ObjectMapper();

        // Unmarshall JSON object as array
        NodeBN_schema[] nodes;
        if( input.contains(",") ){ nodes = objectMapper.readValue(input, NodeBN_schema[].class); } // Read value from string
        else { nodes = objectMapper.readValue(new File(input), NodeBN_schema[].class); } // Read value from file or url

        // Initialize the nodes
        setNodes(nodes);

        // Initialize the number of nodes of BN
        setNumOfNodes(nodes.length);

        // Initialize counters
        Vector<Counter> counters_bn = new Vector<>();
        if( this.getTypeNetwork() == NAIVE ){
            // Convention : The last node of NodeBN_schema array correspond to the class node and class counter/parameter => nodeParents and nodeParentsValues = null
            for (NodeBN_schema node : nodes) {
                // Class node
                if(node.equals(nodes[nodes.length-1])){
                    counters_bn.addAll(initializeCountersNBC(null,node));
                    continue;
                }
                // Attribute node
                counters_bn.addAll(initializeCountersNBC(node,nodes[nodes.length-1]));
            }
        }
        else{ for (NodeBN_schema node : nodes) { counters_bn.addAll(initializeCounters(node)); } }

        // Initialize the rest variables of counters based on type of counter
        if( this.getTypeCounter() == RANDOMIZED ){ initializeRandomizedCounter(counters_bn,nodes,COORDINATOR); }
        else if( this.getTypeCounter() == EXACT ){ initializeExactCounter(counters_bn,COORDINATOR); }
        else if( this.getTypeCounter() == CONTINUOUS ){ initializeContinuousCounter(counters_bn,COORDINATOR); }
        else{ initializeDeterministicCounter(counters_bn,nodes,COORDINATOR); } // DETERMINISTIC counter

        // Initialize the key of each counter
        this.setCoordinatorState(new Counters());
        LinkedHashMap<Long,Counter> keyedCounters = this.getCoordinatorState().getCounters();
        if( this.getTypeNetwork() == NAIVE ){
            long hashedKeyNBC;
            for(Counter counter: counters_bn){

                // Get the key
                String keyNBC = getKeyNBC(this.getDatasetSchema().get(counter.getNodeName().get(0)),counter);

                // Hash the key
                hashedKeyNBC = hashKey(keyNBC.getBytes());

                // Update the worker state
                keyedCounters.put(hashedKeyNBC,counter);

                // Update the counter
                counter.setCounterKey(hashedKeyNBC);
            }
        }
        else{
            long hashedKey;
            for(Counter counter: counters_bn){

                // Get the key
                String key = getKey(counter);

                // Hash the key
                hashedKey = hashKey(key.getBytes());

                // Update the worker state
                keyedCounters.put(hashedKey,counter);

                // Update the counter
                counter.setCounterKey(hashedKey);
            }
        }

        // Update the coordinator state
        updateCoordinatorState();

        // Initialize the number of round for each counter
        // initializeNumRound();

        // Initialize the number of messages sent by coordinator
        // initializeNumMessagesSent();
        setNumMessagesSent(0L);

        // Initialize the number of messages sent from Workers
        setNumMessagesSentWorkers(0L);

        // Initialize the overhead of messages
        setOverheadMessages(0L);

        // Initialize the endOfStreamCounter
        setEndOfStreamCounter(0);

        // Initialize the endOfWorkerCounter
        setEndOfWorkerCounter(0);


        // Collect some statistics
        System.out.println( "ErrorSetup : " + this.getErrorSetup()
                                            + " , setupCounter : " + this.getSetupCounter()
                                            + " , typeCounter : " + this.getTypeCounter()
                                            + " , typeNetwork : " + this.getTypeNetwork()
                                            + " , numOfWorkers : " + this.getNumOfWorkers()
                                            + " , eps : " + this.getEps()
                                            + " , delta : " + this.getDelta());

        // Check the correctness of epsilons of randomized individual counters
        if( this.getTypeCounter() == RANDOMIZED ) checkEpsilons(new ArrayList<>(this.getCoordinatorState().getCounters().values()),this.getEps());

    }

    // Initialize the number of messages sent by coordinator based on setupCounter
    /*public void initializeNumMessagesSent() throws Exception {

        // Check the type of counter
        if( this.getTypeCounter() == RANDOMIZED ){

            // Check the setup of counter
            if( this.getSetupCounter() == HALVED ){ setNumMessagesSent(((long) this.getSizeOfCounter()*this.getNumOfWorkers())); }
            else setNumMessagesSent(0L);
        }
        else if( this.getTypeCounter() == EXACT || this.getTypeCounter() == CONTINUOUS ){ setNumMessagesSent(0L); }
        else{
            // DETERMINISTIC counter
            setNumMessagesSent(0L);
        }
    }*/
    public void initializeNumMessagesSent() throws Exception {

        // Check the type of counter
        if( this.getTypeCounter() == RANDOMIZED ){

            // Check the setup of counter
            if( this.getSetupCounter() == ZERO ) setNumMessagesSent(0L);
            else{
                // Check the error setup
                if( this.getErrorSetup() == NON_UNIFORM ){
                    long numMessagesSent = 0L;
                    for(Counter counter : coordinatorState.value().getCounters().values()){
                        numMessagesSent += counter.getNumMessages()*2*this.getNumOfWorkers();
                    }
                    setNumMessagesSent(numMessagesSent);
                }
                else{
                    ArrayList<Long> keys = new ArrayList<>(coordinatorState.value().getCounters().keySet());
                    long numOfRounds = coordinatorState.value().getCounters().get(keys.get(0)).getNumMessages();
                    setNumMessagesSent(((long) this.getSizeOfCounter()*2*this.getNumOfWorkers()*numOfRounds));
                }

            }
        }
        else if( this.getTypeCounter() == DETERMINISTIC ){

            // Check the setup of counter
            if( this.getSetupCounter() == ZERO ) setNumMessagesSent(0L);
            else{

                // Check the error setup
                if( this.getErrorSetup() == NON_UNIFORM ){
                    long numMessagesSent = 0L;
                    for(Counter counter : coordinatorState.value().getCounters().values()){
                        numMessagesSent += counter.getCounterDoubles()*2*this.getNumOfWorkers();
                    }
                    setNumMessagesSent(numMessagesSent);
                }
                else{
                    ArrayList<Long> keys = new ArrayList<>(coordinatorState.value().getCounters().keySet());
                    long numOfRounds = coordinatorState.value().getCounters().get(keys.get(0)).getCounterDoubles();
                    setNumMessagesSent(((long) this.getSizeOfCounter()*2*this.getNumOfWorkers()*numOfRounds));
                }

            }
        }
        else if( this.getTypeCounter() == EXACT || this.getTypeCounter() == CONTINUOUS ){ setNumMessagesSent(0L); }
        else{ setNumMessagesSent(0L); }
    }

    // Initialize number of round for each counter
    public void initializeNumRound() throws IOException {

        if( this.getTypeCounter() == RANDOMIZED && this.getSetupCounter() == HALVED ){

            // Initialize the number of messages for each counter to 1.NumMessages act as num of round
            this.getCoordinatorState().getCounters().values().forEach( counter -> counter.setNumMessages(1L) );
        }
    }

    // Get the size of counter
    public int getSizeOfCounter() throws Exception { return this.getCoordinatorState().getCounters().size(); }

    // Check if all counters are synchronized
    public boolean checkCountersSync() throws IOException {
        for(Counter counter : this.getCoordinatorState().getCounters().values()){ if(!counter.isSync()) return false; }
        return true;
    }

    // Check if the queries are ready that is if fulfill the appropriate size
    public static boolean AreQueriesReady(ListState<Input> state, long sizeOfQueries) throws Exception {

        // Get queries
        List<Input> queries = (List<Input>) state.get();
        return queries.size() == sizeOfQueries;
    }

    // Reset the field true parameter of counters , using only for Naive Bayes Classifier
    public void resetTrueParameters() throws IOException {
        this.getCoordinatorState().getCounters().values().forEach( counter -> counter.setTrueParameter(0) );
    }

    // Print the worker state
    public void printCoordinatorState(CoProcessFunction<?,?,?>.Context ctx) throws Exception {

        ctx.output(coordinator_stats ,"Coordinator state : "
                                        + " numMessagesSent : " + this.getNumMessagesSent()
                                        + " , numMessagesSentWorkers : " + this.getNumMessagesSentWorkers()
                                        + " , overheadMessages : " + this.getOverheadMessages()
                                        + " , endOfStreamCounter " + this.getEndOfStreamCounter()
                                        + " , endOfWorkerCounter " + this.getEndOfWorkerCounter()
                                        + " , size of counters : " + this.getSizeOfCounter()
                                        + super.toString());

        // Print all the counters
        this.getCoordinatorState().getCounters().values().forEach( counter ->  ctx.output(coordinator_stats ,counter.toString()+"\n"));

    }

    @Override
    public String toString(){

        try {
            return  "Coordinator state : "
                    + " counters : " + this.getCoordinatorState()
                    + " , numMessagesSent : " + this.getNumMessagesSent()
                    + " , overheadMessages: " + this.getOverheadMessages()
                    + " , endOfStreamCounter " + this.getEndOfStreamCounter()
                    + " , endOfWorkerCounter " + this.getEndOfWorkerCounter();

        }
        catch (IOException e) { return e.toString(); }

    }

}
