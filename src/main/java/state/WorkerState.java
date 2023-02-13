package state;

import com.fasterxml.jackson.databind.ObjectMapper;
import config.CBNConfig;
import datatypes.counter.Counter;
import datatypes.NodeBN_schema;
import datatypes.counter.Counters;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static config.InternalConfig.TypeCounter.*;
import static config.InternalConfig.SetupCounter.*;
import static config.InternalConfig.ErrorSetup.*;
import static config.InternalConfig.TypeNetwork.*;
import static state.State.TypeNode.WORKER;
import static job.JobKafka.worker_stats;
import static worker.WorkerFunction.getKey;
import static worker.WorkerFunction.getKeyNBC;

public class WorkerState extends State {

    private final String uid = UUID.randomUUID().toString(); // Unique id per worker
    private transient ValueState<Counters> workerState; // The maintained counters
    private transient ValueState<Long> numMessages; // The total number of messages sent by worker
    private transient ValueState<Counter> totalCount; // The total count of input
    private transient ValueState<NodeBN_schema[]> nodes; // The nodes of BN or NBC
    private transient ValueState<Integer> endOfStreamCounter; // Counter of the END_OF_STREAM messages
    private transient ValueState<Long> timer; // Use for Worker statistics

    // Constructors
    public WorkerState(RuntimeContext ctx, CBNConfig config){

        // Initialize state
        super(config);

        // Initialize worker state
        this.workerState = ctx.getState(new ValueStateDescriptor<>("counters"+uid,Types.GENERIC(Counters.class)));
        this.numMessages = ctx.getState(new ValueStateDescriptor<>("numMessages"+uid,Long.class));
        this.totalCount = ctx.getState(new ValueStateDescriptor<>("totalCount"+uid,Types.POJO(Counter.class)));
        this.nodes = ctx.getState(new ValueStateDescriptor<>("nodes"+uid,Types.OBJECT_ARRAY(TypeInformation.of(NodeBN_schema.class))));
        this.endOfStreamCounter = ctx.getState(new ValueStateDescriptor<>("endOfStreamCounter"+uid,Integer.class));
        this.timer = ctx.getState(new ValueStateDescriptor<>("time"+uid,Long.class));
    }

    // Getters
    public Counters getWorkerState() throws IOException { return workerState.value(); }
    public Long getNumMessages() throws IOException { return numMessages.value(); }
    public Counter getTotalCount() throws IOException { return totalCount.value(); }
    public NodeBN_schema[] getNodes() throws IOException { return nodes.value(); }
    public Integer getEndOfStreamCounter() throws IOException { return endOfStreamCounter.value(); }
    public Long getTimer() throws IOException { return timer.value(); }

    public Counter getCounter(long key) throws IOException { return workerState.value().getCounters().get(key); }
    public LinkedHashMap<Long, Counter> getCounters() throws IOException { return workerState.value().getCounters(); }

    // Setters
    public void setWorkerState(Counters workerState) throws IOException { this.workerState.update(workerState); }
    public void setNumMessages(Long numMessages) throws IOException { this.numMessages.update(numMessages); }
    public void setTotalCount(Counter totalCount) throws IOException { this.totalCount.update(totalCount); }
    public void setNodes(NodeBN_schema[] nodes) throws IOException { this.nodes.update(nodes); }
    public void setEndOfStreamCounter(Integer endOfStreamCounter) throws IOException { this.endOfStreamCounter.update(endOfStreamCounter); }
    public void setTimer(Long timer) throws IOException { this.timer.update(timer); }

    // Methods
    public void updateWorkerState() throws IOException { this.setWorkerState(this.getWorkerState()); }
    public NodeBN_schema getNode(int index) throws IOException { return nodes.value()[index]; }

    // Initialize the state of worker
    public void initializeWorkerState(String input) throws IOException {

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
            // Convention : The last node of NodeBN_schema array correspond to the class node and for class counter/parameter => nodeParents and nodeParentsValues = null
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

        // Initialize the total count
        Counter total_count = new Counter();
        ArrayList<String> name = new ArrayList<>(); name.add("total_count");
        total_count.setNodeName(name);
        total_count.setActualNode(false);
        total_count.setCounter(0L);

        // Update total count and endOfStream counter
        setTotalCount(total_count);
        setEndOfStreamCounter(0);
        setTimer(0L);

        // Initialize the rest variables of counters
        if( this.getTypeCounter() == RANDOMIZED ){ initializeRandomizedCounter(counters_bn,nodes,WORKER); }
        else if( this.getTypeCounter() == EXACT ){ initializeExactCounter(counters_bn,WORKER); }
        else if( this.getTypeCounter() == CONTINUOUS ){ initializeContinuousCounter(counters_bn,WORKER); }
        else{ initializeDeterministicCounter(counters_bn,nodes,WORKER); }  // DETERMINISTIC counter

        // Initialize the key of each counter
        this.setWorkerState(new Counters());
        LinkedHashMap<Long,Counter> keyedCounters = this.getWorkerState().getCounters();
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

        // Update the worker state
        updateWorkerState();

        // Initialize the number of messages based on errorSetup and setupCounter
        // initializeNumMessages();
        setNumMessages(0L);

    }

    // Initialize the number of messages based on errorSetup and setupCounter
    public void initializeNumMessages() throws IOException {

        // Check the type of counter
        if( this.getTypeCounter() == RANDOMIZED ){

            // Check the setup of counter
            if( this.getSetupCounter() == HALVED ){
                // Check the error setup
                if( this.getErrorSetup() == NON_UNIFORM ){

                    AtomicLong num_messages = new AtomicLong();

                    // Find the maximum of number of messages of counters
                    this.getWorkerState().getCounters().values().forEach(counter -> {
                        if(counter.getNumMessages() > num_messages.get()) num_messages.set(counter.getNumMessages());
                    });
                    setNumMessages(num_messages.get());
                }
                else setNumMessages(new ArrayList<>(this.getWorkerState().getCounters().values()).get(0).getNumMessages());
            }
            else setNumMessages(0L);
        }
        else if( this.getTypeCounter() == EXACT || this.getTypeCounter() == CONTINUOUS ){ setNumMessages(0L); }
        else{
            // DETERMINISTIC counter
            setNumMessages(0L);
        }
    }

    // Check if all counters are synchronized
    public boolean checkCountersSync() throws IOException {
        for(Counter counter : this.getWorkerState().getCounters().values()){ if(!counter.isSync()) return false; }
        return true;
    }

    // Print the worker state
    public void printWorkerState(KeyedCoProcessFunction<?,?,?,?>.Context ctx) throws IOException {

        ctx.output(worker_stats, "Worker state : "
                                    + " Worker " + ctx.getCurrentKey().toString()
                                    + " , numMessages : " + this.getNumMessages()
                                    + " , totalCount : " + this.getTotalCount()
                                    + super.toString()
                                    + " , Size of total counters is : " + this.getWorkerState().getCounters().size());

        // Print all the counters
        this.getWorkerState().getCounters().values().forEach(counter -> ctx.output(worker_stats,"Worker "+ctx.getCurrentKey()+" : "+counter.toString()+"\n"));
    }

    // Collect worker state(test)
    public String collectWorkerState() throws IOException {

        StringBuilder str = new StringBuilder();

        // Print all the counters
        this.getWorkerState().getCounters().values().forEach(counter -> str.append("\n").append(counter.toString()).append("\n"));

        return str.toString();
    }

    @Override
    public String toString(){

        try {
            return  "Worker state : "
                    + " counters : " + this.getWorkerState()
                    + " , numMessages : " + this.getNumMessages()
                    + " , totalCount : " + this.getTotalCount();

        }
        catch (IOException e) { return e.toString(); }

    }

}
