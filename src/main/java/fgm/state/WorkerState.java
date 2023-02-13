package fgm.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import config.FGMConfig;
import datatypes.NodeBN_schema;
import datatypes.counter.Counter;
import datatypes.counter.Counters;
import fgm.datatypes.VectorType;
import fgm.safezone.SafeZone;
import fgm.sketch.AGMS;
import fgm.sketch.CountMin;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

import static config.InternalConfig.TypeNetwork.NAIVE;
import static config.InternalConfig.TypeState.*;
import static config.InternalConfig.ErrorSetup.*;
import static fgm.job.JobFGM.worker_stats;
import static fgm.sketch.CountMin.calculateDepth;
import static fgm.sketch.CountMin.calculateWidth;
import static fgm.state.CoordinatorState.*;
import static fgm.worker.WorkerFunction.getKey;
import static fgm.worker.WorkerFunction.getKeyNBC;
import static state.State.initializeCounters;
import static state.State.initializeCountersNBC;


public class WorkerState extends FGMState {

    // Fields
    private final String uid = UUID.randomUUID().toString(); // Unique id per worker

    // Protocol fields
    private transient ValueState<VectorType> driftVector; // Drift vector Xi
    private transient ValueState<VectorType> estimatedVector; // Estimated state vector Ei
    private transient ValueState<SafeZone> safeZone; // The safe zone
    private transient ValueState<Integer> localCounter; // Local counter ci

    private transient ValueState<Double> quantum; // Quantum - theta
    private transient ValueState<Double> zeta; // Zeta
    private transient ValueState<Double> initialZeta; // Initial zeta
    private transient ValueState<Boolean> syncWorker; // This variable using for synchronization of subround and round
    private transient ValueState<Long> lastTs; // Using for per window computation

    // Rebalancing fields
    private transient ValueState<Double> lambda; // Lambda

    // Bayesian Network or Classifier
    private transient ValueState<Counters> parameters; // Parameters of Bayesian Network
    private transient ValueState<NodeBN_schema[]> nodes; // The nodes of BN or NBC

    // Statistic fields
    private transient ValueState<Long> numOfMessages; // Number of messages sent by workers
    private transient ValueState<Long> totalCount; // The total elements received from workers
    private transient ValueState<Integer> endOfStreamCounter; // Counter of the END_OF_STREAM messages
    private transient ValueState<Long> timer; //Use for Worker statistics

    // Hold the phi , maybe not ??? , not used at present time
    // Split the class MessageFGM to two classes ???
    // numOfRound , testing purposes ???
    // First round = Zero round used as warmup-initialization ??? , done
    // test-RC with NBC ???
    // NodeBN schema for NBC ???
    // Experiments => Training runtime vs Cluster size ???
    // Number of individuals counter and number of messages ???
    // Send only violations => individual counters and send all the counters => state ???


    // Constructors
    public WorkerState(RuntimeContext ctx, FGMConfig config){

        // Initialize state
        super(config);

        // Initialize worker state
        this.driftVector = ctx.getState(new ValueStateDescriptor<>("driftVector"+uid, Types.GENERIC(VectorType.class)));
        this.estimatedVector = ctx.getState(new ValueStateDescriptor<>("estimatedVector"+uid, Types.GENERIC(VectorType.class)));
        this.safeZone = ctx.getState(new ValueStateDescriptor<>("safeZone"+uid,Types.POJO(SafeZone.class)));
        this.localCounter = ctx.getState(new ValueStateDescriptor<>("localCounter"+uid, Integer.class));

        this.quantum = ctx.getState(new ValueStateDescriptor<>("quantum"+uid, Double.class));
        this.zeta = ctx.getState(new ValueStateDescriptor<>("zeta"+uid, Double.class));
        this.initialZeta = ctx.getState(new ValueStateDescriptor<>("initialZeta"+uid, Double.class));
        this.syncWorker = ctx.getState(new ValueStateDescriptor<>("syncWorker"+uid, Boolean.class));

        // Rebalancing fields
        this.lambda = ctx.getState(new ValueStateDescriptor<>("lambda"+uid,Double.class));

        // Parameters/counters
        this.parameters = ctx.getState(new ValueStateDescriptor<>("parameters"+uid,Types.GENERIC(Counters.class)));
        this.nodes = ctx.getState(new ValueStateDescriptor<>("nodes"+uid,Types.OBJECT_ARRAY(TypeInformation.of(NodeBN_schema.class))));

        // Statistics
        this.numOfMessages = ctx.getState(new ValueStateDescriptor<>("numOfMessages"+uid, Long.class));
        this.totalCount = ctx.getState(new ValueStateDescriptor<>("totalCount"+uid,Long.class));
        this.lastTs = ctx.getState(new ValueStateDescriptor<>("lastTs"+uid,Long.class));
        this.endOfStreamCounter = ctx.getState(new ValueStateDescriptor<>("endOfStreamCounter"+uid,Integer.class));
        this.timer = ctx.getState(new ValueStateDescriptor<>("timer"+uid,Long.class));
    }

    // Getters
    public VectorType getDriftVector() throws IOException { return driftVector.value(); }
    public VectorType getEstimatedVector() throws IOException { return estimatedVector.value(); }
    public Integer getLocalCounter() throws IOException { return localCounter.value(); }
    public Double getQuantum() throws IOException { return quantum.value(); }
    public Double getZeta() throws IOException { return zeta.value(); }
    public Long getLastTs() throws IOException { return lastTs.value(); }
    public Double getInitialZeta() throws IOException { return initialZeta.value(); }
    public SafeZone getSafeZone() throws IOException { return safeZone.value(); }
    public Boolean getSyncWorker() throws IOException { return syncWorker.value(); }

    // Rebalancing
    public Double getLambda() throws IOException { return lambda.value(); }

    // Bayesian Network or Classifier
    public Counters getParameters() throws IOException { return parameters.value(); }
    public NodeBN_schema[] getNodes() throws IOException { return nodes.value(); }
    public Counter getCounter(long key) throws IOException { return parameters.value().getCounters().get(key); }
    public LinkedHashMap<Long, Counter> getCounters() throws IOException { return parameters.value().getCounters(); }

    // Statistics
    public Long getNumOfMessages() throws IOException { return numOfMessages.value(); }
    public Long getTotalCount() throws IOException { return totalCount.value(); }
    public Integer getEndOfStreamCounter() throws IOException { return endOfStreamCounter.value(); }
    public Long getTimer() throws IOException { return timer.value(); }

    public double[][] getDrift() throws IOException { return driftVector.value().get(); }
    public double[][] getEstimation() throws IOException { return estimatedVector.value().get(); }


    // Setters
    public void setDriftVector(VectorType driftVector) throws IOException { this.driftVector.update(driftVector); }
    public void setDriftVector(double[][] driftVector) throws IOException { this.driftVector.value().set(driftVector); }
    public void setEstimatedVector(VectorType estimatedVector) throws IOException { this.estimatedVector.update(estimatedVector); }
    public void setEstimatedVector(double[][] estimatedVector) throws IOException { this.estimatedVector.value().set(estimatedVector); }
    public void setLocalCounter(Integer localCounter) throws IOException { this.localCounter.update(localCounter); }
    public void setQuantum(Double quantum) throws IOException { this.quantum.update(quantum); }
    public void setZeta(Double zeta) throws IOException { this.zeta.update(zeta); }
    public void setNumOfMessages(Long numOfMessages) throws IOException { this.numOfMessages.update(numOfMessages); }
    public void setTotalCount(Long totalCount) throws IOException{ this.totalCount.update(totalCount); }
    public void setLastTs(Long lastTs) throws IOException { this.lastTs.update(lastTs); }
    public void setInitialZeta(Double initialZeta) throws IOException { this.initialZeta.update(initialZeta); }
    public void setSafeZone(SafeZone safeZone) throws IOException { this.safeZone.update(safeZone); }
    public void setSyncWorker(Boolean syncWorker) throws IOException { this.syncWorker.update(syncWorker); }
    public void setEndOfStreamCounter(Integer endOfStreamCounter) throws IOException { this.endOfStreamCounter.update(endOfStreamCounter); }
    public void setTimer(Long timer) throws IOException { this.timer.update(timer); }

    // Rebalancing
    public void setLambda(Double lambda) throws IOException { this.lambda.update(lambda); }

    // Bayesian Network or Classifier
    public void setParameters(Counters parameters) throws IOException { this.parameters.update(parameters); }
    public void setNodes(NodeBN_schema[] nodes) throws IOException { this.nodes.update(nodes); }


    // Methods

    public void updateEstimatedVector() throws IOException { this.setEstimatedVector(this.getEstimatedVector()); }
    public void updateDriftVector() throws IOException { this.setDriftVector(this.getDriftVector()); }
    public void updateParameters() throws IOException { this.setParameters(this.getParameters()); }
    public NodeBN_schema getNode(int index) throws IOException { return nodes.value()[index]; }

    // Initialize the state of worker
    public void initializeWorkerState(FGMConfig config) throws IOException {

        // Initialize the parameters
        ObjectMapper objectMapper = new ObjectMapper();

        // Unmarshall JSON object as array
        NodeBN_schema[] nodes;
        if( config.BNSchema().contains(",") ){ nodes = objectMapper.readValue(config.BNSchema(), NodeBN_schema[].class); } // Read value from string
        else { nodes = objectMapper.readValue(new File(config.BNSchema()), NodeBN_schema[].class); } // Read value from file or url

        // Initialize the nodes
        setNodes(nodes);

        // Initialize the number of nodes of BN
        setNumOfNodes(nodes.length);

        // Initialize counters-parameters
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

        // Initialize the key of the counters
        this.setParameters(new Counters());
        LinkedHashMap<Long,Counter> keyedCounters = this.getParameters().getCounters();
        if( this.getTypeNetwork() == NAIVE ){
            long hashedKeyNBC;
            for(Counter counter : counters_bn){

                // Get the key
                String keyNBC = getKeyNBC(this.getDatasetSchema().get(counter.getNodeName().get(0)),counter);

                // Hash the key
                hashedKeyNBC = hashKey(keyNBC.getBytes());

                // Update the parameters
                keyedCounters.put(hashedKeyNBC,counter);

                // Update the counter
                counter.setCounterKey(hashedKeyNBC);
            }
        }
        else{
            long hashedKey;
            for(Counter counter : counters_bn){

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

        // Update the parameters
        updateParameters();

        // Initialize the estimated and drift vector
        if( config.typeState() == VECTOR ){ initializeEstAndDriftVector(); }
        else if( config.typeState() == COUNT_MIN ){

            // Initialize the estimatedVector
            initializeEstimatedVectorCM(config);

            // Get the estimated vector
            CountMin cm = (CountMin) this.getEstimatedVector();

            // Initialize the driftVector
            setDriftVector(new CountMin(cm.depth(),cm.width()));

            // Reset the vectors
            this.getDriftVector().resetVector();
            this.getEstimatedVector().resetVector();

            // Update the vectors
            this.updateEstimatedVector();
            this.updateDriftVector();
        }
        else{
            // AGMS sketch
            initializeEstAndDriftAGMS();
        }

        // Initialize the safe zone
        if( config.errorSetup() == BASELINE ){ setSafeZone(new SafeZone(this.baseline())); }
        else{ setSafeZone(new SafeZone(this.uniform())); }


        // Initialize the fields of rebalancing
        setLambda(1.0d);

        // Initialize the rest of the variables
        setLocalCounter(0);
        setQuantum(0d);
        setZeta(0d);
        setNumOfMessages(0L);
        setTotalCount(0L);
        setLastTs(0L);
        setInitialZeta(0d);
        setSyncWorker(false);
        setEndOfStreamCounter(0);
        setTimer(0L);

    }

    // Initialize estimated vector , using CountMin as state
    public void initializeEstimatedVectorCM(FGMConfig config) throws IOException {

        if( this.getTypeNetwork() == NAIVE ){

            // Naive Bayes Classifier(NBC)

            int m = (this.getDatasetSchema().size()-1)*findClassValue(new ArrayList<>(this.getCounters().values()));  // Calculate the m = num_attributes*num_class_values

            int depth = calculateDepth(config.deltaSketch(),m); // Calculate the depth

            int a = this.getDatasetSchema().size(); // Calculate the a = number of attributes which are used on update process

            // Calculate the width
            int width;
            if( config.beta() == 0 ) width = calculateWidth(config.epsilonSketch(),a,config.streamSize(),depth,this.getCounters().values().size());
            else width = calculateWidth(config.epsilonSketch(),a,config.streamSize(),config.beta());

            // Initialize the estimated vector
            setEstimatedVector(new CountMin(depth,width));
        }
        else{

            // Bayesian Network(BN)

            int m = findNumRetCounts(new ArrayList<>(this.getCounters().values())); // Calculate the m = number of retrieved counts on estimation process

            int depth = calculateDepth(config.deltaSketch(),m); // Calculate the depth

            int a = findNumUpdates(new ArrayList<>(this.getCounters().values())); // Calculate a = number of attributes which are used on update process

            // Calculate the width
            int width;
            if( config.beta() == 0 ) width = calculateWidth(config.epsilonSketch(),a,config.streamSize(),depth,this.getCounters().values().size());
            else width = calculateWidth(config.epsilonSketch(),a,config.streamSize(),config.beta());

            // Initialize the estimated vector
            setEstimatedVector(new CountMin(depth,width));
        }

    }

    // Initialize estimated and drift vector , using Vector as state
    public void initializeEstAndDriftVector() throws IOException {

        // Initialize the vector
        setEstimatedVector(new fgm.datatypes.Vector());
        setDriftVector(new fgm.datatypes.Vector());

        for( Counter parameter : this.getCounters().values() ){

            // Get the key and added to vectors

            // Add to vectors
            this.getEstimatedVector().addEntry(parameter.getCounterKey());
            this.getDriftVector().addEntry(parameter.getCounterKey());
        }

        // Update the vectors
        updateEstimatedVector();
        updateDriftVector();
    }

    // Initialize estimated and drift vector , using AGMS as state
    public void initializeEstAndDriftAGMS() throws IOException{

        // Depth and width fixed or calculate ???

        // Initialization
        setEstimatedVector(new AGMS(7,100));
        setDriftVector(new AGMS(7,100));

        // Update the vectors
        updateEstimatedVector();
        updateDriftVector();
    }

    // Reset all the counters of parameters(testing purposes)
    public void resetParameters() throws IOException {
        this.getCounters().values().forEach( counter -> counter.setCounter(0) );
    }

    // Print the worker state
    public void printWorkerState(KeyedCoProcessFunction<?,?,?,?>.Context ctx) throws IOException {

        ctx.output(worker_stats,"\nWorker state : "
                                    + " Worker " + ctx.getCurrentKey().toString()
                                    + " , drift vector : " + this.driftVector.value().toString()
                                    + " , estimated vector : " + this.estimatedVector.value().toString()
                                    + " , safeZone : " + this.getSafeZone().toString()
                                    + " , local counter : " + this.getLocalCounter()
                                    + " , lambda : " + this.getLambda()
                                    + " , quantum : " + this.getQuantum()
                                    + " , zeta : " + this.getZeta()
                                    + " , initial zeta : " + this.getInitialZeta()
                                    + " , total count : " + this.getTotalCount()
                                    + " , lastTs : " + new Timestamp(this.getLastTs())
                                    + " , syncWorker : " + this.getSyncWorker()
                                    + " , number of messages : " + this.getNumOfMessages()
                                    + super.toString());


        // Print the size of counters
        ctx.output(worker_stats,"Worker state : " + " Size of total parameters is : " + this.getCounters().values().size());

        // Print all the counters
        this.getCounters().values().forEach( counter ->  ctx.output(worker_stats,"Worker "+ctx.getCurrentKey()+" : "+counter.toString()));
    }

    @Override
    public String toString(){

        try {
            return  "Worker state : "
                     + " drift vector : " + Arrays.deepToString(this.getDrift())
                     + " , estimated vector : " + Arrays.deepToString(this.getEstimation())
                     + " , local counter : " + this.getLocalCounter()
                     + " , total count : " + this.getTotalCount()
                     + " , lambda : " + this.getLambda()
                     + " , quantum : " + this.getQuantum()
                     + " , zeta : " + this.getZeta()
                     + " , initial zeta : " + this.getInitialZeta()
                     + " , number of messages : " + this.getNumOfMessages()
                     + " , lastTs : " + new Timestamp(this.getLastTs())
                     + " , syncWorker : " + this.getSyncWorker();

        }
        catch (IOException e) { return e.toString(); }

    }

}
