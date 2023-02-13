package fgm.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import config.FGMConfig;
import datatypes.NodeBN_schema;
import datatypes.counter.Counter;
import datatypes.counter.Counters;
import datatypes.input.Input;
import fgm.datatypes.VectorType;
import fgm.safezone.SafeZone;
import fgm.sketch.AGMS;
import fgm.sketch.CountMin;
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

import static config.InternalConfig.ErrorSetup.BASELINE;
import static config.InternalConfig.TypeNetwork.*;
import static config.InternalConfig.TypeState.*;
import static datatypes.counter.Counter.findRefOrphanCounter;
import static fgm.job.JobFGM.coordinator_stats;
import static fgm.sketch.CountMin.calculateDepth;
import static fgm.sketch.CountMin.calculateWidth;
import static fgm.worker.WorkerFunction.getKey;
import static fgm.worker.WorkerFunction.getKeyNBC;
import static state.State.initializeCounters;
import static state.State.initializeCountersNBC;

public class CoordinatorState extends FGMState {

    // Fields
    private final String uid = UUID.randomUUID().toString(); // Unique id for coordinator

    // Protocol fields
    private transient ValueState<VectorType> estimatedVector; // Estimated vector E
    private transient ValueState<Integer> globalCounter; // Global counter c
    private transient ValueState<SafeZone> safeZone; // Safe zone
    private transient ValueState<Double> psi; // Psi
    private transient ValueState<Boolean> syncCord; // This variable using for synchronization of subround and round
    private final double epsilonPsi; // EpsilonPsi

    // Rebalancing
    private final boolean enableRebalancing; // Enable the rebalancing
    private transient ValueState<VectorType> balanceVector; // Balance vector B
    private transient ValueState<Double> psiBeta; // PsiBeta
    private transient ValueState<Double> lambda; // Lambda

    // Statistic fields
    private transient ValueState<Long> numOfSubrounds; // Number of subrounds
    private transient ValueState<Long> numOfRounds; // Number of rounds
    private transient ValueState<Long> numOfMessages; // Number of messages sent by coordinator
    private transient ValueState<Long> numOfSentDriftMessages; // Number of drift messages sent by coordinator
    private transient ValueState<Long> numOfSentQuantumMessages; // Number of quantum messages sent by coordinator
    private transient ValueState<Long> numOfSentZetaMessages; // Number of zeta messages sent by coordinator
    private transient ValueState<Long> numOfSentLambdaMessages; // Number of lambda messages sent by coordinator
    private transient ValueState<Long> numOfSentEstMessages; // Number of estimate messages sent by coordinator - keep only this
    private transient ValueState<Integer> endOfStreamCounter; // Counter of the END_OF_STREAM messages
    private transient ValueState<Long> numOfDriftMessages; // Number of drift messages received from coordinator
    private transient ValueState<Long> numOfIncrementMessages; // Number of increment messages received from coordinator
    private transient ValueState<Long> numOfZetaMessages; // Number of zeta messages received from coordinator

    // Bayesian Network or Classifier
    private transient ValueState<Counters> parameters; // Parameters of Bayesian Network
    private transient ValueState<NodeBN_schema[]> nodes; // The nodes of BN or NBC

    // psi , phi ??? , only psi-done , phi not used-done
    // One counter for updates , not used at the present time => Counter needed for handleZeta,handleDrift ???


    // Constructor
    public CoordinatorState(RuntimeContext ctx, FGMConfig config){

        // Initialize state
        super(config);

        // Initialize coordinator state - FGM
        this.estimatedVector = ctx.getState(new ValueStateDescriptor<>("estimatedVector"+uid, Types.GENERIC(VectorType.class)));
        this.safeZone = ctx.getState(new ValueStateDescriptor<>("safeZone"+uid,Types.POJO(SafeZone.class)));
        this.globalCounter = ctx.getState(new ValueStateDescriptor<>("zeta"+uid, Integer.class));
        this.psi = ctx.getState(new ValueStateDescriptor<>("psi"+uid,Double.class));
        this.syncCord = ctx.getState(new ValueStateDescriptor<>("syncCord"+uid,Boolean.class));

        // Rebalancing fields
        this.balanceVector = ctx.getState(new ValueStateDescriptor<>("balanceVector"+uid, Types.GENERIC(VectorType.class)));
        this.psiBeta = ctx.getState(new ValueStateDescriptor<>("psiBeta"+uid,Double.class));
        this.lambda = ctx.getState(new ValueStateDescriptor<>("lambda"+uid,Double.class));

        // Statistic fields
        this.numOfSubrounds = ctx.getState(new ValueStateDescriptor<>("numOfSubrounds"+uid, Long.class));
        this.numOfRounds = ctx.getState(new ValueStateDescriptor<>("numOfRounds"+uid, Long.class));
        this.numOfMessages = ctx.getState(new ValueStateDescriptor<>("numOfMessages"+uid, Long.class));
        this.numOfSentDriftMessages = ctx.getState(new ValueStateDescriptor<>("numOfSentDriftMessages"+uid, Long.class));
        this.numOfSentQuantumMessages = ctx.getState(new ValueStateDescriptor<>("numOfSentQuantumMessages"+uid, Long.class));
        this.numOfSentZetaMessages = ctx.getState(new ValueStateDescriptor<>("numOfSentZetaMessages"+uid, Long.class));
        this.numOfSentLambdaMessages = ctx.getState(new ValueStateDescriptor<>("numOfSentLambdaMessages"+uid, Long.class));
        this.numOfSentEstMessages = ctx.getState(new ValueStateDescriptor<>("numOfEstMessages"+uid, Long.class));
        this.endOfStreamCounter = ctx.getState(new ValueStateDescriptor<>("endOfStreamCounter"+uid, Integer.class));
        this.numOfDriftMessages = ctx.getState(new ValueStateDescriptor<>("numOfDriftMessages"+uid, Long.class));
        this.numOfIncrementMessages = ctx.getState(new ValueStateDescriptor<>("numOfIncrementMessages"+uid, Long.class));
        this.numOfZetaMessages = ctx.getState(new ValueStateDescriptor<>("numOfZetaMessages"+uid, Long.class));

        // Parameters/counters
        this.parameters = ctx.getState(new ValueStateDescriptor<>("parameters"+uid,Types.GENERIC(Counters.class)));
        this.nodes = ctx.getState(new ValueStateDescriptor<>("nodes"+uid,Types.OBJECT_ARRAY(TypeInformation.of(NodeBN_schema.class))));

        // Constant of FGM
        this.epsilonPsi = config.epsilonPsi();
        this.enableRebalancing = config.enableRebalancing();
    }

    // Getters
    public VectorType getEstimatedVector() throws IOException { return estimatedVector.value(); }
    public SafeZone getSafeZone() throws IOException { return safeZone.value(); }
    public Integer getGlobalCounter() throws IOException { return globalCounter.value(); }
    public Double getPsi() throws IOException { return psi.value(); }
    public double getEpsilonPsi() { return epsilonPsi; }
    public Boolean getSyncCord() throws IOException { return syncCord.value(); }

    // Rebalancing
    public VectorType getBalanceVector() throws IOException { return balanceVector.value(); }
    public Double getPsiBeta() throws IOException { return psiBeta.value(); }
    public Double getLambda() throws IOException { return lambda.value(); }
    public boolean isEnableRebalancing() { return enableRebalancing; }

    // Bayesian Network or Classifier
    public NodeBN_schema[] getNodes() throws IOException { return nodes.value(); }
    public Counters getParameters() throws IOException { return parameters.value(); }
    public Counter getCounter(long key) throws IOException { return parameters.value().getCounters().get(key); }
    public LinkedHashMap<Long, Counter> getCounters() throws IOException { return parameters.value().getCounters(); }

    // Statistics
    public Long getNumOfSubrounds() throws IOException { return numOfSubrounds.value(); }
    public Long getNumOfRounds() throws IOException { return numOfRounds.value(); }
    public Long getNumOfMessages() throws IOException { return numOfMessages.value(); }
    public Integer getEndOfStreamCounter() throws IOException { return endOfStreamCounter.value(); }
    public Long getNumOfDriftMessages() throws IOException { return numOfDriftMessages.value(); }
    public Long getNumOfIncrementMessages() throws IOException { return numOfIncrementMessages.value(); }
    public Long getNumOfZetaMessages() throws IOException { return numOfZetaMessages.value(); }
    public Long getNumOfSentDriftMessages() throws IOException { return numOfSentDriftMessages.value(); }
    public Long getNumOfSentQuantumMessages() throws IOException { return numOfSentQuantumMessages.value(); }
    public Long getNumOfSentZetaMessages() throws IOException { return numOfSentZetaMessages.value(); }
    public Long getNumOfSentLambdaMessages() throws IOException { return numOfSentLambdaMessages.value(); }
    public Long getNumOfSentEstMessages() throws IOException { return numOfSentEstMessages.value(); }

    public double[][] getEstimation() throws IOException { return estimatedVector.value().get(); }
    public double[][] getBalance() throws IOException { return  balanceVector.value().get(); }

    // Setters
    public void setEstimatedVector(VectorType estimatedVector) throws IOException { this.estimatedVector.update(estimatedVector); }
    public void setEstimatedVector(double[][] estimatedVector) throws IOException { this.estimatedVector.value().set(estimatedVector); }
    public void setSafeZone(SafeZone safeZone) throws IOException { this.safeZone.update(safeZone); }
    public void setGlobalCounter(Integer globalCounter) throws IOException { this.globalCounter.update(globalCounter); }
    public void setPsi(Double psi) throws IOException { this.psi.update(psi); }
    public void setSyncCord(Boolean syncCord) throws IOException { this.syncCord.update(syncCord); }

    // Rebalancing
    public void setBalanceVector(VectorType balanceVector) throws IOException { this.balanceVector.update(balanceVector); }
    public void setBalanceVector(double[][] balanceVector) throws IOException { this.balanceVector.value().set(balanceVector); }
    public void setPsiBeta(Double psiBeta) throws IOException { this.psiBeta.update(psiBeta); }
    public void setLambda(Double lambda) throws IOException { this.lambda.update(lambda); }

    // Bayesian Network or Classifier
    public void setParameters(Counters parameters) throws IOException { this.parameters.update(parameters); }
    public void setNodes(NodeBN_schema[] nodes) throws IOException { this.nodes.update(nodes); }

    // Statistics
    public void setNumOfSubrounds(Long numOfSubrounds) throws IOException { this.numOfSubrounds.update(numOfSubrounds); }
    public void setNumOfRounds(Long numOfRounds) throws IOException { this.numOfRounds.update(numOfRounds); }
    public void setNumOfMessages(Long numOfMessages) throws IOException { this.numOfMessages.update(numOfMessages); }
    public void setEndOfStreamCounter(Integer endOfStreamCounter) throws IOException { this.endOfStreamCounter.update(endOfStreamCounter); }
    public void setNumOfDriftMessages(Long numOfDriftMessages) throws IOException { this.numOfDriftMessages.update(numOfDriftMessages); }
    public void setNumOfIncrementMessages(Long numOfIncrementMessages) throws IOException { this.numOfIncrementMessages.update(numOfIncrementMessages); }
    public void setNumOfZetaMessages(Long numOfZetaMessages) throws IOException { this.numOfZetaMessages.update(numOfZetaMessages); }
    public void setNumOfSentDriftMessages(Long numOfSentDriftMessages) throws IOException { this.numOfSentDriftMessages.update(numOfSentDriftMessages); }
    public void setNumOfSentQuantumMessages(Long numOfSentQuantumMessages) throws IOException { this.numOfSentQuantumMessages.update(numOfSentQuantumMessages); }
    public void setNumOfSentZetaMessages(Long numOfSentZetaMessages) throws IOException { this.numOfSentZetaMessages.update(numOfSentZetaMessages); }
    public void setNumOfSentLambdaMessages(Long numOfSentLambdaMessages) throws IOException { this.numOfSentLambdaMessages.update(numOfSentLambdaMessages); }
    public void setNumOfSentEstMessages(Long numOfEstMessages) throws IOException { this.numOfSentEstMessages.update(numOfEstMessages); }


    // Methods
    public void updateEstimatedVector() throws IOException { this.setEstimatedVector(this.getEstimatedVector()); }
    public void updateBalanceVector() throws IOException { this.setBalanceVector(this.getBalanceVector()); }
    public void updateParameters() throws IOException { this.setParameters(this.getParameters()); }
    public NodeBN_schema getNode(int index) throws IOException { return nodes.value()[index]; }

    // Update parameters(testing purposes)
    public void updateParameters(Counters parameters) throws IOException {

        for (long key : this.getCounters().keySet()){
            Counter tmp = this.getCounter(key);
            tmp.setCounter(tmp.getCounter()+parameters.getCounters().get(key).getCounter());
            this.getCounters().put(key,tmp);
        }
    }

    // Reset the counters of parameters(testing purposes)
    public void resetCounters() throws IOException {
        this.getCounters().values().forEach( parameter -> parameter.setCounter(0) );
    }

    // Get parameter from list of parameters based on index
    public Counter getParameter(long key) throws IOException { return this.getCounters().get(key); }

    // Get the size of parameters
    public int getSizeParameters() throws IOException { return this.getCounters().values().size(); }

    // Reset the field true parameter of parameters
    public void resetTrueParameters() throws IOException {
        this.getCounters().values().forEach( parameter -> parameter.setTrueParameter(0) );
    }

    // Initialize the coordinator state
    public void initializeCoordinatorState(FGMConfig config) throws IOException {

        // Initialize parameters
        ObjectMapper objectMapper = new ObjectMapper();

        // Unmarshall JSON object as array
        NodeBN_schema[] nodes;
        if( config.BNSchema().contains(",") ){ nodes = objectMapper.readValue(config.BNSchema(), NodeBN_schema[].class); } // Read value from string
        else { nodes = objectMapper.readValue(new File(config.BNSchema()), NodeBN_schema[].class); } // Read value from file or url

        // Initialize the nodes
        setNodes(nodes);

        // Initialize the number of nodes of BN
        setNumOfNodes(nodes.length);

        // Initialize counters a.k.a parameters
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
        else{
            for (NodeBN_schema node : nodes) { counters_bn.addAll(initializeCounters(node)); }
        }

        // Initialize the key of the parameters
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

        // Initialize the estimated and balance vector based on state type
        if( config.typeState() == VECTOR ){ initializeEstAndBalVector(); }
        else if( config.typeState() == COUNT_MIN ){

            // Initialize the estimated vector
            initializeEstimatedVectorCM(config);
            this.getEstimatedVector().resetVector(); // Reset the vector
            this.updateEstimatedVector(); // Update the vector

            // Get the estimated vector
            CountMin cm = (CountMin) this.getEstimatedVector();

            // Initialize the balanced vector
            setBalanceVector(new CountMin(cm.depth(),cm.width()));
            this.getBalanceVector().resetVector(); // Reset the balance vector
            this.updateBalanceVector(); // Update the balance vector
        }
        else{
            // AGMS sketch
            initializeEstAndBalAGMS();
        }

        // Initialize the safe zone
        if( config.errorSetup() == BASELINE ){ setSafeZone(new SafeZone(this.baseline())); }
        else{ setSafeZone(new SafeZone(this.uniform())); }

        // Initialize the rest of the fields of rebalancing
        setPsiBeta(0d);
        setLambda(1d);


        // Collect some statistics
        System.out.println( "TypeNetwork : " + this.getTypeNetwork()
                            + " , typeState  : " + this.getTypeState()
                            + " , numOfWorkers : " + this.getNumOfWorkers()
                            + " , eps : " + this.getEps()
                            + " , delta : " + this.getDelta()
                            + " , errorSetup : " + this.getErrorSetup()
                            + " , enableRebalancing : " + this.isEnableRebalancing()
                            + " , epsilonPsi : " + this.getEpsilonPsi() );

        if( config.typeState() == COUNT_MIN ){
            System.out.println("beta : " + config.beta()
                               + " streamSize : " + config.streamSize()
                               + " , epsilonSketch : " + config.epsilonSketch()
                               + " , deltaSketch : " + config.deltaSketch());
        }


        // Initialize the rest of the variables
        setGlobalCounter(0);
        setNumOfRounds(0L);
        setNumOfSubrounds(0L);
        setNumOfMessages(0L);
        setPsi(0d);
        setEndOfStreamCounter(0);
        setNumOfDriftMessages(0L);
        setNumOfIncrementMessages(0L);
        setNumOfZetaMessages(0L);
        setNumOfSentDriftMessages(0L);
        setNumOfSentEstMessages(0L);
        setNumOfSentLambdaMessages(0L);
        setNumOfSentQuantumMessages(0L);
        setNumOfSentZetaMessages(0L);
        setSyncCord(false);

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
            if( config.beta() == 0) width = calculateWidth(config.epsilonSketch(),a,config.streamSize(),depth,this.getCounters().values().size());
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
            if( config.beta() == 0 ) width = calculateWidth(config.epsilonSketch(),a,config.streamSize(),depth,getCounters().values().size());
            else width = calculateWidth(config.epsilonSketch(),a,config.streamSize(),config.beta());

            // Initialize the estimated vector
            setEstimatedVector(new CountMin(depth,width));
        }

    }

    // Initialize estimated and balance vector , using Vector as state
    public void initializeEstAndBalVector() throws IOException {

        // Initialize the vectors
        setEstimatedVector(new fgm.datatypes.Vector());
        setBalanceVector(new fgm.datatypes.Vector());

        for(Counter parameter : this.getCounters().values()){

            // Get the key and added to vectors

            // Add to vectors
            this.getEstimatedVector().addEntry(parameter.getCounterKey());
            this.getBalanceVector().addEntry(parameter.getCounterKey());
        }

        // Update the vectors
        updateEstimatedVector();
        updateBalanceVector();

    }

    // Initialize estimated and balance vector , using AGMS sketch as state
    public void initializeEstAndBalAGMS() throws IOException{

        // Depth and width fixed or calculate ???

        // Initialization
        setEstimatedVector(new AGMS(7,100));
        setBalanceVector(new AGMS(7,100));

        // Update the vectors
        updateEstimatedVector();
        updateBalanceVector();
    }

    // Find the values of class node of Naive Bayes Classifier(NBC)
    public static int findClassValue(List<Counter> parameters){

        int numClass = 0;

        for (Counter parameter : parameters){
            // Check the parameter
            if(parameter.getNodeParents() == null && parameter.getNodeParentValues() == null) numClass++;
        }

        return numClass;
    }

    // Find the number of updates per input of Bayesian Network(BN)
    public static int findNumUpdates(List<Counter> parameters){

        int totalUpdates = 0;
        List<String> names = new ArrayList<>();

        String name;

        for(Counter parameter : parameters){

            // Get the name
            name = parameter.concatParNames();

            // Check for actual node only
            if(parameter.isActualNode() && !names.contains(name)){

                totalUpdates++;

                // Check if exist parents
                if(parameter.getNodeParents() != null && parameter.getNodeParentValues() != null ) totalUpdates++;

                // Add the concatenated name
                names.add(name);
            }
        }

        return totalUpdates;
    }

    // Find the number of retrieved counts on estimation process of Bayesian Network(BN)
    public static int findNumRetCounts(List<Counter> parameters){

        int totalRetCounts = 0;
        List<String> names = new ArrayList<>();

        String name;

        for(Counter parameter : parameters){

            // Get the name
            name = parameter.concatParNames();

            // Check for actual node only
            if(parameter.isActualNode() && !names.contains(name)){

                totalRetCounts++;

                // Check if exist parents
                if(parameter.getNodeParents() != null && parameter.getNodeParentValues() != null ) totalRetCounts++;
                else{ totalRetCounts += findRefOrphanCounter(parameters,parameter); }

                // Add the concatenated name
                names.add(name);
            }
        }

        return totalRetCounts;
    }

    // Check if the queries are ready that is if fulfill the appropriate size
    public static boolean AreQueriesReady(ListState<Input> state, long sizeOfQueries) throws Exception {

        // Get queries
        List<Input> queries = (List<Input>) state.get();
        return queries.size() == sizeOfQueries;
    }

    // Print coordinator state
    public void printCoordinatorState(CoProcessFunction<?,?,?>.Context ctx) throws IOException {

        ctx.output(coordinator_stats, "Coordinator state : "
                                        + " estimated vector : " + this.estimatedVector.value().toString()
                                        + " , balance vector : " + this.balanceVector.value().toString()
                                        + " , safe zone : " + this.safeZone.value().toString()
                                        + " , epsilonPsi : " + this.getEpsilonPsi()
                                        + " , enableRebalancing : " + this.isEnableRebalancing()
                                        + " , globalCounter : " + this.getGlobalCounter()
                                        + " , psi : " + this.getPsi()
                                        + " , psiBeta : " + this.getPsiBeta()
                                        + " , lambda : " + this.getLambda()
                                        + " , syncCord : " + this.getSyncCord()
                                        + " , number of subrounds : " + this.getNumOfSubrounds()
                                        + " , number of rounds : " + this.getNumOfRounds()
                                        + " , number of messages : " + this.getNumOfMessages()
                                        + super.toString());

        // Print the size of counters
        ctx.output(coordinator_stats,("Coordinator state : " + " Size of total parameters is : " + this.getCounters().values().size()));

        // Print all the counters
        this.getCounters().values().forEach( counter -> ctx.output(coordinator_stats,counter.toString()));
    }

    @Override
    public String toString(){

        try {
            return  "Coordinator state : "
                    + " estimated vector : " + Arrays.deepToString(this.getEstimation())
                    + " , balance vector : " + Arrays.deepToString(this.getBalance())
                    + " , globalCounter : " + this.getGlobalCounter()
                    + " , psi : " + this.getPsi()
                    + " , psiBeta : " + this.getPsiBeta()
                    + " , lambda : " + this.getLambda()
                    + " , number of subrounds : " + this.getNumOfSubrounds()
                    + " , number of rounds : " + this.getNumOfRounds()
                    + " , number of messages : " + this.getNumOfMessages()
                    + " , syncCord : " + this.getSyncCord();
        }
        catch (IOException e) { return e.toString(); }
    }

}
