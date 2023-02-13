package state;

import com.fasterxml.jackson.databind.ObjectMapper;
import config.CBNConfig;
import datatypes.counter.Counter;
import datatypes.NodeBN_schema;
import net.openhft.hashing.LongHashFunction;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static config.InternalConfig.ErrorSetup;
import static config.InternalConfig.SetupCounter;
import static config.InternalConfig.TypeCounter;
import static config.InternalConfig.TypeNetwork;
import static datatypes.NodeBN_schema.*;
import static datatypes.NodeBN_schema.findCardinalities;
import static state.State.TypeNode.COORDINATOR;
import static utils.MathUtils.*;
import static utils.Util.convertToString;


public class State implements Serializable {

    // RANDOMIZED COUNTERS(RC)
    // Workers fields
    // Fields of Counter class: name,value,actualNode,trueParameter,nodeParents,nodeParentValues,counter_value
    // Fields : epsilon,delta,resConstant
    // Field prob : Corresponds to the probability of the counter
    // Field lastTs : Last timestamp received from input source
    // Field counter : Corresponds to the local counter-ni
    // Field counterDoubles : Used to check when the local counter doubles
    // Field lastBroadcastValue : Corresponds to the last broadcast value from Coordinator for a specific counter-n_dash
    // Field lastSentValue : Corresponds to the last sent value from Worker for a specific counter-ni_dash
    // Field numMessages : Corresponds to the number of messages sent from Worker for a specific counter
    // Field sync : Used for synchronization for each counter

    // Field last sent values,last sent updates not used from Worker side

    // Coordinator fields
    // Fields of Counter class: name,value,actualNode,trueParameter,nodeParents,nodeParentValues,counter_value
    // Fields : epsilon,delta,resConstant
    // Field prob : Corresponds to the probability of the counter
    // Field lastBroadcastValue : Corresponds to the last broadcast value sent from Coordinator for a specific counter-n_dash
    // Field numMessages : Corresponds to the number of rounds for a specific counter
    // Field last sent values :  Corresponds to last sent value per worker-ni_dash
    // Field last sent updates : Corresponds to the last sent update(whenever the local counter doubles) per worker-ni'
    // Field sync : Used for synchronization for each counter
    // Field counter : Acts as the temporary counter of COUNTER messages from Workers

    // Fields lastTs,counterDoubles,lastSentValue not used from Coordinator side


    // DETERMINISTIC COUNTERS(DC)
    // Workers fields
    // Fields of Counter class: name,value,actualNode,trueParameter,nodeParents,nodeParentValues,counter_value
    // Fields : epsilon,delta
    // Field lastTs : Last timestamp received from input source for a specific counter
    // Field counter : Corresponds to the total value from a specific counter
    // Field lastBroadcastValue : Corresponds to the last broadcast value from Coordinator for a specific counter
    // Field lastSentValue : Corresponds to the local drift from a specific counter
    // Field numMessages : Corresponds to the number of messages sent from Worker for a specific counter
    // Field sync : Used for synchronization for each counter

    // Field counterDoubles not used from Worker side
    // Field prob,resConstant,last sent values,last sent updates not used from DC

    // Coordinator fields
    // Fields of Counter class: name,value,actualNode,trueParameter,nodeParents,nodeParentValues,counter_value
    // Fields : epsilon,delta
    // Field counter : Corresponds to the estimator from a specific counter
    // Field counterDoubles : Corresponds to the number of rounds for a specific counter
    // Field lastBroadcastValue : Corresponds to the last broadcast value from Coordinator for a specific counter
    // Field lastSentValue : Acts as temporary last broadcast value until all DRIFT messages from Workers reach to the Coordinator
    // Field numMessages : Acts as the temporary counter of INCREMENT/DRIFT messages from Workers
    // Field sync : Used for synchronization for each counter

    // Field lastTs not used from Coordinator side
    // Field prob,resConstant,last sent values,last sent updates not used from DC


    // EXACT
    // Workers fields
    // Fields of Counter class: name,value,actualNode,trueParameter,nodeParents,nodeParentValues,counter_value
    // Field lastSentValue : Corresponds to the last sent value from Worker for a specific counter
    // Field numMessages : Corresponds to the number of messages sent from Worker for a specific counter

    // Coordinator fields
    // Fields of Counter class: name,value,actualNode,trueParameter,nodeParents,nodeParentValues,counter_value
    // Field last sent values :  Corresponds to last sent value per worker


    private final LinkedHashMap<String,Integer> datasetSchema; // The schema of dataset

    private final ErrorSetup errorSetup; // Configure the error setup about counters
    private final SetupCounter setupCounter; // Configure the initial state about counters
    private final TypeCounter typeCounter; // Type of counter
    private final TypeNetwork typeNetwork; // Type of network
    private final boolean dummyFather; // Enable/disable the mechanism of Dummy Father

    private final int numOfWorkers; // The number of workers-sites
    private final double eps; // Epsilon(input parameter)
    private final double delta; // Delta(input parameter)
    private final long condition; // Starting condition(input parameter-refer to deterministic and randomized counters)

    private long numOfNodes; // Correspond to the number of nodes of Bayesian Network

    // Constructors
    public State(CBNConfig config){

        // The schema of dataset
        datasetSchema = initializeDatasetSchema(config.datasetSchema());

        // Internal configuration
        errorSetup = config.errorSetup();
        setupCounter = config.setupCounter();
        typeCounter = config.typeCounter();
        typeNetwork = config.typeNetwork();
        dummyFather = config.dummyFather();

        // User configuration
        numOfWorkers = config.workers();
        eps = config.epsilon();
        delta = config.delta();
        condition = config.condition();
    }

    // Getters
    public LinkedHashMap<String,Integer> getDatasetSchema() { return datasetSchema; }
    public ErrorSetup getErrorSetup() { return errorSetup; }
    public SetupCounter getSetupCounter() { return setupCounter; }
    public TypeCounter getTypeCounter() { return typeCounter; }
    public TypeNetwork getTypeNetwork() { return typeNetwork; }
    public int getNumOfWorkers() { return numOfWorkers; }
    public double getEps() { return eps; }
    public double getDelta() { return delta; }
    public long getCondition() { return condition; }
    public long getNumOfNodes() { return numOfNodes; }
    public boolean getDummyFather() { return dummyFather; }

    // Setters
    public void setNumOfNodes(long numOfNodes) { this.numOfNodes = numOfNodes; }


    // Methods

    // Initialize the schema of dataset as Linked HashMap<String,Integer>
    // The key correspond to each node/attribute of dataset and value correspond to the position of node/attribute in schema
    public static LinkedHashMap<String,Integer> initializeDatasetSchema(String datasetSchema){

        LinkedHashMap<String,Integer> schema = new LinkedHashMap<>();
        int pos = 0;
        for(String node : datasetSchema.split(",")){

            // Update the schema
            schema.put(node,pos);

            // Update the position
            pos++;
        }
        return schema;
    }

    // Initialize all the counters(fields of Counter class).This function used for Bayesian Network(BN)
    public static ArrayList<Counter> initializeCounters(NodeBN_schema node){

        ArrayList<Counter> counters = new ArrayList<>();
        Collection<List<String>> permutations = null;
        ArrayList<String> name_parents = null;
        List<Collection<String>> values;

        boolean first_time = true;
        boolean hasParents = false;

        // Check if exists parent nodes
        if(node.getParents().length != 0){
            hasParents = true;
            name_parents = getNameParents(node); // Name of parents nodes
            values = getValueParents(node); // The values of parents nodes
            permutations = generatePermutations(values); // Generate the permutations
        }

        // For each value of node , generate the corresponding counters
        for(int i=0;i<node.getValues().length;i++){

            // If exists parents
            if(hasParents){

                // The index of permutation
                int permutationIndex = 0;

                // Generate the counters
                for (List<String> perm : permutations) {

                    // Get the permutation
                    LinkedList<String> permutation = (LinkedList<String>) perm;

                    // Generate the counter
                    Counter counter = generateCounter(node,i,permutationIndex,true,name_parents,permutation,true);

                    // Add to counters
                    counters.add(counter);

                    if(first_time){

                        // Generate the parent node only one time per node
                        Counter parent_counter = generateCounter(node, -1,-1,false, name_parents, permutation,true);

                        // Add to counters
                        counters.add(parent_counter);
                    }

                    // Increment the index
                    permutationIndex++;
                }

                first_time = false;
            }
            else{

                // Generate the counter
                Counter counter = generateCounter(node,i,-1,true,null,null,true);

                // Add to counters
                counters.add(counter);

            }

        }

        return counters;
    }

    // Initialize the counters/parameters(fields of Counter class) for Naive Bayes Classifier(NBC)
    public static ArrayList<Counter> initializeCountersNBC(NodeBN_schema node,NodeBN_schema classNode){

        ArrayList<Counter> counters = new ArrayList<>();
        ArrayList<String> name_parents = new ArrayList<>();
        int classValueIndex;  // The index of class value

        // Check if node is null => We have got the class node
        if(node == null){

            // For each class value generate the corresponding counter
            for (int i=0;i<classNode.getValues().length;i++) {

                // Generate the counter
                Counter counter = generateCounter(classNode,i,-1,true,null,null,false);

                // Add to counters
                counters.add(counter);
            }

            return counters;
        }

        // Get the class name
        name_parents.add(classNode.getName());

        // For each value of node , generate the corresponding counters
        for(int i=0;i<node.getValues().length;i++){

            // The index of class value
            classValueIndex = 0;

            // Generate the counters for each attribute value and class value
            for (String classValue : classNode.getValues()) {

                // Generate the counter
                LinkedList<String> value = new LinkedList<>(); value.add(classValue);
                Counter counter = generateCounter(node,i,classValueIndex,true,name_parents,value,false);

                // Add to counters
                counters.add(counter);

                // Increment the index
                classValueIndex++;
            }

        }

        return counters;
    }

    // The value of typeBN argument declare if the generated counter parameter used to Bayesian Network(BN) or Naive Bayes Classifiers(NBC)
    // typeBN = true for BN and typeBN = false for NBC
    public static Counter generateCounter(NodeBN_schema node,
                                          int valueIndex,
                                          int permutationIndex,
                                          boolean actualNode,
                                          ArrayList<String> parentsName,
                                          LinkedList<String> parentsValues,
                                          boolean typeBN){

        // Counter
        Counter counter = new Counter();

        // Marked as actual node
        counter.setActualNode(actualNode);

        // Parent counter
        if(valueIndex == -1){

            // Set the name of node
            counter.setNodeName(parentsName);

            // Set the value of node
            counter.setNodeValue(new ArrayList<>(parentsValues));

            // Set the name of parents node
            ArrayList<String> name_parents = new ArrayList<>();
            name_parents.add(node.getName());
            counter.setNodeParents(name_parents);

            return counter;
        }

        // Set the name of node
        ArrayList<String> name = new ArrayList<>();
        name.add(node.getName());
        counter.setNodeName(name);

        // Set the value of node
        ArrayList<String> value = new ArrayList<>();
        value.add(node.getValues()[valueIndex]);
        counter.setNodeValue(value);

        // Set the name and values of parents node
        counter.setNodeParents(parentsName);
        if(parentsValues != null) counter.setNodeParentValues(new ArrayList<>(parentsValues));

        // Set the trueParameter
        if(typeBN) {
            if (parentsValues != null) {
                String[] probValues = node.getTrueParameters()[valueIndex].split(",");
                counter.setTrueParameter(Double.parseDouble(probValues[permutationIndex]));
            } else {
                counter.setTrueParameter(Double.parseDouble(node.getTrueParameters()[valueIndex]));
            }
        }
        return counter;

    }

    // Initialize all the actual counters except of parent counters of Bayesian Network(BN)
    public ArrayList<Counter> initializeActualCounters(NodeBN_schema node){

        ArrayList<Counter> counters = new ArrayList<>();
        Collection<List<String>> permutations = null;
        ArrayList<String> name_parents = null;
        List<Collection<String>> values;

        boolean hasParents = false;

        // Check if exists parent nodes
        if(node.getParents().length != 0){
            hasParents = true;
            name_parents = getNameParents(node); // Name of parents nodes
            values = getValueParents(node); // The values of parents nodes
            permutations = generatePermutations(values); // Generate the permutations
        }

        // For each value of node , generate the corresponding counters
        for(int i=0;i<node.getValues().length;i++){

            // If exists parents
            if(hasParents){

                // The index of permutation
                int permutationIndex = 0;

                // Generate the counters
                for (List<String> perm : permutations) {

                    // Get the permutation
                    LinkedList<String> permutation = (LinkedList<String>) perm;

                    // Generate the counter
                    Counter counter = generateCounter(node,i,permutationIndex,true,name_parents,permutation,true);

                    // Add to counters
                    counters.add(counter);

                    // Increment the index
                    permutationIndex++;
                }
            }
            else{

                // Generate the counter
                Counter counter = generateCounter(node,i,-1,true,null,null,true);

                // Add to counters
                counters.add(counter);

            }

        }

        return counters;
    }


    // Initialize the counters/parameters(BN or NBC) based on type of counter(fields of Randomized and CoordinatorCounter class)
    public void initializeRandomizedCounter(List<Counter> counters, NodeBN_schema[] nodes,TypeNode typeNode){

        // Calculate the epsilon based on ErrorSetup
        double eps = 0;
        double alpha=0,beta = 0;
        if( errorSetup == ErrorSetup.BASELINE ){ eps = baseline(); }
        else if( errorSetup == ErrorSetup.UNIFORM ){ eps = uniform(); }
        else{ // NON_UNIFORM

            ArrayList<Double> nums;
            if(dummyFather) nums = calculateAlphaAndBeta(nodes); // Dummy Father enable
            else nums = calculateAlphaAndBetaOrphanNodes(nodes); // Dummy Father disable

            alpha = nums.get(0);
            beta = nums.get(1);
        }

        // Initialize the counters
        for(Counter counter : counters){

            // Initialize the epsilon based on ErrorSetup
            if( eps != 0 ){ counter.setEpsilon(eps); } // BASELINE and UNIFORM
            else{

                // Cardinalities(Ji and Ki)
                ArrayList<Integer> cardinalities;

                // NON_UNIFORM
                if( counter.isActualNode() ){ // Actual node
                    cardinalities = findCardinalities(nodes,counter.getNodeParents(),counter.getNodeName()); // Find the Ji and Ki
                    if(dummyFather){
                        // If node has not parents(Ki=0) then set the Ki=1 => Dummy Father
                        if( cardinalities.get(1) == 0 ) cardinalities.set(1,1);
                    }
                    counter.setEpsilon(nonUniform(cardinalities.get(0),cardinalities.get(1),alpha,0,false));
                }
                else{ // Parent node
                    ArrayList<String> nodeNames = counter.getNodeName();
                    cardinalities = findCardinalities(nodes,nodeNames,null); // Find the Ki
                    counter.setEpsilon(nonUniform(cardinalities.get(0),cardinalities.get(1),0,beta,true));
                }
            }

            // Initialize the delta and rescaling constant
            counter.setDelta(delta);
            counter.setResConstant(1/Math.sqrt(delta));

            // Initialize the rest of the fields based on SetupCounter
            if( setupCounter == SetupCounter.ZERO ){ initializeZeroCounter(counter,typeNode); }
            else if( setupCounter == SetupCounter.HALVED ){ initializeHalvedRandomizedCounter(counter,typeNode); }
            else if( setupCounter == SetupCounter.SUB_TRIPLED ){ initializeConditionedRandomizedCounter(counter,typeNode,3); }
            else if( setupCounter == SetupCounter.SUB_QUADRUPLED ){ initializeConditionedRandomizedCounter(counter,typeNode,4); }
            else if( setupCounter == SetupCounter.SUB_QUINTUPLED ){ initializeConditionedRandomizedCounter(counter,typeNode,5); }
            else if( setupCounter == SetupCounter.CONDITION ){ initializeConditionedRandomizedCounter(counter,typeNode,condition); }
            else{ initializeZeroCounter(counter,typeNode); }

        }

    }
    public void initializeExactCounter(List<Counter> counters,TypeNode typeNode){

        // Initialize the counters
        for(Counter counter : counters){

            // Initialize epsilon,delta and rescaling constant
            counter.setEpsilon(0);
            counter.setDelta(0);
            counter.setResConstant(0);

            // Initialize the rest of the fields of counter
            initializeZeroCounter(counter,typeNode);
        }

    }
    public void initializeContinuousCounter(List<Counter> counters,TypeNode typeNode){

        // Initialize the counters
        for(Counter counter : counters){

            // Initialize epsilon,delta and rescaling constant
            counter.setEpsilon(this.getEps());
            counter.setDelta(0);
            counter.setResConstant(0);

            // Initialize the rest of the fields of counter
            initializeZeroCounter(counter,typeNode);
        }
    }
    public void initializeDeterministicCounter(List<Counter> counters,NodeBN_schema[] nodes,TypeNode typeNode){

        double eps = 0d;
        if( errorSetup == ErrorSetup.BASELINE ){ eps = baseline(); }
        else if( errorSetup == ErrorSetup.UNIFORM ){ eps = uniform(); }
        else{ nonUniform(counters,nodes); }

        // Initialize the counters
        for(Counter counter : counters){

            // Initialize epsilon,delta and rescaling constant based on errorSetup
            if( eps != 0 ){ counter.setEpsilon(eps); }
            counter.setDelta(0);
            counter.setResConstant(0);

            // Initialize the rest of the fields of counter
            if( setupCounter == SetupCounter.ZERO ){ initializeZeroCounter(counter,typeNode); }
            else if( setupCounter == SetupCounter.HALVED ){ initializeHalvedDeterministicCounter(counter,typeNode); }
            else if( setupCounter == SetupCounter.SUB_TRIPLED ){ initializeConditionedDeterministicCounter(counter,typeNode,3); }
            else if( setupCounter == SetupCounter.SUB_QUADRUPLED ){ initializeConditionedDeterministicCounter(counter,typeNode,4); }
            else if( setupCounter == SetupCounter.SUB_QUINTUPLED ){ initializeConditionedDeterministicCounter(counter,typeNode,5); }
            else if( setupCounter == SetupCounter.CONDITION ){ initializeConditionedDeterministicCounter(counter,typeNode,condition); }
            else{ initializeZeroCounter(counter,typeNode); }
        }
    }


    // Initialize the counters of BN based on setup of counter
    public void initializeZeroCounter(Counter counter,TypeNode typeNode){

        if( typeNode == COORDINATOR ){

            counter.setLastSentValues(new HashMap<>());
            counter.setLastSentUpdates(new HashMap<>());

            // Initialize the fields of CoordinatorCounter class
            for(int i=0;i<numOfWorkers;i++){
                counter.setLastSentValues(String.valueOf(i),0L);
                counter.setLastSentUpdates(String.valueOf(i),0L);
            }

        }

        counter.setCounter(0L); // Initialize the value of counter
        counter.setCounterDoubles(0L); // Initialize the value of counterDoubles
        counter.setLastBroadcastValue(0L);  // Initialize the value of lastBroadcastValue
        counter.setLastSentValue(0L); // Initialize the lastSentValue
        counter.setNumMessages(0L); // Initialize the number of messages
        counter.setProb(1.0d); // Initialize probability
        counter.setLastTs(0); // Initialize the last timestamp
        counter.setSync(true); // Initialize sync

    }

    /*public void initializeHalvedCounter(Counter counter,TypeNode typeNode){

        long valueHalved = findValueProbHalved(counter.getEpsilon(),numOfWorkers,counter.getResConstant());

        if( typeNode == COORDINATOR ){

            counter.setLastSentValues(new HashMap<>());
            counter.setLastSentUpdates(new HashMap<>());

            // Initialize the fields of CoordinatorCounter class
            for(int i=0;i<numOfWorkers;i++){
                counter.setLastSentValues(String.valueOf(i),valueHalved/numOfWorkers);
                counter.setLastSentUpdates(String.valueOf(i),valueHalved/numOfWorkers);
            }

        }

        counter.setCounter(0L); // Initialize the value of counter
        counter.setCounterDoubles(valueHalved/numOfWorkers); // Initialize the value of counterDoubles
        counter.setLastBroadcastValue(valueHalved);  // Initialize the value of lastBroadcastValue
        counter.setLastSentValue(counter.getCounterDoubles()); // Initialize the lastSentValue
        counter.setNumMessages(counter.getLastSentValue()); // Initialize the number of messages
        counter.setProb(1.0/ highestPowerOf2(counter.getEpsilon(),counter.getLastBroadcastValue(),numOfWorkers,counter.getResConstant())); // Initialize probability

        counter.setLastTs(0); // Initialize the last timestamp
        counter.setSync(true); // Initialize sync

    }*/
    public void initializeHalvedRandomizedCounter(Counter counter,TypeNode typeNode){

        long valueHalved = findValueProbHalved(counter.getEpsilon(),numOfWorkers,counter.getResConstant());

        if( typeNode == COORDINATOR ){

            counter.setLastSentValues(new HashMap<>());
            counter.setLastSentUpdates(new HashMap<>());

            // Initialize the fields of CoordinatorCounter class
            for(int i=0;i<numOfWorkers;i++){
                counter.setLastSentValues(String.valueOf(i),valueHalved/numOfWorkers);
                counter.setLastSentUpdates(String.valueOf(i),valueHalved/numOfWorkers);
            }

        }

        if( typeNode == COORDINATOR ){
            counter.setCounter(0L); // Initialize the value of counter
            counter.setCounterDoubles(0); // Initialize the value of counterDoubles
            counter.setLastSentValue(0); // Initialize the lastSentValue
            counter.setNumMessages((long) log2(valueHalved)); // Initialize the number of rounds
        }
        else{
            counter.setCounter(valueHalved/numOfWorkers); // Initialize the value of counter
            counter.setCounterDoubles(valueHalved/numOfWorkers); // Initialize the value of counterDoubles
            counter.setLastSentValue(counter.getCounterDoubles()); // Initialize the lastSentValue
            // counter.setNumMessages(counter.getLastSentValue()); // Initialize the number of messages
            counter.setNumMessages(0); // Initialize the number of messages
        }

        counter.setLastBroadcastValue(valueHalved);  // Initialize the value of lastBroadcastValue
        counter.setProb(1.0/ highestPowerOf2(counter.getEpsilon(),counter.getLastBroadcastValue(),numOfWorkers,counter.getResConstant())); // Initialize probability

        counter.setLastTs(0); // Initialize the last timestamp
        counter.setSync(true); // Initialize sync

    }
    public void initializeConditionedRandomizedCounter(Counter counter,TypeNode typeNode,long condition){

        long value = findValueProbConditioned(counter.getEpsilon(),numOfWorkers,counter.getResConstant(),condition);

        if( typeNode == COORDINATOR ){

            counter.setLastSentValues(new HashMap<>());
            counter.setLastSentUpdates(new HashMap<>());

            // Initialize the fields of CoordinatorCounter class
            for(int i=0;i<numOfWorkers;i++){
                counter.setLastSentValues(String.valueOf(i),value/numOfWorkers);
                counter.setLastSentUpdates(String.valueOf(i),value/numOfWorkers);
            }

        }

        if( typeNode == COORDINATOR ){
            counter.setCounter(0L); // Initialize the value of counter
            counter.setCounterDoubles(0); // Initialize the value of counterDoubles
            counter.setLastSentValue(0); // Initialize the lastSentValue
            counter.setNumMessages((long) log2(value)); // Initialize the number of rounds
        }
        else{
            counter.setCounter(value/numOfWorkers); // Initialize the value of counter
            counter.setCounterDoubles(value/numOfWorkers); // Initialize the value of counterDoubles
            counter.setLastSentValue(counter.getCounterDoubles()); // Initialize the lastSentValue
            // counter.setNumMessages(counter.getLastSentValue()); // Initialize the number of messages
            counter.setNumMessages(0); // Initialize the number of messages
        }

        counter.setLastBroadcastValue(value);  // Initialize the value of lastBroadcastValue

        // Initialize probability
        double power = highestPowerOf2(counter.getEpsilon(),counter.getLastBroadcastValue(),numOfWorkers,counter.getResConstant());
        if(power <= 0) power = 1d;
        counter.setProb(1.0/ power);

        counter.setLastTs(0); // Initialize the last timestamp
        counter.setSync(true); // Initialize sync

    }

    public void initializeConditionCounter(Counter counter,TypeNode typeNode){

        // COORDINATOR
        if( typeNode == COORDINATOR ){

            counter.setLastSentValues(new HashMap<>());
            counter.setLastSentUpdates(new HashMap<>());

            // Initialize the fields of CoordinatorCounter class
            for(int i=0;i<numOfWorkers;i++){
                counter.setLastSentValues(String.valueOf(i), 0L);
                counter.setLastSentUpdates(String.valueOf(i), 0L);
            }
        }

        if( typeNode == COORDINATOR) counter.setCounter(condition); // Initialize the value of counter.Act as estimator of counter on coordinator side
        else counter.setCounter(0L); // Initialize the value of counter.Act as total count on workers for deterministic counter

        counter.setCounterDoubles(0L); // Initialize the value of counterDoubles,act as number of round for deterministic counter on coordinator side
        counter.setLastBroadcastValue(condition);  // Initialize the value of lastBroadcastValue,act as "condition" for deterministic counters

        if( typeNode == COORDINATOR ) counter.setLastSentValue(condition); // Initialize the lastSentValue,act as last broadcast tmp value on coordinator
        else counter.setLastSentValue(0L); // Initialize the lastSentValue,act as local drift on workers for deterministic counters

        counter.setNumMessages(0L); // Initialize the number of messages
        counter.setProb(1.0d); // Initialize probability
        counter.setLastTs(0); // Initialize the last timestamp
        counter.setSync(true); // Initialize sync
    }
    public void initializeHalvedDeterministicCounter(Counter counter,TypeNode typeNode){

        long valueHalved = findValueCondHalved(counter.getEpsilon(),numOfWorkers);

        // COORDINATOR
        if( typeNode == COORDINATOR ){

            counter.setLastSentValues(new HashMap<>());
            counter.setLastSentUpdates(new HashMap<>());

            // Initialize the fields of CoordinatorCounter class
            for(int i=0;i<numOfWorkers;i++){
                counter.setLastSentValues(String.valueOf(i), 0L);
                counter.setLastSentUpdates(String.valueOf(i), 0L);
            }
        }


        if( typeNode == COORDINATOR ){
            counter.setCounter(valueHalved); // Initialize the value of counter.Act as estimator of counter on coordinator side
            counter.setCounterDoubles((long) (Math.log(valueHalved)/Math.log(numOfWorkers))); // Initialize the value of counterDoubles,act as number of round for deterministic counter on coordinator side
            counter.setNumMessages(0L); // Initialize the number of messages,acts as the temporary counter of INCREMENT/DRIFT messages from Workers
        }
        else{
            counter.setCounter(valueHalved/numOfWorkers); // Initialize the value of counter.Act as total count on workers for deterministic counter
            // counter.setNumMessages(counter.getCounter()); // Initialize the number of messages
            counter.setNumMessages(0L); // Initialize the number of messages
            counter.setCounterDoubles(0L); // Initialize the value of counterDoubles

        }

        counter.setLastBroadcastValue(valueHalved);  // Initialize the value of lastBroadcastValue,act as "condition" for deterministic counters
        counter.setLastSentValue(0L); // Initialize the lastSentValue.Act as last broadcast tmp value on coordinator side and act as local drift on workers for deterministic counters

        counter.setProb(1.0d); // Initialize probability
        counter.setLastTs(0); // Initialize the last timestamp
        counter.setSync(true); // Initialize sync
    }
    public void initializeConditionedDeterministicCounter(Counter counter,TypeNode typeNode,long condition){

        long value = findValueConditioned(counter.getEpsilon(),numOfWorkers,condition);

        // COORDINATOR
        if( typeNode == COORDINATOR ){

            counter.setLastSentValues(new HashMap<>());
            counter.setLastSentUpdates(new HashMap<>());

            // Initialize the fields of CoordinatorCounter class
            for(int i=0;i<numOfWorkers;i++){
                counter.setLastSentValues(String.valueOf(i), 0L);
                counter.setLastSentUpdates(String.valueOf(i), 0L);
            }
        }


        if( typeNode == COORDINATOR ){
            counter.setCounter(value); // Initialize the value of counter.Act as estimator of counter on coordinator side
            counter.setCounterDoubles((long) (Math.log(value)/Math.log(numOfWorkers))); // Initialize the value of counterDoubles,act as number of round for deterministic counter on coordinator side
            counter.setNumMessages(0L); // Initialize the number of messages,acts as the temporary counter of INCREMENT/DRIFT messages from Workers
        }
        else{
            counter.setCounter(value/numOfWorkers); // Initialize the value of counter.Act as total count on workers for deterministic counter
            // counter.setNumMessages(counter.getCounter()); // Initialize the number of messages
            counter.setNumMessages(0L); // Initialize the number of messages
            counter.setCounterDoubles(0L); // Initialize the value of counterDoubles

        }

        counter.setLastBroadcastValue(value);  // Initialize the value of lastBroadcastValue,act as "condition" for deterministic counters
        counter.setLastSentValue(0L); // Initialize the lastSentValue.Act as last broadcast tmp value on coordinator side and act as local drift on workers for deterministic counters

        counter.setProb(1.0d); // Initialize probability
        counter.setLastTs(0); // Initialize the last timestamp
        counter.setSync(true); // Initialize sync
    }


    // Initialize the error of counters based on setup of error
    public double baseline(){ return eps/(3*numOfNodes); } // BASELINE
    public double uniform(){ return eps/(16*Math.sqrt(numOfNodes)); } // UNIFORM

    // NON_UNIFORM
    public void nonUniform(List<Counter> counters, NodeBN_schema[] nodes){

        // Calculate alpha,beta
        double alpha,beta;
        ArrayList<Double> nums;
        if(dummyFather) nums = calculateAlphaAndBeta(nodes); // Dummy Father enable
        else nums = calculateAlphaAndBetaOrphanNodes(nodes); // Dummy Father disable
        alpha = nums.get(0);
        beta = nums.get(1);

        // Initialize the epsilon for all counters
        for(Counter counter : counters){

            // Cardinalities(Ji and Ki)
            ArrayList<Integer> cardinalities;

            // NON_UNIFORM
            if( counter.isActualNode() ){ // Actual node
                cardinalities = findCardinalities(nodes,counter.getNodeParents(),counter.getNodeName()); // Find the Ji and Ki
                if(dummyFather){
                    // If node has not parents(Ki=0) then set the Ki=1 => Dummy Father
                    if( cardinalities.get(1) == 0 ) cardinalities.set(1,1);
                }
                counter.setEpsilon(nonUniform(cardinalities.get(0),cardinalities.get(1),alpha,0,false));
            }
            else{ // Parent node
                ArrayList<String> nodeNames = counter.getNodeName();
                cardinalities = findCardinalities(nodes,nodeNames,null); // Find the Ki
                counter.setEpsilon(nonUniform(cardinalities.get(0),cardinalities.get(1),0,beta,true));
            }
        }


    }
    public double nonUniform(int nodeCardinality,int parentsCardinality,double alpha,double beta,boolean parentNode){
        if(parentNode){
            // Parent node
            // Calculate the μi = (Ki)^1/3*eps/16b
            return (Math.pow(parentsCardinality,1.0/3)*eps)/(16*beta);
        }
        else{
            // Actual node
            // Calculate the vi = (Ji*Ki)^1/3*eps/16a
            int product = nodeCardinality*parentsCardinality;
            return (Math.pow(product,1.0/3)*eps)/(16*alpha);
        }
    }
    public ArrayList<Double> calculateAlphaAndBeta(NodeBN_schema[] nodes){

        // Calculate the alpha and beta without included orphan nodes - Dummy Father

        ArrayList<Double> nums = new ArrayList<>();
        double alpha=0,beta=0;
        int parents_cardinality;

        for (NodeBN_schema node : nodes) {

            // Get the parents cardinality
            parents_cardinality = node.getCardinalityParents();

            // Update alpha
            // If node has not parents(Ki=0) then set the Ki=1 => Dummy Father
            if( parents_cardinality == 0 ) { alpha += Math.pow(node.getCardinality(), 2.0 / 3); }// (Ji*Ki)^2/3
            else{ alpha += Math.pow(node.getCardinality() * parents_cardinality, 2.0 / 3); } // (Ji*Ki)^2/3
            alpha += Math.pow(node.getCardinality() * parents_cardinality, 2.0 / 3);

            // Update beta
            beta += Math.pow(parents_cardinality,2.0/3); // (Ki)^2/3
        }

        alpha = Math.sqrt(alpha);
        beta = Math.sqrt(beta);

        nums.add(alpha);
        nums.add(beta);

        return nums;

    }
    public ArrayList<Double> calculateAlphaAndBetaOrphanNodes(NodeBN_schema[] nodes){

        // Calculate the alpha and beta included orphan nodes

        ArrayList<Double> nums = new ArrayList<>();
        double alpha=0,beta=0;
        int parents_cardinality;

        for (NodeBN_schema node : nodes) {

            // Get the parents cardinality
            parents_cardinality = node.getCardinalityParents();

            // Update alpha
            alpha += Math.pow(node.getCardinality() * parents_cardinality, 2.0 / 3);

            // Update beta
            beta += Math.pow(parents_cardinality,2.0/3); // (Ki)^2/3
        }

        alpha = Math.sqrt(alpha);
        beta = Math.sqrt(beta);

        nums.add(alpha);
        nums.add(beta);

        return nums;

    }

    // Check the correctness of epsilons(error setup) for each individual randomized counter in case of NON-UNIFORM setup
    public static void checkEpsilons(List<Counter> counters,double epsilon){

        // Calculate the sum of vi's and mi's
        double sumVi = 0d;
        double sumMi = 0d;

        List<String> actualNodeNames = new ArrayList<>();
        List<String> parentNodeNames = new ArrayList<>();

        for (Counter counter : counters){
            // Vi - actual node
            if(counter.isActualNode() && !actualNodeNames.contains(convertToString(counter.getNodeName()))){
                // Update sum of vi's
                sumVi += Math.pow(counter.getEpsilon(),2);
                actualNodeNames.add(convertToString(counter.getNodeName()));
            }
            // Mi - parent node
            else if( !counter.isActualNode() && !parentNodeNames.contains(counter.concatParNames())){
                // Update sum of mi's
                sumMi += Math.pow(counter.getEpsilon(),2);
                parentNodeNames.add(counter.concatParNames());
            }
        }

        // Less messages then set for orphan nodes Ki=1
        // Better accuracy then set for orphan nodes Ki=0
        // calculateAlphaAndBeta and initializeRandomizedCounter

        System.out.println("Sum of vi's : " + sumVi + " , sum of mi's : " + sumMi + " , epsilon^2/256 : " + Math.pow(epsilon,2)/256 + " , individual counters : " + counters.size());
        System.out.println("Compare vi : " + (sumVi <= (Math.pow(epsilon,2)/256)) + " and mi : "+( sumMi <= (Math.pow(epsilon,2)/256)));

    }


    // Calculate the size of dataset in case we want to bounded the ratio ground truth joint distribution/joint distribution using MLE
    // If m >= ((1+ε)^2/2λ^2(d+1)*ε^2)*log(n*J^(d+1)/δ) where J = maximum cardinality from all nodes , d = maximum number of parents from all nodes
    // λ : P[Xi|par(Xi)] >= λ for all i,Xi,par(Xi) , n = number of nodes and m = size of dataset
    // then P[ e^-εn <= ground truth joint distribution/joint distribution using MLE <= e^εn ] > 1-δ
    // The argument prob correspond to the probability of joint distribution P[X] = product(P[Xi]) for all i = 1 ... n
    public static long calculateSizeOfDataset(NodeBN_schema[] nodes,double epsilon,double delta,double prob){

        // Find the J,d
        int J = findMaxCardinality(nodes);
        int d = findMaxParents(nodes);

        // Find λ given the prob
        double lambda = Math.pow(10,log10(prob)/nodes.length);
        // double lambda = prob;

        // Find m
        return (long) ((Math.pow(1+epsilon,2) / (2*Math.pow(lambda,2*(d+1))*Math.pow(epsilon,2)))*log10(nodes.length*Math.pow(J,d+1)/delta));
    }

    /**
     * Combines several collections of elements and create permutations of all of them, taking one element from each
     * collection, and keeping the same order in resultant lists as the one in original list of collections.
     *
     * <ul>Example
     * <li>Input  = { {a,b,c} , {1,2,3,4} }</li>
     * <li>Output = { {a,1} , {a,2} , {a,3} , {a,4} , {b,1} , {b,2} , {b,3} , {b,4} , {c,1} , {c,2} , {c,3} , {c,4} }</li>
     * </ul>
     *
     * @param collections Original list of collections which elements have to be combined.
     * @return Resultant collection of lists with all permutations of original list.
     */
    public static <T> Collection<List<T>> generatePermutations(List<Collection<T>> collections) {
        if (collections == null || collections.isEmpty()) { return Collections.emptyList(); }
        else {
            Collection<List<T>> res = new LinkedList<>();
            permutations(collections, res, 0, new LinkedList<>());
            return res;
        }
    }

    /** Recursive implementation permutations
     *
     *
     * */
    public static <T> void permutations(List<Collection<T>> ori, Collection<List<T>> res, int d, List<T> current) {
        // If depth equals number of original collections, final reached, add and return
        if (d == ori.size()) {
            res.add(current);
            return;
        }

        // Iterate from current collection and copy 'current' element N times, one for each element
        Collection<T> currentCollection = ori.get(d);
        for (T element : currentCollection) {
            List<T> copy = new LinkedList<>(current);
            copy.add(element);
            permutations(ori, res, d + 1, copy);
        }
    }


    // Hashing the key using xxh3 hash function
    // The type of key is a byte array
    public static long hashKey(byte[] key){ return LongHashFunction.xx3().hashBytes(key); }

    // Hashing the key using xxh3 hash function
    // The type of key is String
    public static long hashKey(String key){ return LongHashFunction.xx3().hashBytes(key.getBytes()); }


    // Collect statistics about a specific Bayesian Network
    public static void collectStatBN(String input,double eps,double delta,boolean dummy) throws IOException {

        ObjectMapper objectMapper = new ObjectMapper();

        // Unmarshall JSON object as array
        NodeBN_schema[] nodes = objectMapper.readValue(new File("src\\main\\java\\bayesianNetworks\\bn_JSON_"+input+".json"), NodeBN_schema[].class);

        Vector<Counter> counters_bn = new Vector<>();
        int avgCard = 0; int avgCardParents = 0; int avgParents = 0;
        int maxCardParents = 0;
        double alpha=0,beta=0;
        int numUpdatesInput = 0;
        for (NodeBN_schema node : nodes) {

            // Calculate the alpha and beta without included orphan nodes - Dummy Father
            // Get the parents cardinality
            int parents_cardinality = node.getCardinalityParents();

            // Dummy Father
            if(dummy){

                // Update alpha
                // If node has not parents(Ki=0) then set the Ki=1 => Dummy Father
                if( parents_cardinality == 0 ) { alpha += Math.pow(node.getCardinality(), 2.0 / 3); }// (Ji*Ki)^2/3
                else{ alpha += Math.pow(node.getCardinality() * parents_cardinality, 2.0 / 3); } // (Ji*Ki)^2/3
                alpha += Math.pow(node.getCardinality() * parents_cardinality, 2.0 / 3);

                // Update beta
                beta += Math.pow(parents_cardinality,2.0/3); // (Ki)^2/3
            }
            else{

                // Update alpha
                alpha += Math.pow(node.getCardinality() * parents_cardinality, 2.0 / 3);

                // Update beta
                beta += Math.pow(parents_cardinality,2.0/3); // (Ki)^2/3
            }

            // Get the cardinality from each node
            avgCard += node.getCardinality();

            // Get the cardinality of domain parents from each node
            avgCardParents += node.getCardinalityParents();

            // Get the number of parents node from each node
            avgParents += node.getParents().length;

            // Initialize the counters for each node
            counters_bn.addAll(initializeCounters(node));

            // Update the number of updates per input
            if(node.getParents().length > 0) numUpdatesInput += 2;
            else numUpdatesInput++;

            // Update the maximum cardinality of parents nodes
            if(node.getCardinalityParents() >= maxCardParents ) maxCardParents = node.getCardinalityParents();
        }
        alpha = Math.sqrt(alpha);
        beta = Math.sqrt(beta);

        AtomicInteger zeroTrueParameter = new AtomicInteger();
        AtomicInteger numParameter = new AtomicInteger();
        int counterZeroEpsilon = 0;
        for (Counter counter : counters_bn) {

            // Cardinalities(Ji and Ki)
            ArrayList<Integer> cardinalities;

            // NON_UNIFORM
            if( counter.isActualNode() ){ // Actual node
                cardinalities = findCardinalities(nodes,counter.getNodeParents(),counter.getNodeName()); // Find the Ji and Ki
                // If node has not parents(Ki=0) then set the Ki=1 => Dummy Father
                if(dummy) {
                    if( cardinalities.get(1) == 0 ) cardinalities.set(1,1);
                }
                counter.setEpsilon(nonUniformStats(cardinalities.get(0),cardinalities.get(1),alpha,0,false,eps));
            }
            else{ // Parent node
                ArrayList<String> nodeNames = counter.getNodeName();
                cardinalities = findCardinalities(nodes,nodeNames,null); // Find the Ki
                counter.setEpsilon(nonUniformStats(cardinalities.get(0),cardinalities.get(1),0,beta,true,eps));
            }

            if(counter.getTrueParameter() == 0d && counter.isActualNode()) zeroTrueParameter.getAndIncrement();
            if(counter.isActualNode()) numParameter.getAndIncrement();
            if(counter.getEpsilon() == 0d) counterZeroEpsilon++;

        }

        // Bayesian Network
        System.out.println("Bayesian Network : "+ input.toUpperCase());
        System.out.println("Number of nodes : "+nodes.length);
        System.out.println("Total number of counters : "+counters_bn.size());
        System.out.println("Maximum number of updates per input : "+2*nodes.length);
        System.out.println("Number of updates per input : "+numUpdatesInput);
        System.out.println("Number of parameters : " + numParameter.get());
        System.out.println("Zero true parameters : " + zeroTrueParameter.get());
        System.out.println("Maximum cardinality from all nodes(J) : " + findMaxCardinality(nodes));
        System.out.println("Average cardinality from all nodes(Ji's) : " + (double)avgCard/nodes.length);
        System.out.println("Maximum cardinality of parents nodes(Ki's) : " + maxCardParents);
        System.out.println("Average cardinality of parents nodes(Ki's) : " + (double)avgCardParents/nodes.length);
        System.out.println("Maximum number of parents from all nodes(d) : " + findMaxParents(nodes));
        System.out.println("Average number of parents from all nodes : " + (double)avgParents/nodes.length);
        System.out.println("Dummy Father : "+dummy);
        System.out.println("Epsilon : "+eps);
        System.out.println("Delta : "+delta);

        // Error setup
        double baseline = eps/(3*nodes.length);
        double uniform = eps/(16*Math.sqrt(nodes.length));
        System.out.println("\n\nBASELINE : "+baseline);
        System.out.println("UNIFORM : "+uniform);

        // NON_UNIFORM
        System.out.println("NON_UNIFORM ");
        System.out.println("alpha : "+alpha);
        System.out.println("beta : "+beta);
        double avgBaseline = 0d;double avgUniform = 0d;
        double avgBaselineActual = 0d;double avgBaselineParents = 0d;
        double avgUniformActual = 0d;double avgUniformParents = 0d;
        double avgErrorCounterNonUniform = 0d;
        int countActual=0;
        long equalEpsilonU=0;long lessEpsilonU=0;long biggerEpsilonU=0;
        long equalEpsilonB=0;long lessEpsilonB=0;long biggerEpsilonB=0;
        for (Counter counter : counters_bn) {

            // Find the error for actual and not actual counters
            if(counter.isActualNode()){
                avgBaselineActual += Math.abs(counter.getEpsilon()-baseline);
                avgUniformActual += Math.abs(counter.getEpsilon()-uniform);
                countActual++;
            }
            else{
                avgBaselineParents += Math.abs(counter.getEpsilon()-baseline);
                avgUniformParents += Math.abs(counter.getEpsilon()-uniform);
            }

            // Find the difference between the error setup algorithms
            avgBaseline += Math.abs(counter.getEpsilon()-baseline);
            avgUniform += Math.abs(counter.getEpsilon()-uniform);

            // Get the error of counter
            avgErrorCounterNonUniform += counter.getEpsilon();

            // BASELINE
            if(counter.getEpsilon() == baseline) equalEpsilonB++;
            else if( baseline > counter.getEpsilon()) biggerEpsilonB++;
            else if( baseline < counter.getEpsilon() ) lessEpsilonB++;

            // UNIFORM
            if(counter.getEpsilon() == uniform) equalEpsilonU++;
            else if( uniform > counter.getEpsilon()) biggerEpsilonU++;
            else if( uniform < counter.getEpsilon() ) lessEpsilonU++;
        }

        System.out.println("Average error of counters using NON_UNIFORM : "+avgErrorCounterNonUniform/counters_bn.size());
        System.out.println("Counter with zero epsilon using NON_UNIFORM : "+counterZeroEpsilon);
        System.out.println("\n\nAverage absolute error BASELINE-UNIFORM from all counters : " + (baseline-uniform));
        System.out.println("Average absolute error BASELINE-NON_UNIFORM from all counters : " + avgBaseline/counters_bn.size());
        System.out.println("Average absolute error BASELINE-NON_UNIFORM from all actual counters : " + avgBaselineActual/countActual);
        System.out.println("Average absolute error BASELINE-NON_UNIFORM from all parents counters : " + avgBaselineParents/(counters_bn.size()-countActual));
        System.out.println("Average absolute error UNIFORM-NON_UNIFORM from all counters : " + avgUniform/counters_bn.size());
        System.out.println("Average absolute error UNIFORM-NON_UNIFORM from all actual counters : " + avgUniformActual/countActual);
        System.out.println("Average absolute error UNIFORM-NON_UNIFORM from all parents counters : " + avgUniformParents/(counters_bn.size()-countActual));
        System.out.println("BASELINE-NON_UNIFORM epsilon => equalEpsilon : "+equalEpsilonB+" , lessEpsilon "+lessEpsilonB+" , biggerEpsilon : "+biggerEpsilonB);
        System.out.println("UNIFORM-NON_UNIFORM epsilon => equalEpsilon : "+equalEpsilonU+" , lessEpsilon "+lessEpsilonU+" , biggerEpsilon : "+biggerEpsilonU);

        System.out.println("\n\nNON_UNIFORM");
        checkEpsilons(counters_bn,eps);

        // BASELINE
        for (Counter counter : counters_bn) { counter.setEpsilon(baseline); }
        System.out.println("BASELINE");
        checkEpsilons(counters_bn,eps);

        // UNIFORM
        for (Counter counter : counters_bn) { counter.setEpsilon(uniform); }
        System.out.println("UNIFORM");
        checkEpsilons(counters_bn,eps);


    }
    public static double nonUniformStats(int nodeCardinality,int parentsCardinality,double alpha,double beta,boolean parentNode,double eps){
        if(parentNode){
            // Parent node
            // Calculate the μi = (Ki)^1/3*eps/16b
            return (Math.pow(parentsCardinality,1.0/3)*eps)/(16*beta);
        }
        else{
            // Actual node
            // Calculate the vi = (Ji*Ki)^1/3*eps/16a
            int product = nodeCardinality*parentsCardinality;
            return (Math.pow(product,1.0/3)*eps)/(16*alpha);
        }
    }

    // Collect statistics about parameter epsilon
    public static void collectStatsEpsilon(int numOfNodes,int numOfWorkers,double eps,double delta){

        // BASELINE
        double epsilonB;

        // UNIFORM
        double epsilonU;

        // Randomized counter
        long broadcastValueRC;

        // Deterministic counter
        long broadcastValueDC;

        for(double epsilon=0.02;epsilon<=eps;epsilon+=0.02){
            epsilonB = epsilon/(3*numOfNodes);
            epsilonU = epsilon/(16*Math.sqrt(numOfNodes));
            System.out.println("\n\nEpsilon "+epsilon
                                +" => BASELINE : "+epsilonB
                                +" , UNIFORM : "+epsilonU
                                +" , difference : "+(epsilonB-epsilonU));

            // BASELINE
            broadcastValueRC = (long) ((2*Math.sqrt(1/delta)*Math.sqrt(numOfWorkers))/epsilonB);
            broadcastValueDC = (long) ((2L *numOfWorkers)/epsilonB);
            System.out.println("Epsilon "+epsilon+" , workers "+numOfWorkers
                                +" , BASELINE => messagesSentRC(nRC) with prob=1 : "+broadcastValueRC
                                +" , messagesSentDC(nDC) with prob=1 : "+broadcastValueDC
                                +" , difference : "+(broadcastValueRC-broadcastValueDC));

            // UNIFORM
            broadcastValueRC = (long) ((2*Math.sqrt(1/delta)*Math.sqrt(numOfWorkers))/epsilonU);
            broadcastValueDC = (long) ((2L *numOfWorkers)/epsilonU);
            System.out.println("Epsilon "+epsilon+" , workers "+numOfWorkers
                                +" , UNIFORM => messagesSentRC(nRC) with prob=1 : "+broadcastValueRC
                                +" , messagesSentDC(nDC) with prob=1 : "+broadcastValueDC
                                +" , difference : "+(broadcastValueRC-broadcastValueDC));
        }
    }

    // Collect statistics about type of error schema
    public static void collectStatsSchema(int numOfNodes,double eps){

        // BASELINE
        double epsilonB;

        // UNIFORM
        double epsilonU;

        for(int nodes=2;nodes<=numOfNodes;nodes+=2){
            epsilonB = eps/(3*nodes);
            epsilonU = eps/(16*Math.sqrt(nodes));
            System.out.println("Nodes of BN "+nodes
                    +" => BASELINE : "+epsilonB
                    +" , UNIFORM : "+epsilonU
                    +" , difference : "+(epsilonB-epsilonU));
        }
    }

    // Collect statistics about type of counters
    public static void collectStatsCounters(int numOfWorkers,double eps,double delta){

        // Randomized counter
        long broadcastValueRC; // (long) ((2*Math.sqrt(1/delta)*Math.sqrt(numOfWorkers))/eps);
        ArrayList<Long> broadcastValuesRC = new ArrayList<>();

        // Deterministic counter
        long broadcastValueDC; // (long) ((2L *numOfWorkers)/eps);
        ArrayList<Long> broadcastValuesDC = new ArrayList<>();


        for(int workers=2;workers<=numOfWorkers;workers+=2){
            broadcastValueRC = (long) ((2*Math.sqrt(1/delta)*Math.sqrt(workers))/eps);
            broadcastValueDC = (long) ((2L *workers)/eps);
            System.out.println("Workers "+workers
                                +" => messagesSentRC(nRC) with prob=1 : "+broadcastValueRC
                                +" , messagesSentDC(nDC) with prob=1 : "+broadcastValueDC
                                +" , difference : "+(broadcastValueRC-broadcastValueDC));

            broadcastValuesRC.add(broadcastValueRC);
            broadcastValuesDC.add(broadcastValueDC);
        }

        System.out.println("bv_rc="+broadcastValuesRC);
        System.out.println("bv_dc="+broadcastValuesDC);
    }

    @Override
    public String toString(){

        return  " , datasetSchema : " + this.getDatasetSchema()
                + " , errorSetup : " + this.getErrorSetup()
                + " , setupCounter : " + this.getSetupCounter()
                + ( this.getSetupCounter() == SetupCounter.CONDITION ? " , condition : " + this.getCondition() : "" )
                + " , typeCounter : " + this.getTypeCounter()
                + " , typeNetwork : " + this.getTypeNetwork()
                + " , numOfWorkers : " + this.getNumOfWorkers()
                + " , eps : " + this.getEps()
                + " , delta : " + this.getDelta()
                + " , numOfNodes : " + this.getNumOfNodes();

    }

    // Type of node
    public enum TypeNode{
        WORKER,
        COORDINATOR
    }

}
