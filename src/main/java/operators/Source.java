package operators;

import com.fasterxml.jackson.databind.ObjectMapper;
import config.CBNConfig;
import config.Config;
import config.FGMConfig;
import datatypes.NodeBN_schema;
import datatypes.input.Input;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.IOException;
import java.util.*;

import static config.InternalConfig.coordinatorKey;
import static datatypes.NodeBN_schema.*;
import static state.State.generatePermutations;
import static utils.MathUtils.log;
import static utils.Util.*;

public class Source {

    // RichSourceFunction for parallelism ???
    // CheckpointedFunction is used only for variable count
    public static class QuerySource implements SourceFunction<Input>, CheckpointedFunction {

        private final Config config; // Configuration
        private final long size; // The size of testing data(queries)
        private final List<String> datasetSchema; // The schema of dataset

        private volatile boolean isRunning = true;
        private long count = 0L;

        // Checkpointed source
        private transient ListState<Long> checkpointedCount;

        // Constructor
        public QuerySource(CBNConfig config){

            // Initialization
            this.config = config;
            size = config.queriesSize();
            datasetSchema = Arrays.asList(config.datasetSchema().split(","));
        }
        public QuerySource(FGMConfig config){

            // Initialization
            this.config = config;
            size = config.queriesSize();
            datasetSchema = Arrays.asList(config.datasetSchema().split(","));
        }

        // Getters
        public Config getConfig() { return config; }
        public long getSize() { return size; }
        public List<String> getDatasetSchema() { return datasetSchema; }
        public boolean isRunning() { return isRunning; }
        public long getCount() { return count; }

        // Setters
        public void setRunning(boolean running) { isRunning = running; }
        public void setCount(long count) { this.count = count; }


        // Methods

        @Override
        public void run(SourceContext<Input> sourceContext) throws IOException {

            ObjectMapper objectMapper = new ObjectMapper();

            // Unmarshall JSON object as array of NodeBN_schema
            NodeBN_schema[] nodes = objectMapper.readValue(config.BNSchema(), NodeBN_schema[].class); // Read value from string
            // NodeBN_schema[] nodes = objectMapper.readValue(new File(config.BNSchema()), NodeBN_schema[].class); Read value from file or url

            // The logic of source
            while ( isRunning && count<size ){

                // This synchronized block ensures that state checkpointing,
                // internal state updates and emission of elements are an atomic operation
                synchronized (sourceContext.getCheckpointLock()) {

                    // Generate data based on true parameters
                    ArrayList<String> output;
                    double prob;

                    // prob = generateQuery(nodes,output); or output = generateQueryHighestProb(nodes);
                    output = generateQuery(nodes);

                    // Calculate the log of probability of output
                    prob = calculateProbability(output,datasetSchema,nodes);

                    // Check the probability(keep highly likely events)
                    // >= log(0.01)
                    if ( prob <= 0 ) {

                        // Create the output(output follow the schema of datasetSchema)
                        Input input = new Input(convertToString(output), coordinatorKey, System.currentTimeMillis());

                        // Collect the value
                        sourceContext.collect(input);

                        // Increment the count
                        count++;
                    }
                }
            }

        }

        @Override
        public void cancel() { isRunning = false; }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            this.checkpointedCount.clear();
            this.checkpointedCount.add(count);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            this.checkpointedCount = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("count", Long.class));

            if (context.isRestored()) {
                for (Long count : this.checkpointedCount.get()) { this.count = count; }
            }
        }

        // Generate query and return the probability , require topological order of Bayesian Network
        public double generateQuery(NodeBN_schema[] nodes,ArrayList<String> output){

            double prob = 0d;
            for (String node : datasetSchema) {
                int index = findIndexNode(node, nodes);
                if (index != -1) output.add(generateValue(nodes[index])); // Generate the value
                prob = prob + log(findProbability(output, datasetSchema, nodes[index])); // Update the probability-required topological order of Bayesian Network
            }

            return prob;
        }

        // Generate query(not require topological order)
        public ArrayList<String> generateQuery(NodeBN_schema[] nodes){

            ArrayList<String> output = new ArrayList<>();
            for (String node : datasetSchema) {
                int index = findIndexNode(node, nodes);
                if (index != -1) output.add(generateValue(nodes[index])); // Generate the value
            }

            return output;
        }

        // Generate query with highest probability
        public ArrayList<String> generateQueryHighestProb(NodeBN_schema[] nodes){

            ArrayList<String> output = new ArrayList<>(datasetSchema);
            output.replaceAll(str -> "");
            int index;

            // For orphan nodes
            for(int i=0;i<datasetSchema.size();i++){
                // Get the index
                index = findIndexNode(datasetSchema.get(i), nodes);
                if( index != -1 ) {
                    // Only for orphan nodes
                    if (nodes[index].getParents().length == 0) { output.set(i,generateHighestProbValue(nodes[index])); }
                }
            }

            // For the rest of the nodes
            for(int i=0;i<datasetSchema.size();i++){
                if( output.get(i).equals("") ){
                    // Get the index
                    index = findIndexNode(datasetSchema.get(i), nodes);
                    if( index != -1 ) output.set(i,generateValue(nodes[index])); // Generate the value
                }
            }

            return output;
        }

        // Generate uniformly at random a value for the node
        public String generateValue(NodeBN_schema node){

            Random rand = new Random();
            int index = rand.nextInt(node.getValues().length);
            return node.getValues()[index];
        }

        // Select the value with the highest probability
        public String generateHighestProbValue(NodeBN_schema node){

            double maxValue=0d;
            int index=0;
            String[] trueParameters = node.getTrueParameters();

            for(int i=0;i<trueParameters.length;i++){
                if( Double.parseDouble(trueParameters[i]) >= maxValue ){
                    maxValue =  Double.parseDouble(trueParameters[i]);
                    index = i;
                }
            }

            return node.getValues()[index];
        }

        // Find the probability of the last item of input given the other items of input(required topological order of Bayesian Network)
        public static double findProbability(List<String> input,List<String> datasetSchema,NodeBN_schema node){

            Collection<List<String>> permutations; // Permutations
            List<Collection<String>> values; // Values of parents nodes

            // Check if exists parent nodes
            if(node.getParents().length != 0){

                // Get values of parents nodes
                values = getValueParents(node);

                // Generate the permutations
                permutations = generatePermutations(values);

                // Find the parent values from input
                List<String> parentsNames = getNameParents(node); // Parents names
                List<String> parentsValues = new ArrayList<>(); // Parents values
                for( String parent : parentsNames ){
                    int index = findIndex(datasetSchema,parent);
                    if( index != -1 ) parentsValues.add(input.get(index));
                }

                // Find the index of permutation
                int indexPermutation = 0;
                for( List<String> permutation : permutations){
                    if(equalsList(permutation,parentsValues)) break;
                    indexPermutation++;
                }

                // Get the probability
                String value = input.get(input.size()-1);
                int indexValue = findIndex(Arrays.asList(node.getValues()),value);
                if( indexValue != -1 ){
                    String[] probValues = node.getTrueParameters()[indexValue].split(",");
                    return Double.parseDouble(probValues[indexPermutation]);
                }

            }
            else{

                // Get the probability of value
                String value = input.get(input.size()-1);
                int index = findIndex(Arrays.asList(node.getValues()),value);
                if(index != -1) return Double.parseDouble(node.getTrueParameters()[index]);
            }

            return 0;
        }

        // Calculate the probability of input
        public static double calculateProbability(List<String> input,List<String> datasetSchema,NodeBN_schema[] nodes){

            Collection<List<String>> permutations; // Permutations
            List<Collection<String>> values; // Values of parents nodes
            double prob = 0d;

            // For each node find the probability
            for(int i=0;i<datasetSchema.size();i++){

                // Find the node
                int indexNode = findIndexNode(datasetSchema.get(i), nodes);

                // Get the node
                NodeBN_schema node = nodes[indexNode];

                // Check if exists parent nodes
                if(node.getParents().length != 0){

                    // Get values of parents nodes
                    values = getValueParents(node);

                    // Generate the permutations
                    permutations = generatePermutations(values);

                    // Find the parent values from input
                    List<String> parentsNames = getNameParents(node); // Parents names
                    List<String> parentsValues = new ArrayList<>(); // Parents values
                    for( String parent : parentsNames ){
                        int index = findIndex(datasetSchema,parent);
                        if( index != -1 ) parentsValues.add(input.get(index));
                    }

                    // Find the index of permutation
                    int indexPermutation = 0;
                    for( List<String> permutation : permutations){
                        if(equalsList(permutation,parentsValues)) break;
                        indexPermutation++;
                    }

                    // Get the probability
                    String value = input.get(i);
                    int indexValue = findIndex(Arrays.asList(node.getValues()),value);
                    if( indexValue != -1 ){
                        String[] probValues = node.getTrueParameters()[indexValue].split(",");
                        prob = prob + log(Double.parseDouble(probValues[indexPermutation]));
                    }
                }
                else{

                    // Get the probability of value
                    String value = input.get(i);
                    int index = findIndex(Arrays.asList(node.getValues()),value);
                    if(index != -1) prob = prob + log(Double.parseDouble(node.getTrueParameters()[index]));
                }
            }

            return prob;
        }

    }


    // Classification Source
    // Generate tuples for classification.Used for Naive Bayes classifier
    public static class ClassificationSource implements SourceFunction<Input>, CheckpointedFunction{

        private final Config config; // Configuration
        private final long size; // The size of testing data
        private final List<String> datasetSchema; // The schema of dataset

        private volatile boolean isRunning = true;
        private long count = 0L;

        // Checkpointed source
        private transient ListState<Long> checkpointedCount;

        // Constructor
        public ClassificationSource(CBNConfig config){

            // Initialization
            this.config = config;
            size = config.queriesSize();
            datasetSchema = Arrays.asList(config.datasetSchema().split(","));
        }
        public ClassificationSource(FGMConfig config){

            // Initialization
            this.config = config;
            size = config.queriesSize();
            datasetSchema = Arrays.asList(config.datasetSchema().split(","));
        }

        // Getters
        public Config getConfig() { return config; }
        public long getSize() { return size; }
        public List<String> getDatasetSchema() { return datasetSchema; }
        public boolean isRunning() { return isRunning; }
        public long getCount() { return count; }

        // Setters
        public void setRunning(boolean running) { isRunning = running; }
        public void setCount(long count) { this.count = count; }

        @Override
        public void run(SourceContext<Input> sourceContext) throws Exception {

            ObjectMapper objectMapper = new ObjectMapper();

            // Unmarshall JSON object as array of NodeBN_schema
            NodeBN_schema[] nodes = objectMapper.readValue(config.BNSchema(), NodeBN_schema[].class); // Read value string
            // NodeBN_schema[] nodes = objectMapper.readValue(new File(config.BNSchema()), NodeBN_schema[].class); // Read value from file or url

            // The logic of source
            while ( isRunning && count<size ){

                // This synchronized block ensures that state checkpointing,
                // internal state updates and emission of elements are an atomic operation
                synchronized (sourceContext.getCheckpointLock()) {

                    // Generate values of attributes at random(except class node)
                    ArrayList<String> output = new ArrayList<>();
                    for(int i=0;i<nodes.length-1;i++){
                        // Get the value
                        output.add(generateAttrValue(nodes[i]));
                    }

                    // Create the output(output follow the schema of datasetSchema except from class value)
                    Input input = new Input(convertToString(output), coordinatorKey, System.currentTimeMillis());

                    // Collect the value
                    sourceContext.collect(input);

                    // Increment the count
                    count++;
                }

            }
        }

        // Generate uniformly at random an attribute value for the node
        public String generateAttrValue(NodeBN_schema node){

            Random rand = new Random();
            int index = rand.nextInt(node.getValues().length);
            return node.getValues()[index];
        }

        @Override
        public void cancel() { isRunning = false; }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            this.checkpointedCount.clear();
            this.checkpointedCount.add(count);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            this.checkpointedCount = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("count", Long.class));

            if (context.isRestored()) {
                for (Long count : this.checkpointedCount.get()) { this.count = count; }
            }
        }


    }

}
