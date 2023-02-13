package operators;

import com.fasterxml.jackson.databind.ObjectMapper;
import config.InternalConfig;
import datatypes.NodeBN_schema;
import datatypes.counter.Counter;
import datatypes.input.Input;
import fgm.sketch.Murmur3;

import java.io.*;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import java.util.Vector;


import static config.InternalConfig.TypeCounter.*;
import static config.InternalConfig.coordinatorKey;
import static coordinator.CoordinatorFunction.calculateEstimator;
import static fgm.sketch.TestCountMin.initializeExactCounter;
import static operators.ParsingLogs.getError;
import static operators.ParsingLogs.getErrorEXACTFile;
import static state.State.TypeNode.WORKER;
import static state.State.hashKey;
import static state.State.initializeCounters;
import static utils.MathUtils.laplaceSmoothing;
import static utils.MathUtils.log;
import static worker.WorkerFunction.getKey;

public class StrToCounter {

    public static String statsPath = "C:\\Users\\Nikos\\Desktop\\test\\link_results\\link\\datasets\\EXACT\\50M\\queriesGT";
    public static String queriesPath = "C:\\Users\\Nikos\\Desktop\\test\\link_results\\link\\datasets\\EXACT\\50M\\querySource_link";
    // public static String queriesPath = "C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\100M\\RC\\BASELINE\\queries1";

    public static void main(String[] args) throws Exception {

        // estimateGT_FGM("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\alarm\\Error_to_MLE-FGM\\5K\\Experiment_1\\BASELINE\\enableRebalancing=false\\coordinator_stats.txt","alarm",0.1,0.25);
        // estimateGT("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Error_to_MLE-DC\\50K\\Experiment_1\\EXACT\\coordinator_stats.txt","hepar2",EXACT,0.1,0.25);
        estimateGT("C:\\Users\\Nikos\\Desktop\\test\\link_results\\link\\datasets\\EXACT\\50M\\coordinator_stats","link",DETERMINISTIC,0.1,0.25);

        // String file1 = "C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Error_to_MLE-DC\\50K\\Experiment_1\\BASELINE";
        // String file2 = "C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Error_to_MLE-DC\\50K\\Experiment_1\\EXACT";
        // getErrorEXACTFile(file1,file2,file1,0.1d);
        // getError(new BufferedReader(new FileReader("queriesGT")));
    }


    // Convert string to counter object
    public static Counter strToCounter(String strCounterValue){

        /*String strCounter = "Counter => name : [ChHepatitis] , value : [active] , actualNode : true , trueParameter : 0.2094241 , nodeParents : [transfusion, vh_amn, injections] , " +
                "nodeParentValues : [present, present, present] , counter_value : [active][present, present, present] ," +
                " counter : 0 , counterDoubles : 0 , lastBroadcastValue : 10 , lastSentValue : 0 , lastTs : 1970-01-01 02:00:00.0 , " +
                "numMessages : 2 , prob : 1.0 , epsilon : 4.761904761904762E-4 , delta : 0.25 , resConstant : 2.0 , sync : true , " +
                "last sent values : {0=2, 1=2, 2=2, 3=4, 4=1, 5=3, 6=2, 7=4} , last sent updates : {0=2, 1=2, 2=2, 3=4, 4=0, 5=2, 6=2, 7=4}";*/

        // Counter rc = objectMapper.readValue(StrCounter.getBytes(),Counter.class);
        // Class c = Class.forName("datatypes.counter.Counter");

        //2. Then, you can create a new instance of the bean.
        //Assuming your Bean1 class has an empty public constructor:
        // Object o = c.newInstance();

        //3. To access the object properties, you need to cast your object to a variable
        // of the type you need to access
        // Counter rc = (Counter) o;

        // Split the string
        String values = strCounterValue.split("=>")[1].trim();

        // Get the counter values
        String[] counterValues = values.split(" , ");
        Counter counter = new Counter();

        // Get the name
        String[] name = counterValues[0].split(":")[1].trim().replace("[","").replace("]","").replace(" ","").split(",");
        counter.setNodeName(new ArrayList<>(Arrays.asList(name)));

        // Get the values
        String[] cValues = counterValues[1].split(":")[1].trim().replace("[","").replace("]","").replace(" ","").split(",");
        counter.setNodeValue(new ArrayList<>(Arrays.asList(cValues)));

        // Actual node
        counter.setActualNode(Boolean.parseBoolean(counterValues[2].split(":")[1].trim()));

        // True parameter
        counter.setTrueParameter(Double.parseDouble(counterValues[3].split(":")[1].trim()));

        // Node parents
        String[] nodeParents = counterValues[4].split(":")[1].trim().replace("[","").replace("]","").replace(" ","").split(",");
        if(nodeParents.length == 1 && nodeParents[0].equals("null")) counter.setNodeParents(null);
        else counter.setNodeParents(new ArrayList<>(Arrays.asList(nodeParents)));

        // Node parents values
        String[] nodeParentsValues = counterValues[5].split(":")[1].trim().replace("[","").replace("]","").replace(" ","").split(",");
        if(nodeParentsValues.length == 1 && nodeParentsValues[0].equals("null")) counter.setNodeParentValues(null);
        else counter.setNodeParentValues(new ArrayList<>(Arrays.asList(nodeParentsValues)));

        // Counter
        counter.setCounter(Long.parseLong(counterValues[7].split(":")[1].trim()));

        // CounterDoubles
        counter.setCounterDoubles(Long.parseLong(counterValues[8].split(":")[1].trim()));

        // LastBroadcastValue
        counter.setLastBroadcastValue(Long.parseLong(counterValues[9].split(":")[1].trim()));

        // LastSentValue
        counter.setLastSentValue(Long.parseLong(counterValues[10].split(":")[1].trim()));

        // LastTs
        counter.setLastTs(Timestamp.valueOf(counterValues[11].split(" : ")[1].trim()).getTime());

        // NumMessages
        counter.setNumMessages(Long.parseLong(counterValues[12].split(":")[1].trim()));

        // Prob
        counter.setProb(Double.parseDouble(counterValues[13].split(":")[1].trim()));

        // Eps
        counter.setEpsilon(Double.parseDouble(counterValues[14].split(":")[1].trim()));

        // Delta
        counter.setDelta(Double.parseDouble(counterValues[15].split(":")[1].trim()));

        // ResConstant
        counter.setResConstant(Double.parseDouble(counterValues[16].split(":")[1].trim()));

        // Sync
        counter.setSync(Boolean.parseBoolean(counterValues[17].split(":")[1].trim()));

        // lastSentValues
        if( counterValues[18].split(":")[1].trim().contains("null")) counter.setLastSentValues(null);
        else {
            String lastSentValues = counterValues[18].split(":")[1].trim().replace("{", "").replace("}", "").replace(" ", "");
            HashMap<String, Long> map = (HashMap<String, Long>) Arrays.stream(lastSentValues.split(",")).map(s -> s.split("=")).collect(Collectors.toMap(e -> e[0], e -> Long.parseLong(e[1])));
            counter.setLastSentValues(map);
        }

        // LastSentUpdates
        if( counterValues[19].split(":")[1].trim().contains("null")) counter.setLastSentUpdates(null);
        else {
            String lastSentUpdates = counterValues[19].split(":")[1].trim().replace("{", "").replace("}", "").replace(" ", "");
            HashMap<String, Long> mapU = (HashMap<String, Long>) Arrays.stream(lastSentUpdates.split(",")).map(s -> s.split("=")).collect(Collectors.toMap(e -> e[0], e -> Long.parseLong(e[1])));
            counter.setLastSentUpdates(mapU);
        }

        // Get the key
        String key = getKey(counter);

        // Hash the key
        long hashedKey = hashKey(key.getBytes());

        // Update the counter
        counter.setCounterKey(hashedKey);

        return counter;

    }

    // Estimate GT for INDIVIDUAL COUNTERS
    public static void estimateGT(String file, String bn, InternalConfig.TypeCounter typeCounter, double eps, double delta) throws Exception {


        Vector<Counter> counters = new Vector<>();
        List<Input> queries = new ArrayList<>();

        // Read from file
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = br.readLine();
        boolean find = false;
        boolean findQueries = false;

        // Skip the lines until reach end of processing
        while ( line != null ) {

            line = line.replaceAll("\0", "");

            // Skip new and empty lines
            if (line.equals("\n") || line.equals("")) {
                line = br.readLine();
                continue;
            }

            if(line.contains("End of processing")) find = true;

            if(find) {
                if (line.contains("Counter =>")) {

                    // Get the counter
                    counters.add(strToCounter(line));

                }
            }

            // Processing BN queries :
            if(line.contains("Processing BN queries :")) findQueries = true;

            if(findQueries){
                if(line.contains("Input :")){

                    // Get the value
                    String value = line.split("=>")[1].split(" , ")[0].split(":")[1].trim().replace(" ","");
                    value = value.replace(",",", ");
                    Input query = new Input(value,coordinatorKey,System.currentTimeMillis());
                    queries.add(query);
                }
            }

            // Read the next line
            line = br.readLine();

        }

        System.out.println("Total counters : "+counters.size());
        System.out.println("Queries size : "+queries.size());

        // Bayesian Network
        ObjectMapper objectMapper = new ObjectMapper();

        // Unmarshall as array
        NodeBN_schema[] nodes = objectMapper.readValue(new File("src\\main\\java\\bayesianNetworks\\bn_JSON_"+bn+".json"), NodeBN_schema[].class);

        // The schema of dataset
        String datasetSchema = new BufferedReader(new FileReader("src\\main\\java\\bayesianNetworks\\dataset_schema_"+bn)).readLine();
        LinkedHashMap<String,Integer> schema = new LinkedHashMap<>();
        int pos = 0;
        for(String node : datasetSchema.split(",")){

            // Update the schema
            schema.put(node,pos);

            // Update the position
            pos++;
        }

        // Initialize all the counters of BN
        Vector<Counter> counters_bn = new Vector<>();
        for (NodeBN_schema node : nodes) { counters_bn.addAll(initializeCounters(node)); }
        initializeExactCounter(counters_bn,WORKER);

        // Alternative => HashMap
        LinkedHashMap<Long,Counter> newCounters = new LinkedHashMap<>();
        for (Counter counter: counters_bn){

            // Get the key
            String key = getKey(counter);

            // Hash the key
            long hashedKey = hashKey(key.getBytes());

            // Update the worker state
            newCounters.put(hashedKey,counter);

            // Update the counter
            counter.setCounterKey(hashedKey);
        }

        // Update the counters
        LinkedHashMap<Long,Counter> modCounters = new LinkedHashMap<>();
        for(Counter counter : counters){

            // Update the true parameters of counters
            Counter tmp = newCounters.get(counter.getCounterKey());
            counter.setTrueParameter(tmp.getTrueParameter());

            // Get the key
            String key = getKey(counter);

            // Hash the key
            long hashedKey = hashKey(key.getBytes());

            // Update the worker state
            modCounters.put(hashedKey,counter);

            // Update the counter
            counter.setCounterKey(hashedKey);

        }

        // Get queries from file
        List<Input> new_queries = getQueries(queriesPath);
        System.out.println("Queries size from file : "+queries.size());

        // Estimate the queries using the counters
        processingQueriesBNGT(new_queries,modCounters,nodes,schema,typeCounter,eps,delta);


    }

    // Bayesian Network(BN)
    public static void processingQueriesBNGT(List<Input> queries,
                                             LinkedHashMap<Long,Counter> counters,
                                             NodeBN_schema[] nodes,
                                             LinkedHashMap<String,Integer> schema,
                                             InternalConfig.TypeCounter typeCounter,double eps,double delta) throws Exception {


        // Lower and upper bound violations
        int lbv = 0,ubv = 0;
        double avgError = 0; double avgAbsError = 0;
        double avgGT = 0d;
        BufferedWriter writer = new BufferedWriter(new FileWriter(statsPath));
        writer.write("\nProcessing BN queries : "+queries.size()+"\n");
        System.out.println("\nProcessing BN queries : "+queries.size()+"\n");

        // Processing the queries
        for(Input query : queries){

            double truthProb = estimateJointProbTruthGT(counters,nodes,schema,query);
            double estProb = estimateJointProbEstGT(counters,nodes,schema,query,typeCounter);
            double error = estProb-truthProb;

            // query.setValue(query.getValue().replace(",",", "));

            // Collect some statistics
            writer.write("\nInput : " + query
                                          + " , estimated probability : " + estProb
                                          + " , truth probability : " + truthProb
                                          + " , error : " + error
                                          + " , absolute error : " + Math.abs(error)
                                          // + "," + Math.exp(-coordinatorState.getEps()*coordinatorState.getDatasetSchema().size()) + "  <= estimated / truth = " + estProb/truthProb + " <= " + Math.exp(coordinatorState.getEps()*coordinatorState.getDatasetSchema().size())
                                          + "," + -eps*schema.size() + "  <= ln(estimated) - ln(truth) = " + (estProb-truthProb) + " <= " + eps*schema.size()
                                          + " with probability " + (1-delta));

            if( (estProb-truthProb) >= (eps*schema.size()) ){ ubv++; }
            else if( (estProb-truthProb) <= (-eps*schema.size()) ){ lbv++; }

            // Update the average error
            avgError += error;
            avgAbsError += Math.abs(error);

            // Update the average of GT log probability
            avgGT += truthProb;
        }

        System.out.println("\nProcessed queries : "+queries.size()+"\n");
        writer.write("\n\nProcessed queries : "+queries.size()+"\n");
        writer.write("\nAverage Ground Truth Log probability : "+ avgGT/queries.size());
        writer.write("\nAverage absolute error : " + avgAbsError/queries.size());
        writer.write("\nAverage error : " + avgError/queries.size()
                                              + " , lower violations : " + lbv
                                              + " , upper violations : " + ubv);

        // Close the buffer
        writer.close();

    }

    public static double estimateJointProbTruthGT(LinkedHashMap<Long,
                                                  Counter> counters,
                                                  NodeBN_schema[] nodes,
                                                  LinkedHashMap<String,Integer> schema,
                                                  Input input) {

        // Get the values of input
        List<String> values = Arrays.asList(input.getValue().replaceAll(" ","").split(","));

        // The log of joint probability of input
        // Log to avoid underflow problems
        // double prob = 1.0d;
        double logProb = 0d;
        double trueParameter;

        // Parsing all nodes
        for( NodeBN_schema node : nodes ) {

            // String buffer
            StringBuffer sb = new StringBuffer();

            // Get the name of node and appended
            String name = node.getName();
            sb.append(name);

            // Check if exists parents
            if (node.getParents().length != 0) {

                // Find and append the value from input using the schema of dataset
                sb.append(",").append(values.get(schema.get(name)));

                // Get the name and values of parents node
                StringBuilder nameParents = new StringBuilder();
                StringBuilder valuesParents = new StringBuilder();
                for (NodeBN_schema nodeParents : node.getParents()) {
                    nameParents.append(nodeParents.getName()).append(",");
                    valuesParents.append(values.get(schema.get(nodeParents.getName()))).append(",");
                }

                // Delete the last comma
                valuesParents.deleteCharAt(valuesParents.length() - 1);

                // Get the key
                String key = sb.append(",").append(nameParents).append(valuesParents).toString().replace(" ", "");

                // Get the true parameter(hashing the key and get the counter)
                trueParameter = counters.get(hashKey(key.getBytes())).getTrueParameter();

            }
            else {

                // Find and append the value from input using the schema of dataset
                sb.append(",").append(values.get(schema.get(name)));

                // Get the key
                String key = sb.toString().replace(" ", "");

                // Get the true parameter(hashing the key and get the counter)
                trueParameter = counters.get(hashKey(key.getBytes())).getTrueParameter();
            }

            // Update the probability
            // prob = prob * counter.getTrueParameter();
            logProb += log(trueParameter);

        }

        return logProb;
    }

    public static double estimateJointProbEstGT(LinkedHashMap<Long,
                                                Counter> counters,
                                                NodeBN_schema[] nodes,
                                                LinkedHashMap<String,Integer> schema,
                                                Input input,
                                                InternalConfig.TypeCounter typeCounter){

        // Get the values of input
        List<String> values = Arrays.asList(input.getValue().replaceAll(" ","").split(","));

        // The joint probability of input
        // Log to avoid underflow problems
        // double prob = 1.0d;
        double logProb = 0d;
        double estProb;
        long estimator_node;
        long estimator_parent = 0L;

        // Parsing all nodes
        for(NodeBN_schema node : nodes){

            // String buffer
            StringBuffer sb = new StringBuffer();

            // Get the name of node and appended
            String name = node.getName();
            sb.append(name);

            // Check if exists parents
            if(node.getParents().length != 0){

                // Parameter = estimator_node / estimator_parent

                // Estimator node
                // Find and append the value from input using the schema of dataset
                sb.append(",").append(values.get(schema.get(name)));

                // Get and append the name and values of parents nodes
                StringBuilder nameParents = new StringBuilder();
                StringBuilder valuesParents = new StringBuilder();
                for(NodeBN_schema nodeParents : node.getParents()){
                    nameParents.append(nodeParents.getName()).append(",");
                    valuesParents.append(values.get(schema.get(nodeParents.getName()))).append(",");
                }

                // Delete the last comma
                valuesParents.deleteCharAt(valuesParents.length()-1);

                // Get the key
                String key = sb.append(",").append(nameParents).append(valuesParents).toString().replace(" ","");

                // Hashing the key and get the counter
                Counter counter = counters.get(hashKey(key.getBytes()));

                // Calculate the estimation of counter based on type of counter
                if( typeCounter == DETERMINISTIC ) estimator_node = counter.getCounter();
                else estimator_node = calculateEstimator(counter);


                // Estimator parent
                // Get the key
                String parentKey = (nameParents.toString()+valuesParents+","+name).replace(" ","");

                // Hashing the key and get the parent-counter
                Counter parentCounter = counters.get(hashKey(parentKey.getBytes()));

                // Calculate the estimation of parent-counter based on type of counter
                if( typeCounter == DETERMINISTIC ) estimator_parent = parentCounter.getCounter();
                else estimator_parent = calculateEstimator(parentCounter);


            }
            else{

                // Parameter = estimator_node / estimator_parent

                // Estimator node
                // Find and append the value from input using the schema of dataset
                sb.append(",").append(values.get(schema.get(name)));

                // Get the key
                String key = sb.toString().replace(" ","");

                // Hashing the key and get the counter
                Counter counter = counters.get(hashKey(key.getBytes()));

                // Calculate the estimation of counter based on type of counter
                if( typeCounter == DETERMINISTIC ) estimator_node = counter.getCounter();
                else estimator_node = calculateEstimator(counter);


                // Estimator parent
                // For each value of node
                String parent_key;
                for(String parent_value : node.getValues()){

                    // Get the key
                    parent_key = (name+","+parent_value).replace(" ","");

                    // Hashing the key and get the counter
                    Counter parentCounter = counters.get(hashKey(parent_key.getBytes()));

                    // Calculate the estimation of parent-counter based on type of counter
                    if( typeCounter == DETERMINISTIC ) estimator_parent += parentCounter.getCounter();
                    else estimator_parent += calculateEstimator(parentCounter);

                }
            }

            // Update the joint probability
            // prob = prob * estimateParameter(coordinatorState,counter);

            // Laplace smoothing for probability of parameter(only for zero parameters)
            // laplaceSmoothing(estimator_node,estimator_parent,findCardinality(new ArrayList<>(coordinatorState.getCoordinatorState().getCounters().values()),counter),1.0d);
            estProb = laplaceSmoothing(estimator_node,estimator_parent,node.getCardinality(),1.0d);

            logProb += log(estProb);
            estimator_parent = 0;
        }

        return logProb;
    }



    // Estimate GT for FGM
    public static void estimateGT_FGM(String file, String bn, double eps, double delta) throws Exception {


        fgm.datatypes.Vector estVector = new fgm.datatypes.Vector();
        List<Input> queries = new ArrayList<>();

        // Read from file
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = br.readLine();
        boolean find = false;
        boolean findQueries = false;

        // Skip the lines until reach end of processing
        while ( line != null ) {

            line = line.replaceAll("\0", "");

            // Skip new and empty lines
            if (line.equals("\n") || line.equals("")) {
                line = br.readLine();
                continue;
            }

            if(line.contains("End of processing")) find = true;

            if(find) {
                if (line.contains("Coordinator state :  estimated vector :")) {

                    // Split the line
                    String estimatedVector = line.split(":")[2].split(" , ")[0].replace("{","").replace("}","");
                    estimatedVector = estimatedVector.trim().replace(" ","");

                    // Update the Vector
                    HashMap<Long, Double> map = (HashMap<Long, Double>) Arrays.stream(estimatedVector.split(",")).map(s -> s.split("=")).collect(Collectors.toMap(e -> Long.parseLong(e[0]), e -> Double.parseDouble(e[1])));
                    LinkedHashMap<Long,Double> estimation = new LinkedHashMap<>();
                    for(Map.Entry<Long,Double> entry : map.entrySet()){ estimation.put(entry.getKey(),entry.getValue()); }
                    estVector.setVector(estimation);

                    find = false;
                }
            }

            // Processing BN queries :
            if(line.contains("Processing BN queries :")) findQueries = true;

            if(findQueries){
                if(line.contains("Input :")){

                    // Get the value
                    String value = line.split("=>")[1].split(" , ")[0].split(":")[1].trim().replace(" ","");
                    value = value.replace(",",", ");
                    Input query = new Input(value,coordinatorKey,System.currentTimeMillis());
                    queries.add(query);
                }
            }

            // Read the next line
            line = br.readLine();

        }

        System.out.println("Total counters : "+estVector.size());
        System.out.println("Queries size : "+queries.size());

        // Bayesian Network
        ObjectMapper objectMapper = new ObjectMapper();

        // Unmarshall as array
        NodeBN_schema[] nodes = objectMapper.readValue(new File("src\\main\\java\\bayesianNetworks\\bn_JSON_"+bn+".json"), NodeBN_schema[].class);

        // The schema of dataset
        String datasetSchema = new BufferedReader(new FileReader("src\\main\\java\\bayesianNetworks\\dataset_schema_"+bn)).readLine();
        LinkedHashMap<String,Integer> schema = new LinkedHashMap<>();
        int pos = 0;
        for(String node : datasetSchema.split(",")){

            // Update the schema
            schema.put(node,pos);

            // Update the position
            pos++;
        }

        // Initialize all the counters of BN
        Vector<Counter> counters_bn = new Vector<>();
        for (NodeBN_schema node : nodes) { counters_bn.addAll(initializeCounters(node)); }
        initializeExactCounter(counters_bn,WORKER);

        // Alternative => HashMap
        LinkedHashMap<Long,Counter> newCounters = new LinkedHashMap<>();
        for (Counter counter: counters_bn){

            // Get the key
            String key = fgm.worker.WorkerFunction.getKey(counter);

            // Hash the key
            long hashedKey = Murmur3.hash64(key.getBytes());

            // Update the worker state
            newCounters.put(hashedKey,counter);

            // Update the counter
            counter.setCounterKey(hashedKey);
        }

        // Get queries from file
        List<Input> new_queries = getQueries(queriesPath);
        System.out.println("Queries size from file : "+queries.size());

        // Estimate the queries using the counters
        processingQueriesBNGT_FGM(new_queries,newCounters,nodes,schema,estVector,eps,delta);


    }

    public static void processingQueriesBNGT_FGM(List<Input> queries,
                                                LinkedHashMap<Long,Counter> counters,
                                                NodeBN_schema[] nodes,
                                                LinkedHashMap<String,Integer> schema,
                                                fgm.datatypes.Vector estVector,
                                                double eps,double delta) throws Exception {


        // Lower and upper bound violations
        int lbv = 0,ubv = 0;
        double avgError = 0; double avgAbsError = 0;
        double avgGT = 0d;
        BufferedWriter writer = new BufferedWriter(new FileWriter(statsPath));
        writer.write("\nProcessing BN queries : "+queries.size()+"\n");
        System.out.println("\nProcessing BN queries : "+queries.size()+"\n");

        // Processing the queries
        for(Input query : queries){

            double truthProb = estimateJointProbTruthGT_FGM(counters,nodes,schema,query);
            double estProb = estimateJointProbEst_FGM(counters,estVector,nodes,schema,query);
            double error = estProb-truthProb;

            // Collect some statistics
            writer.write("\nInput : " + query
                                        + " , estimated probability : " + estProb
                                        + " , truth probability : " + truthProb
                                        + " , error : " + error
                                        + " , absolute error : " + Math.abs(error)
                                        // + "," + Math.exp(-coordinatorState.getEps()*coordinatorState.getDatasetSchema().size()) + "  <= estimated / truth = " + estProb/truthProb + " <= " + Math.exp(coordinatorState.getEps()*coordinatorState.getDatasetSchema().size())
                                        + "," + -eps*schema.size() + "  <= ln(estimated) - ln(truth) = " + (estProb-truthProb) + " <= " + eps*schema.size()
                                        + " with probability " + (1-delta));

            if( (estProb-truthProb) >= (eps*schema.size()) ){ ubv++; }
            else if( (estProb-truthProb) <= (-eps*schema.size()) ){ lbv++; }

            // Update the average error
            avgError += error;
            avgAbsError += Math.abs(error);

            // Update the average of GT log probability
            avgGT += truthProb;
        }

        System.out.println("\nProcessed queries : "+queries.size()+"\n");
        writer.write("\n\nProcessed queries : "+queries.size()+"\n");
        writer.write("\nAverage Ground Truth Log probability : "+ avgGT/queries.size());
        writer.write("\nAverage absolute error : " + avgAbsError/queries.size());
        writer.write("\nAverage error : " + avgError/queries.size()
                                            + " , lower violations : " + lbv
                                            + " , upper violations : " + ubv);

        // Close the buffer
        writer.close();

    }


    // Estimate the joint probability based on estimated parameters of Bayesian Network(BN)
    public static double estimateJointProbEst_FGM(LinkedHashMap<Long,Counter> counters,
                                                  fgm.datatypes.Vector estVector,
                                                  NodeBN_schema[] nodes,
                                                  LinkedHashMap<String,Integer> schema,
                                                  Input input) {

        // Get the values of input
        List<String> values = Arrays.asList(input.getValue().replaceAll(" ","").split(","));

        // The joint probability of input
        // Log to avoid underflow problems
        // double prob = 1.0d;
        double logProb = 0d;
        double estProb;
        double estimator_node;
        double estimator_parent = 0L;

        // Parsing all nodes
        for(NodeBN_schema node : nodes){

            // String buffer
            StringBuffer sb = new StringBuffer();

            // Get the name of node and appended
            String name = node.getName();
            sb.append(name);

            // Check if exists parents
            if(node.getParents().length != 0){

                // Parameter = estimator_node / estimator_parent

                // Estimator node
                // Find and append the value from input using the schema of dataset
                sb.append(",").append(values.get(schema.get(name)));

                // Get and append the name and values of parents nodes
                StringBuilder nameParents = new StringBuilder();
                StringBuilder valuesParents = new StringBuilder();
                for(NodeBN_schema nodeParents : node.getParents()){
                    nameParents.append(nodeParents.getName()).append(",");
                    valuesParents.append(values.get(schema.get(nodeParents.getName()))).append(",");
                }

                // Delete the last comma
                valuesParents.deleteCharAt(valuesParents.length()-1);

                // Get the key
                String key = sb.append(",").append(nameParents).append(valuesParents).toString().replace(" ","");

                // Hashing the key and get the counter
                Counter counter = counters.get(Murmur3.hash64(key.getBytes()));

                // Get the estimation
                estimator_node = estVector.estimate(counter.getCounterKey());


                // Estimator parent
                // Get the key
                String parentKey = (nameParents.toString()+valuesParents+","+name).replace(" ","");

                // Hashing the key and get the parent-counter
                Counter parentCounter = counters.get(Murmur3.hash64(parentKey.getBytes()));

                // Get the estimation
                estimator_parent = estVector.estimate(parentCounter.getCounterKey());
            }
            else{

                // Parameter = estimator_node / estimator_parent

                // Estimator node
                // Find and append the value from input using the schema of dataset
                sb.append(",").append(values.get(schema.get(name)));

                // Get the key
                String key = sb.toString().replace(" ","");

                // Hashing the key and get the counter
                Counter counter = counters.get(Murmur3.hash64(key.getBytes()));

                // Get the estimation
                estimator_node = estVector.estimate(counter.getCounterKey());


                // Estimator parent
                // For each value of node
                String parent_key;
                for(String parent_value : node.getValues()){

                    // Get the key
                    parent_key = (name+","+parent_value).replace(" ","");

                    // Hashing the key and get the counter
                    Counter parentCounter = counters.get(Murmur3.hash64(parent_key.getBytes()));

                    // Get the estimation
                    estimator_parent += estVector.estimate(parentCounter.getCounterKey());
                }
            }

            // Laplace smoothing for probability of parameter(only for zero parameters)
            estProb = laplaceSmoothing(estimator_node,estimator_parent,node.getCardinality(),1.0d);

            // Update the joint probability
            // prob = prob * estimateParameter(coordinatorState,counter);
            logProb += log(estProb);
            estimator_parent = 0;
        }

        return logProb;
    }

    public static double estimateJointProbTruthGT_FGM(LinkedHashMap<Long,
                                                      Counter> counters,
                                                      NodeBN_schema[] nodes,
                                                      LinkedHashMap<String,Integer> schema,
                                                      Input input) {

        // Get the values of input
        List<String> values = Arrays.asList(input.getValue().replaceAll(" ","").split(","));

        // The log of joint probability of input
        // Log to avoid underflow problems
        // double prob = 1.0d;
        double logProb = 0d;
        double trueParameter;

        // Parsing all nodes
        for( NodeBN_schema node : nodes ) {

            // String buffer
            StringBuffer sb = new StringBuffer();

            // Get the name of node and appended
            String name = node.getName();
            sb.append(name);

            // Check if exists parents
            if (node.getParents().length != 0) {

                // Find and append the value from input using the schema of dataset
                sb.append(",").append(values.get(schema.get(name)));

                // Get the name and values of parents node
                StringBuilder nameParents = new StringBuilder();
                StringBuilder valuesParents = new StringBuilder();
                for (NodeBN_schema nodeParents : node.getParents()) {
                    nameParents.append(nodeParents.getName()).append(",");
                    valuesParents.append(values.get(schema.get(nodeParents.getName()))).append(",");
                }

                // Delete the last comma
                valuesParents.deleteCharAt(valuesParents.length() - 1);

                // Get the key
                String key = sb.append(",").append(nameParents).append(valuesParents).toString().replace(" ", "");

                // Get the true parameter(hashing the key and get the counter)
                trueParameter = counters.get(Murmur3.hash64(key.getBytes())).getTrueParameter();

            }
            else {

                // Find and append the value from input using the schema of dataset
                sb.append(",").append(values.get(schema.get(name)));

                // Get the key
                String key = sb.toString().replace(" ", "");

                // Get the true parameter(hashing the key and get the counter)
                trueParameter = counters.get(Murmur3.hash64(key.getBytes())).getTrueParameter();
            }

            // Update the probability
            // prob = prob * counter.getTrueParameter();
            logProb += log(trueParameter);

        }

        return logProb;
    }



    // Get queries from file
    public static List<Input> getQueries(String file) throws IOException {

        // Read the queries line by line
        BufferedReader br = new BufferedReader(new FileReader(file));

        List<Input> queries = new ArrayList<>();

        String line = br.readLine();

        while( line != null ){

            // Strips off all non-ASCII characters
            line = line.replaceAll("[^\\x00-\\x7F]", "");
            // Erases all the ASCII control characters
            line = line.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "");
            // Skip empty lines
            if(line.equals("")) {
                line = br.readLine();
                continue;
            }

            // Process the line
            if(line.contains("Input")){

                String[] splitsLine = line.replaceAll("\0","").split("=>")[1].trim().split(" , ");

                // Get the value
                String value = splitsLine[0].split(":")[1].trim();

                // Get the key
                // String key = splitsLine[1].split(":")[1].trim();

                // Get the timestamp
                long ts = Timestamp.valueOf(splitsLine[2].split(" : ")[1].trim()).getTime();

                Input input = new Input(value, coordinatorKey, ts);
                queries.add(input);
            }
            else{

                String value = line.trim();
                Input input = new Input(value, coordinatorKey, System.currentTimeMillis());
                queries.add(input);

            }

            // Read the next line
            line = br.readLine();
        }

        // Close the buffer
        br.close();

        return  queries;

    }



}
