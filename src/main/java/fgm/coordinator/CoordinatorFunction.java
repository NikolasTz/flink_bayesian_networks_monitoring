package fgm.coordinator;

import config.FGMConfig;
import datatypes.NodeBN_schema;
import datatypes.counter.Counter;
import datatypes.input.Input;
import datatypes.message.MessageFGM;
import fgm.state.CoordinatorState;
import job.JobKafka;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static config.InternalConfig.TypeMessage.*;
import static config.InternalConfig.TypeMessage;
import static datatypes.counter.Counter.findCardinality;
import static fgm.job.JobFGM.coordinator_stats;
import static fgm.state.FGMState.hashKey;
import static utils.MathUtils.*;
import static utils.Util.equalsList;
import static utils.Util.findIndex;

public class CoordinatorFunction {

    // Handle INCREMENT message
    public static void handleIncrement(CoordinatorState coordinatorState,
                                       MessageFGM message,
                                       CoProcessFunction<?,?,?>.Context ctx,
                                       Collector<MessageFGM> out) throws IOException {

        // Wait asynchronously for INCREMENT messages(after new subround or round)
        if(!coordinatorState.getSyncCord()) return;

        // Update the global counter c
        coordinatorState.setGlobalCounter((int) (coordinatorState.getGlobalCounter()+message.getValue()));

        // If c > k , where k is the number of workers
        if( coordinatorState.getGlobalCounter() > coordinatorState.getNumOfWorkers() ){

            // Finish the subround or round after receive all zi

            // Broadcast ZETA message
            broadcastZeta(coordinatorState,out);

            // Collect some statistics about broadcast messages
            // collectBroadcastMessages(ctx,ZETA,0);

            // Update the number of messages
            coordinatorState.setNumOfMessages(coordinatorState.getNumOfMessages()+coordinatorState.getNumOfWorkers());

            // Reset the global counter and psi
            coordinatorState.setGlobalCounter(0);
            coordinatorState.setPsi(0d);

            // Wait asynchronously for zeta.Update the sync flag
            coordinatorState.setSyncCord(false);
        }
    }

    // Handle ZETA message
    public static void handleZeta(CoordinatorState coordinatorState,
                                  MessageFGM message,
                                  CoProcessFunction<?,?,?>.Context ctx,
                                  Collector<MessageFGM> out) throws Exception {

        // Receive zeta only after INCREMENT messages or REBALANCE
        if(coordinatorState.getSyncCord()) return;

        // Update the global counter
        coordinatorState.setGlobalCounter(coordinatorState.getGlobalCounter()+1);

        // Update the psi value
        coordinatorState.setPsi(coordinatorState.getPsi()+message.getValue());

        // Update the parameters(testing purposes)
        // coordinatorState.updateParameters(message.getCounters());

        // Wait until receive numOfWorkers messages that is messages from all workers
        if( coordinatorState.getGlobalCounter() == coordinatorState.getNumOfWorkers() ){

            // Reset the global counter c
            coordinatorState.setGlobalCounter(0);

            // Calculate the phi(0) , use the estimated vector(not change) or save the phi.In this case we use the estimated state vector E
            double phi0 = coordinatorState.getSafeZone().safeFunction(coordinatorState.getEstimatedVector().getEmptyInstance(),coordinatorState.getEstimation());

            // Check the psi+psiBeta > epsilonPsi*k*phi(0)
            if( (coordinatorState.getPsi() + coordinatorState.getPsiBeta()) >= coordinatorState.getEpsilonPsi()*coordinatorState.getNumOfWorkers()*phi0){

                // The subrounds ends
                // New round begins

                // Finish the round,broadcast the DRIFT message
                broadcastDrift(coordinatorState,out);

                // Update the number of messages
                coordinatorState.setNumOfMessages(coordinatorState.getNumOfMessages()+coordinatorState.getNumOfWorkers());

                // Collect some statistics about broadcast messages
                // collectBroadcastMessages(ctx,DRIFT,0);

                // Collect some statistics about the state of Coordinator ???
            }
            else{

                // New subround begins
                newSubRound(coordinatorState,ctx,out);

                // Collect some statistics
                // estimateParameters(coordinatorState,ctx);
            }

            // Reset the counters(testing purposes)
            // coordinatorState.resetCounters();
        }

    }

    // Handle DRIFT message
    public static void handleDrift(CoordinatorState coordinatorState,
                                   MessageFGM message,
                                   CoProcessFunction<?,?,?>.Context ctx,
                                   Collector<MessageFGM> out) throws IOException {

        // Drift vectors always are welcome

        // Update the global counter c
        coordinatorState.setGlobalCounter(coordinatorState.getGlobalCounter()+1);

        // Update the balance vector B
        coordinatorState.setBalanceVector(addArrays(coordinatorState.getBalance(),message.getVector()));
        // coordinatorState.updateBalanceVector();

        // If received numOfWorkers messages
        if( coordinatorState.getGlobalCounter() == coordinatorState.getNumOfWorkers() ){

            // Reset the global counter c
            coordinatorState.setGlobalCounter(0);

            // Update global estimated vector => E + B/k , only after new round

            // Enable rebalancing after the zero round which is used as initialization round of FGM protocol
            // If we want the rebalancing enabled only once per round then leave the clause coordinatorState.getLambda() == 1
            if( coordinatorState.isEnableRebalancing() && coordinatorState.getNumOfRounds() >= 1 && coordinatorState.getLambda() == 1){

                // Attempt to begin new subround instead of new round
                rebalance(coordinatorState,ctx,out);
            }
            else{

                // New round begin
                newRound(coordinatorState,ctx,out);

                // Reset the balancing vector B and fields of rebalancing after new round
                resetRebalance(coordinatorState);
            }

        }

    }

    // New round
    public static void newRound(CoordinatorState coordinatorState,
                                CoProcessFunction<?,?,?>.Context ctx,
                                Collector<MessageFGM> out) throws IOException {

        // New round begin

        // Update global estimated vector => E + B/k
        // For SUM/COUNT the constant 1/k maybe not needed(Vector class not needed)
        coordinatorState.setEstimatedVector(addArrays(coordinatorState.getEstimation(),coordinatorState.getBalance()));
        // coordinatorState.updateEstimatedVector();

        // Collect some statistics
        ctx.output(coordinator_stats,"Number of round : " + coordinatorState.getNumOfRounds() + " and num of subrounds : " + coordinatorState.getNumOfSubrounds());

        // Reset the number of subRounds
        coordinatorState.setNumOfSubrounds(0L);

        // Update the number of rounds
        coordinatorState.setNumOfRounds(coordinatorState.getNumOfRounds()+1L);

        // Broadcast the global estimated vector to all workers
        // Start subrounds
        broadcastEstimate(coordinatorState,out);

        // Collect some statistics about broadcast messages
        // collectBroadcastMessages(ctx,ESTIMATE,coordinatorState.getEstimation());

        // Update the number of messages
        coordinatorState.setNumOfMessages(coordinatorState.getNumOfMessages()+coordinatorState.getNumOfWorkers());

        // Set the psi = k*phi(0) (verification purposes)
        // At the beginning of new round the psiBeta = 0 and lambda = 1
        double phi0 = coordinatorState.getSafeZone().safeFunction(coordinatorState.getEstimatedVector().getEmptyInstance(),coordinatorState.getEstimation());
        coordinatorState.setPsi(coordinatorState.getNumOfWorkers()*phi0);

        // Collect some statistics
        // if(coordinatorState.getTypeNetwork() == NAIVE ) getEstimationNBC(coordinatorState);
        // else getEstimation(coordinatorState);
        // getEstimatedClass(coordinatorState,new Input("Sunny,Mild,Normal,Strong","0",0));
        // estimateJointProbTruth(coordinatorState,new Input("False,False,False,False,False","0",0));
        // estimateJointProbEst(coordinatorState,new Input("False,False,False,False,False","0",0));

        // Update the syncCord.Wait for INCREMENT messages
        coordinatorState.setSyncCord(true);

    }

    // New subround
    public static void newSubRound(CoordinatorState coordinatorState,
                                   CoProcessFunction<?,?,?>.Context ctx,
                                   Collector<MessageFGM> out) throws IOException {

        // Update the number of subrounds
        coordinatorState.setNumOfSubrounds(coordinatorState.getNumOfSubrounds()+1L);

        // Calculate the new quantum(theta) = -(psi+psiBeta)/2k
        double quantum = -(coordinatorState.getPsi()+coordinatorState.getPsiBeta()) / (2*coordinatorState.getNumOfWorkers());

        // Broadcast quantum to all workers
        broadcastQuantum(coordinatorState,out,quantum);

        // Collect some statistics about broadcast messages
        // collectBroadcastMessages(ctx,QUANTUM,quantum);

        // Update the number of messages
        coordinatorState.setNumOfMessages(coordinatorState.getNumOfMessages()+coordinatorState.getNumOfWorkers());

        // Reset the global counter
        coordinatorState.setGlobalCounter(0);

        // Wait for INCREMENT messages
        coordinatorState.setSyncCord(true);
    }


    // Rebalance for reducing the upstream overhead of shipping E
    public static void rebalance(CoordinatorState coordinatorState,
                                 CoProcessFunction<?,?,?>.Context ctx,
                                 Collector<MessageFGM> out) throws IOException {

        // Choose a new lambda
        coordinatorState.setLambda(0.2d);

        // Calculate the psiBeta
        coordinatorState.setPsiBeta(calculatePsiBeta(coordinatorState));

        // Broadcast lambda to workers
        broadcastLambda(coordinatorState,out);

        // Update the number of messages
        coordinatorState.setNumOfMessages(coordinatorState.getNumOfMessages()+coordinatorState.getNumOfWorkers());

        // Collect some statistics
        // collectBroadcastMessages(ctx,LAMBDA,coordinatorState.getLambda());

        // Wait for ZETA messages
        coordinatorState.setSyncCord(false);

    }

    // Reset the field of rebalancing
    public static void resetRebalance(CoordinatorState coordinatorState) throws IOException {

        // Reset the balance vector B
        coordinatorState.getBalanceVector().resetVector();

        // Update the balance vector B
        // coordinatorState.updateBalanceVector();

        // Reset the psiBeta and lambda
        coordinatorState.setPsiBeta(0d);
        coordinatorState.setLambda(1.0d);
    }

    // Calculate the quantum
    public static double calculateQuantum(CoordinatorState coordinatorState) throws IOException {

        // Without rebalancing , quantum = -psi/2k
        return -coordinatorState.getPsi() / 2*coordinatorState.getNumOfWorkers();

        // With rebalancing , quantum = -(psi+psiBeta)/2k
        // return -(coordinatorState.getPsi()+coordinatorState.getPsiBeta()) / 2* coordinatorState.getNumOfWorkers();

    }

    // Calculate psiBeta
    public static double calculatePsiBeta(CoordinatorState coordinatorState) throws IOException {

        if(coordinatorState.getLambda() == 1) return 0;
        else{

            // Get the lambda
            double lambda = coordinatorState.getLambda();

            // Get the k
            int k = coordinatorState.getNumOfWorkers();

            // Calculate the phi(B/(1-lambda)k)
            // Balance vector acts as drift vector on safe function
            double phi = coordinatorState.getSafeZone().safeFunction(scaleArray(coordinatorState.getBalance(),1/((1-lambda)*k)),coordinatorState.getEstimation());

            // psiBeta = (1-lambda)*k*phi(B/(1-lambda)k)
            return (1-lambda)*k*phi;
        }
    }


    // *************************************************************************
    //                                  Broadcasts
    // *************************************************************************

    // Broadcast DRIFT message to all workers
    public static void broadcastDrift(CoordinatorState coordinatorState,Collector<MessageFGM> out) throws IOException {

        // Broadcast the message
        for(int i=0;i<coordinatorState.getNumOfWorkers();i++){
            out.collect(new MessageFGM(String.valueOf(i),DRIFT,System.currentTimeMillis(),0));
        }

        // Update the number of messages
        coordinatorState.setNumOfSentDriftMessages(coordinatorState.getNumOfSentDriftMessages()+coordinatorState.getNumOfWorkers());

    }

    // Broadcast QUANTUM message to all workers
    public static void broadcastQuantum(CoordinatorState coordinatorState,Collector<MessageFGM> out,double quantum) throws IOException {

        // Broadcast the message
        for(int i=0;i<coordinatorState.getNumOfWorkers();i++){
            out.collect(new MessageFGM(String.valueOf(i),QUANTUM,System.currentTimeMillis(),quantum));
        }

        // Update the number of messages
        coordinatorState.setNumOfSentQuantumMessages(coordinatorState.getNumOfSentQuantumMessages()+coordinatorState.getNumOfWorkers());
    }

    // Broadcast ESTIMATE message to all workers
    public static void broadcastEstimate(CoordinatorState coordinatorState,Collector<MessageFGM> out) throws IOException {

        // Broadcast the message
        for(int i=0;i<coordinatorState.getNumOfWorkers();i++){
            out.collect(new MessageFGM(String.valueOf(i),ESTIMATE,System.currentTimeMillis(),coordinatorState.getEstimation()));
        }

        // Update the number of messages
        coordinatorState.setNumOfSentEstMessages(coordinatorState.getNumOfSentEstMessages()+coordinatorState.getNumOfWorkers());
    }

    // Broadcast ZETA message to all workers
    public static void broadcastZeta(CoordinatorState coordinatorState,Collector<MessageFGM> out) throws IOException {

        // Broadcast the message
        for(int i=0;i<coordinatorState.getNumOfWorkers();i++){
            out.collect(new MessageFGM(String.valueOf(i),ZETA,System.currentTimeMillis(),0));
        }

        // Update the number of messages
        coordinatorState.setNumOfSentZetaMessages(coordinatorState.getNumOfSentZetaMessages()+coordinatorState.getNumOfWorkers());
    }

    // Broadcast LAMBDA message to all workers
    public static void broadcastLambda(CoordinatorState coordinatorState,Collector<MessageFGM> out) throws IOException {

        // Broadcast the message
        for(int i=0;i<coordinatorState.getNumOfWorkers();i++){
            out.collect(new MessageFGM(String.valueOf(i),LAMBDA,System.currentTimeMillis(),coordinatorState.getLambda()));
        }

        // Update the number of messages
        coordinatorState.setNumOfSentLambdaMessages(coordinatorState.getNumOfSentLambdaMessages()+coordinatorState.getNumOfWorkers());
    }


    // *************************************************************************
    //                               Start of FGM
    // *************************************************************************

    // Using for start of the FGM protocol.Register an event or processing timer to collect the drift vectors
    public static void start(CoordinatorState coordinatorState,
                             CoProcessFunction<?,?,?>.Context ctx,
                             FGMConfig config,
                             Input input) throws IOException {

        // Initialization
        coordinatorState.initializeCoordinatorState(config);
        coordinatorState.printCoordinatorState(ctx);
        ctx.output(coordinator_stats,"FGMConfig => "+config);

        System.out.println("Warmup : Register event timer");

        // Register an event or processing timer for the fist window. Choose based on config.enableWindow()
        // Timestamp firstWindow = new Timestamp(input.getTimestamp()-(input.getTimestamp() % config.windowSize()) + config.windowSize() - 1);
        // Timestamp now = new Timestamp(System.currentTimeMillis());

        // Register an event or processing timer to collect the drift vectors ???
        // ctx.timerService().registerEventTimeTimer(System.currentTimeMillis()-(System.currentTimeMillis() % config.windowSize()) + config.windowSize() - 1);
        ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis()+20);
        // (input.getTimestamp() - (input.getTimestamp() % windowSize) + windowSize - 1)

    }


    // *************************************************************************
    //                           Parameter estimation
    // *************************************************************************

    // Bayesian Network(BN)

    // Get the estimation for all parameters of Bayesian Network(BN)
    public static void getEstimation(CoordinatorState coordinatorState) throws IOException {

        // For each parameter , calculate the estimation
        for (Counter counter : coordinatorState.getCounters().values()) {

            // Get the key
            // String key = getKey(counter);

            // Get the estimation
            double estimation = coordinatorState.getEstimatedVector().estimate(counter.getCounterKey());

            // Collect some statistics
            System.out.println( "Counter => "
                                + " , name : " + counter.getNodeName()
                                + " , node values : " + counter.getNodeValue()
                                + " , nodeParents : " + counter.getNodeParents()
                                + " , nodeParentsValues : " + counter.getNodeParentValues()
                                + " , estimated count : " + estimation);

        }
    }

    // Estimate all the parameters of Bayesian Network(BN)
    public static void estimateParameters(CoordinatorState coordinatorState, CoProcessFunction<?,?,?>.Context ctx) throws Exception {

        double parameter;
        Counter counter;

        for (long key : coordinatorState.getCounters().keySet()){

            counter = coordinatorState.getCounter(key);

            if(counter.isActualNode()){

                // Estimate the parameter
                parameter = estimateParameter(coordinatorState,counter);

                // Get the truth parameter
                double truthParameter = counter.getTrueParameter();

                // Collect some statistics about parameters
                ctx.output(coordinator_stats, "Parameter : " + parameter
                                                 + " " + counter.getParameter()
                                                 + " , error : " + Math.abs(parameter-truthParameter));
            }

        }

    }

    // Estimate the joint probability based on truth parameters(BN)
    public static double estimateJointProbTruth(CoordinatorState coordinatorState, Input input) throws Exception {

        // Get the values of input
        List<String> values = Arrays.asList(input.getValue().replaceAll(" ","").split(","));

        // The log of joint probability of input
        // Log to avoid underflow problems
        // double prob = 1.0d;
        double logProb = 0d;
        double trueParameter;

        // Parsing all nodes
        for( NodeBN_schema node : coordinatorState.getNodes() ) {

            // String buffer
            StringBuffer sb = new StringBuffer();

            // Get the name of node and appended
            String name = node.getName();
            sb.append(name);

            // Check if exists parents
            if (node.getParents().length != 0) {

                // Find and append the value from input using the schema of dataset
                sb.append(",").append(values.get(coordinatorState.getDatasetSchema().get(name)));

                // Get the name and values of parents node
                StringBuilder nameParents = new StringBuilder();
                StringBuilder valuesParents = new StringBuilder();
                for (NodeBN_schema nodeParents : node.getParents()) {
                    nameParents.append(nodeParents.getName()).append(",");
                    valuesParents.append(values.get(coordinatorState.getDatasetSchema().get(nodeParents.getName()))).append(",");
                }

                // Delete the last comma
                valuesParents.deleteCharAt(valuesParents.length() - 1);

                // Get the key
                String key = sb.append(",").append(nameParents).append(valuesParents).toString().replace(" ", "");

                // Get the true parameter(hashing the key and get the counter)
                trueParameter = coordinatorState.getCounters().get(hashKey(key.getBytes())).getTrueParameter();

            }
            else {

                // Find and append the value from input using the schema of dataset
                sb.append(",").append(values.get(coordinatorState.getDatasetSchema().get(name)));

                // Get the key
                String key = sb.toString().replace(" ", "");

                // Get the true parameter(hashing the key and get the counter)
                trueParameter = coordinatorState.getCounters().get(hashKey(key.getBytes())).getTrueParameter();
            }

            // Update the probability
            // prob = prob * counter.getTrueParameter();
            logProb += log(trueParameter);

        }

        return logProb;
    }

    // Estimate the joint probability based on estimated parameters of Bayesian Network(BN)
    public static double estimateJointProbEst(CoordinatorState coordinatorState, Input input) throws Exception {

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
        for(NodeBN_schema node : coordinatorState.getNodes()){

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
                sb.append(",").append(values.get(coordinatorState.getDatasetSchema().get(name)));

                // Get and append the name and values of parents nodes
                StringBuilder nameParents = new StringBuilder();
                StringBuilder valuesParents = new StringBuilder();
                for(NodeBN_schema nodeParents : node.getParents()){
                    nameParents.append(nodeParents.getName()).append(",");
                    valuesParents.append(values.get(coordinatorState.getDatasetSchema().get(nodeParents.getName()))).append(",");
                }

                // Delete the last comma
                valuesParents.deleteCharAt(valuesParents.length()-1);

                // Get the key
                String key = sb.append(",").append(nameParents).append(valuesParents).toString().replace(" ","");

                // Hashing the key and get the counter
                Counter counter = coordinatorState.getCounters().get(hashKey(key.getBytes()));

                // Get the estimation
                estimator_node = coordinatorState.getEstimatedVector().estimate(counter.getCounterKey());


                // Estimator parent
                // Get the key
                String parentKey = (nameParents.toString()+valuesParents+","+name).replace(" ","");

                // Hashing the key and get the parent-counter
                Counter parentCounter = coordinatorState.getCounters().get(hashKey(parentKey.getBytes()));

                // Get the estimation
                estimator_parent = coordinatorState.getEstimatedVector().estimate(parentCounter.getCounterKey());
            }
            else{

                // Parameter = estimator_node / estimator_parent

                // Estimator node
                // Find and append the value from input using the schema of dataset
                sb.append(",").append(values.get(coordinatorState.getDatasetSchema().get(name)));

                // Get the key
                String key = sb.toString().replace(" ","");

                // Hashing the key and get the counter
                Counter counter = coordinatorState.getCounters().get(hashKey(key.getBytes()));

                // Get the estimation
                estimator_node = coordinatorState.getEstimatedVector().estimate(counter.getCounterKey());


                // Estimator parent
                // For each value of node
                String parent_key;
                for(String parent_value : node.getValues()){

                    // Get the key
                    parent_key = (name+","+parent_value).replace(" ","");

                    // Hashing the key and get the counter
                    Counter parentCounter = coordinatorState.getCounters().get(hashKey(parent_key.getBytes()));

                    // Get the estimation
                    estimator_parent += coordinatorState.getEstimatedVector().estimate(parentCounter.getCounterKey());
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

    // Estimate the parameter for Bayesian Network(BN)
    public static double estimateParameter(CoordinatorState coordinatorState, Counter counter) throws Exception {

        double estimator_node;
        double estimator_parent = 0d;
        long parent_index = -1L;
        // String parentKey;

        ArrayList<Long> indexes = null;
        Counter parent_counter;

        // Check for parents node
        if(counter.getNodeParents() != null){
            // Find the parent node
            parent_index = findParentNode(coordinatorState,counter);
        }
        else{
            // Find all indexes of parent node for the counter
            indexes = findIndexesCounter(coordinatorState,counter);
        }


        // Parameter = estimator_node / estimator_parent

        // Get the estimation
        estimator_node = coordinatorState.getEstimatedVector().estimate(counter.getCounterKey());

        // Calculate the estimator of parent
        if( parent_index != -1L ){

            // Get the parent node
            parent_counter = coordinatorState.getCounter(parent_index);

            // Get the estimation using the key
            estimator_parent = coordinatorState.getEstimatedVector().estimate(parent_counter.getCounterKey());
        }
        else{
            for(Long indexNode : Objects.requireNonNull(indexes) ){

                // Get the key of parent node and using it for the estimation
                estimator_parent += coordinatorState.getEstimatedVector().estimate(coordinatorState.getCounter(indexNode).getCounterKey());

            }
        }

        // Laplace smoothing for probability of parameter(only for zero parameters)
        return laplaceSmoothing(estimator_node,estimator_parent,findCardinality(new ArrayList<>(coordinatorState.getCounters().values()),counter),1.0d);

    }

    // Find the parents node of search counter(BN)
    public static long findParentNode(CoordinatorState coordinatorState, Counter searchCounter) throws Exception {

        Counter counter;

        for(long key : coordinatorState.getCounters().keySet()){

            counter = coordinatorState.getCounter(key);

            if( equalsList(counter.getNodeName(),searchCounter.getNodeParents())
                    && equalsList(counter.getNodeValue(),searchCounter.getNodeParentValues())
                    && equalsList(counter.getNodeParents(),searchCounter.getNodeName()) ){

                return key;
            }

        }

        return -1L;
    }

    // Find all the indexes of search counter(only for orphan counter) , that is all the counters with the same node name(BN)
    public static ArrayList<Long> findIndexesCounter(CoordinatorState coordinatorState, Counter searchCounter) throws Exception {

        ArrayList<Long> indexes = new ArrayList<>();
        Counter counter;

        for(long key : coordinatorState.getCounters().keySet()){

            counter = coordinatorState.getCounter(key);

            if( equalsList(counter.getNodeName(),searchCounter.getNodeName())
                    && equalsList(counter.getNodeParents(),searchCounter.getNodeParents())
                    && equalsList(counter.getNodeParentValues(),searchCounter.getNodeParentValues())
                    && counter.isActualNode() == searchCounter.isActualNode() ){

                indexes.add(key);
            }
        }

        return indexes;
    }


    // Naive Bayes Classifier(NBC)

    // Get the estimation for all parameters of Naive Bayes Classifier(NBC)
    public static void getEstimationNBC(CoordinatorState coordinatorState) throws IOException{

        // Estimated total count
        double estimatedTotalCount = 0;

        // For each parameter , calculate the estimation
        for (Counter counter : coordinatorState.getCounters().values()) {

            // Get the estimation
            double estimation = coordinatorState.getEstimatedVector().estimate(counter.getCounterKey());
            if( counter.getNodeParents() == null ) estimatedTotalCount += estimation; // Class node

            // Collect some statistics
            System.out.println( "Counter => "
                                + " , name : " + counter.getNodeName()
                                + " , node values : " + counter.getNodeValue()
                                + " , nodeParents : " + counter.getNodeParents()
                                + " , nodeParentsValues : " + counter.getNodeParentValues()
                                + " , estimated count : " + estimation
                                + " , actual count : " + counter.getCounter());

        }

        System.out.println("Estimated total count : " + estimatedTotalCount);

    }

    // Estimate the class of input(NBC) , using MAP(Maximum a Posterior) that is the class with the highest probability
    public static String getEstimatedClass(CoordinatorState coordinatorState,Input input) throws IOException {

        // For each class value calculate the probability and keep the max
        // In Naive Bayes Classifier set up the attributes are mutually independent
        // For Naive Bayes Classifier the trueParameter field as estimated count on this function

        // P(Ci|Attributes) = P(Ci) * P(Attributes|Ci) / P(Attributes)
        // P(Ci) = Count(Ci) / Total count
        // P(Ai|Ci) = Count(Ai and Ci) / Count(Ci)
        // P(Ai) = Count(Ai) / Count(Ai for all values of attribute) but these probabilities are the same(not calculate) that is the denominator is the same

        // Get the values of input
        List<String> inputValues = Arrays.asList(input.getValue().replaceAll(" ","").split(","));
        double estimatedTotalCount = 0; // Estimated total count

        // Convention : Class node locates at the last position of nodes
        NodeBN_schema classNode = coordinatorState.getNodes()[(int) (coordinatorState.getNumOfNodes()-1)];

        // Get the estimated total count
        for (String value : classNode.getValues()){

            // Get the key
            int index = coordinatorState.getDatasetSchema().get(classNode.getName());
            String classKey = (index +","+ value).replace(" ","");

            // Update the estimated total count
            estimatedTotalCount += coordinatorState.getEstimatedVector().estimate(hashKey(classKey.getBytes()));
        }

        // Calculate the probability for each class value
        double prob = Double.NEGATIVE_INFINITY;
        String classValue = null;

        double logProbClass;
        double classEstimator;
        int index;

        // For each class value
        for (String value : classNode.getValues()){

            // Format of key : <index,attr_value,class_value>

            // Class node
            // Log to avoid underflow problems
            // Calculate the probability of class node
            // double probClass;

            // Get the key of class counter
            index = coordinatorState.getDatasetSchema().get(classNode.getName());
            String classKey = (index +","+ value).replace(" ","");

            // Get the class counter
            Counter classCounter = coordinatorState.getCounter(hashKey(classKey.getBytes()));

            // Get the estimation of class counter
            classEstimator = coordinatorState.getEstimatedVector().estimate(classCounter.getCounterKey());

            // Laplace smoothing for probability of class(only for zero parameters) - Avoid overfitting and zero probabilities
            logProbClass = log(laplaceSmoothing(classEstimator,estimatedTotalCount, classNode.getCardinality(),1.0d));

            // Probability of attribute
            double probAttr;
            double attrEstimator;

            // Calculate the probability of attributes - Convention
            String attrValue;
            String attrKey;
            for(int i=0;i<(coordinatorState.getNodes().length-1);i++){

                // Get the key of attribute counter
                index = coordinatorState.getDatasetSchema().get(coordinatorState.getNode(i).getName());
                attrValue = inputValues.get(index);
                attrKey = (index +","+attrValue+","+ value).replace(" ","");

                // Get the attribute counter
                Counter attrCounter = coordinatorState.getCounter(hashKey(attrKey.getBytes()));

                // Get the estimation of attribute
                attrEstimator = coordinatorState.getEstimatedVector().estimate(attrCounter.getCounterKey());

                // Laplace smoothing for probability of attribute(only for zero parameters)
                probAttr = laplaceSmoothing(attrEstimator,classEstimator,coordinatorState.getNodes()[i].getCardinality(),1.0d);

                // Update the probability
                // probClass *= probAttr;
                logProbClass += log(probAttr);

            }

            // Update the probability and class value
            if(logProbClass >= prob){
                prob = logProbClass; // Update the probability
                classValue = classCounter.getNodeValue().get(0); // Get the class value
            }
        }

        return classValue;

    }

    // Check if parameter is needed on estimation of probability(NBC)
    public static boolean checkParameter(List<String> inputValues, Counter parameter, List<String> datasetSchema){

        int index;

        // Check the node values
        ArrayList<String> counterNames = parameter.getNodeName(); // Names of node
        ArrayList<String> counterValues = parameter.getNodeValue(); // Values of node
        for (int i=0;i<counterNames.size();i++){

            index = findIndex(datasetSchema,counterNames.get(i));
            if(index == -1) return false; // Not found
            if(!counterValues.get(i).equals(inputValues.get(index))) return false; // Not needed

        }
        return true;
    }


    // *************************************************************************
    //                                 Queries
    // *************************************************************************

    // Bayesian Network(BN)
    public static void processingQueriesBN(List<Input> queries,
                                           CoordinatorState coordinatorState,
                                           CoProcessFunction<?,?,?>.Context ctx) throws Exception {

        // Lower and upper bound violations
        int lbv = 0,ubv = 0;
        double avgError = 0; double avgAbsError = 0;
        double avgGT = 0;
        ctx.output(coordinator_stats,"\nProcessing BN queries : "+queries.size()+"\n");

        // Processing the queries
        for(Input query : queries){

            double truthProb = estimateJointProbTruth(coordinatorState,query);
            double estProb = estimateJointProbEst(coordinatorState,query);
            double error = estProb-truthProb;

            // Collect some statistics
            ctx.output(coordinator_stats, "Input : " + query
                                             + " , estimated probability : " + estProb
                                             + " , truth probability : " + truthProb
                                             + " , error : " + error
                                             + " , absolute error : " + Math.abs(error)
                                             // + "," + Math.exp(-coordinatorState.getEps()*coordinatorState.getDatasetSchema().size()) + "  <= estimated / truth = " + estProb/truthProb + " <= " + Math.exp(coordinatorState.getEps()*coordinatorState.getDatasetSchema().size())
                                             + " , " + -coordinatorState.getEps()*coordinatorState.getDatasetSchema().size() + "  <= ln(estimated) - ln(truth) = " + (estProb-truthProb) + " <= " + coordinatorState.getEps()*coordinatorState.getDatasetSchema().size()
                                             + " with probability " + (1-coordinatorState.getDelta()));

            if( (estProb-truthProb) >= (coordinatorState.getEps()*coordinatorState.getDatasetSchema().size()) ){ ubv++; }
            else if( (estProb-truthProb) <= (-coordinatorState.getEps()*coordinatorState.getDatasetSchema().size()) ){ lbv++; }

            // Update the average error
            avgError += error;
            avgAbsError += Math.abs(error);

            // Update the average of GT log probability
            avgGT += truthProb;
        }

        System.out.println("\nProcessed queries : "+queries.size()+"\n");
        ctx.output(coordinator_stats,"Average Ground Truth Log probability : "+ avgGT/queries.size());
        ctx.output(coordinator_stats,"Average absolute error : " + avgAbsError/queries.size());
        ctx.output(coordinator_stats, "Average error : " + avgError/queries.size()
                                                            + " , lower violations : " + lbv
                                                            + " , upper violations : " + ubv);

        // Reset the endOfStreamCounter
        coordinatorState.setEndOfStreamCounter(0);
    }

    // Naive Bayes Classifier(NBC)
    public static void processingQueriesNBC(List<Input> queries,
                                            CoordinatorState coordinatorState,
                                            CoProcessFunction<?,?,?>.Context ctx) throws Exception {

        // Collect some statistics
        ctx.output(coordinator_stats,"\nProcessing BN queries : "+queries.size()+"\n");

        // Processing the queries
        for(Input query : queries){

            // For each query estimate the class label
            String classValue = getEstimatedClass(coordinatorState,query);

            // Find accuracy,F1 score ???

            ctx.output(JobKafka.coordinator_stats,"Input : " + query + " , estimated class : " + classValue);

        }

        // Collect some statistics
        System.out.println("\nProcessed queries : "+queries.size()+"\n");

        // Reset the endOfStreamCounter
        coordinatorState.setEndOfStreamCounter(0);

    }


    // *************************************************************************
    //                                 Metrics
    // *************************************************************************

    // Collect statistics about broadcast messages
    public static <V> void collectBroadcastMessages(CoProcessFunction<?,?,?>.Context ctx,
                                                    TypeMessage typeMessage,
                                                    V value){

        /*ctx.output(coordinator_stats,"\nCoordinator : broadcast message to workers "
                                        + " , type_message : " + typeMessage
                                        + " , value of message : " + value
                                        + " at " + new Timestamp(System.currentTimeMillis())+"\n");*/

    }

    // Collect statistics about received messages
    public static void collectRecMessages(CoProcessFunction<?,?,?>.Context ctx,MessageFGM message){

        /*ctx.output(coordinator_stats,"\nCoordinator : "
                                        + " receive message : " + message
                                        + " at " + new Timestamp(System.currentTimeMillis()));*/

    }

    // Collect statistics about coordinator state
    public static void collectCordState(CoordinatorState coordinatorState,CoProcessFunction<?,?,?>.Context ctx){
        // ctx.output(coordinator_stats,"\n"+coordinatorState.toString());
    }

}
