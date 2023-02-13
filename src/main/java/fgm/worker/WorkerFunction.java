package fgm.worker;

import datatypes.NodeBN_schema;
import datatypes.counter.Counter;
import datatypes.input.Input;
import datatypes.message.MessageFGM;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import fgm.state.WorkerState;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static config.InternalConfig.TypeMessage.*;
import static config.InternalConfig.TypeMessage;
import static fgm.job.JobFGM.worker_stats;
import static fgm.state.FGMState.hashKey;
import static utils.MathUtils.compareArrays;
import static utils.MathUtils.scaleArray;
import static utils.Util.convertToString;
import static config.InternalConfig.TypeNetwork.NAIVE;

public class WorkerFunction {

    public static void handleInput(WorkerState workerState,
                                   KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                   Input input,
                                   Collector<MessageFGM> out) throws IOException {

        // ctx.output(worker_stats, "\nWorker " + ctx.getCurrentKey()+" input : "+input.getValue());

        // Update total count
        workerState.setTotalCount(workerState.getTotalCount()+1L);

        // Update the drift vector
        if( workerState.getTypeNetwork() == NAIVE){ updateDriftVectorNBC(workerState,input); }
        else{ updateDriftVectorBN(workerState,input); }


        // Alternative way is to do it continuously or when the timestamp is changed(or per window checking) - input.getTimestamp() != workerState.getLastTs()
        if( workerState.getTotalCount() == 10 ){
            // Send drift vector - Warm up round
            sendDriftVector(workerState,ctx,out);
        }

        // Update the timestamp
        workerState.setLastTs(input.getTimestamp());

        // Collect some statistics
        // ctx.output(worker_stats,"Worker "+ctx.getCurrentKey()+" : Attempt subround process with flag: "+workerState.getSyncWorker()+" at "+ new Timestamp(workerState.getLastTs()));

        // Start subround process
        subRound(workerState,ctx,out);

        // Collect some statistics
        // collectWorkerState(workerState,ctx);

        if( workerState.getTotalCount() % 5000 == 0 ){ System.out.println("Worker "+ctx.getCurrentKey()+" processing 5000 tuples"); }

    }


    // Update the drift vector of Bayesian Network(BN)
    public static void updateDriftVectorBN(WorkerState workerState,Input input) throws IOException {

        // Values of input
        List<String> inputValues = Arrays.asList(input.getValue().replaceAll(" ","").split(","));

        // For each of node of BN or NBC
        for(NodeBN_schema node : workerState.getNodes() ){

            // String buffer
            StringBuffer sb = new StringBuffer();

            // Get the name of node and appended
            String name = node.getName();
            sb.append(name);

            // Check if exists parents
            if(node.getParents().length != 0){

                // Counter
                // Find and append the value from input using the schema of dataset
                sb.append(",").append(inputValues.get(workerState.getDatasetSchema().get(name)));

                // Get and append the name and values of parents nodes
                StringBuilder nameParents = new StringBuilder();
                StringBuilder valuesParents = new StringBuilder();
                for(NodeBN_schema nodeParents : node.getParents()){
                    nameParents.append(nodeParents.getName()).append(",");
                    valuesParents.append(inputValues.get(workerState.getDatasetSchema().get(nodeParents.getName()))).append(",");
                }

                // Delete the last comma
                valuesParents.deleteCharAt(valuesParents.length()-1);

                // Get the key
                String key = sb.append(",").append(nameParents).append(valuesParents).toString().replace(" ","");

                // Hashing the key and get the counter
                Counter counter = workerState.getCounters().get(hashKey(key.getBytes()));

                // Update the drift vector
                workerState.getDriftVector().update(counter.getCounterKey());

                // Update the counter,testing purposes
                counter.setCounter(counter.getCounter()+1L);



                // Parent counter
                // Get the key of parent counter
                String parentKey = (nameParents.toString()+valuesParents+","+name).replace(" ","");

                // Hashing the key and get the parent-counter
                Counter parentCounter = workerState.getCounters().get(hashKey(parentKey.getBytes()));

                // Update the drift vector
                workerState.getDriftVector().update(parentCounter.getCounterKey());

                // Update the parent-counter,testing purposes
                parentCounter.setCounter(parentCounter.getCounter()+1L);

            }
            else{

                // Find and append the value from input using the schema of dataset
                sb.append(",").append(inputValues.get(workerState.getDatasetSchema().get(name)));

                // Get the key
                String key = sb.toString().replace(" ","");

                // Hashing the key and get the counter
                Counter counter = workerState.getCounters().get(hashKey(key.getBytes()));

                // Update the drift vector
                workerState.getDriftVector().update(counter.getCounterKey());

                // Update the counter,testing purposes
                counter.setCounter(counter.getCounter()+1L);

            }

        }

        // Update the drift vector
        // workerState.updateDriftVector();

    }

    // Update the drift vector of Naive Bayes Classifiers(NBC)
    public static void updateDriftVectorNBC(WorkerState workerState,Input input) throws IOException{

        // Values of input
        List<String> inputValues = Arrays.asList(input.getValue().replaceAll(" ","").split(","));


        // Convention : The last node of NodeBN_schema array correspond to the class node and for class counter/parameter => nodeParents and nodeParentsValues = null
        // Class node
        NodeBN_schema classNode = workerState.getNodes()[(int) (workerState.getNumOfNodes()-1)];
        int classIndex = workerState.getDatasetSchema().get(classNode.getName());
        String classValue = inputValues.get(classIndex);
        String classKey = (classIndex+","+classValue).replace(" ","");
        Counter classCounter = workerState.getCounters().get(hashKey(classKey.getBytes()));

        // Update the drift vector
        workerState.getDriftVector().update(classCounter.getCounterKey());

        // Update the counter,testing purposes
        classCounter.setCounter(classCounter.getCounter()+1L);


        // Another option for key is the following form : <index,attr_value,class_value>
        // Attribute nodes
        // For each attribute node of NBC
        int index;
        for(int i=0;i<workerState.getNumOfNodes()-1;i++){

            // String buffer
            StringBuffer sb = new StringBuffer();

            // Get the index
            index = workerState.getDatasetSchema().get(workerState.getNode(i).getName());

            // Find and append the index,the attribute value from input using the schema of dataset and the class value
            sb.append(index).append(",").append(inputValues.get(index)).append(",").append(classValue);

            // Get the key
            String key = sb.toString().replace(" ","");

            // Hashing the key and get the counter
            Counter attrCounter = workerState.getCounters().get(hashKey(key.getBytes()));

            // Update the drift vector
            workerState.getDriftVector().update(attrCounter.getCounterKey());

            // Update the counter,testing purposes
            attrCounter.setCounter(attrCounter.getCounter()+1L);

        }

    }

    // Update the drift vector - opt_v1
   /* public static void updateDriftVector(WorkerState workerState,Input input) throws IOException {

        // Values of input
        List<String> inputValues = Arrays.asList(input.getValue().replaceAll(" ","").split(","));

        // Increment all counters based on input , update the sketch
        for (Counter counter : workerState.getParameters()) {
            if (checkIncrement(inputValues, counter, workerState.getDatasetSchema())){

                // Get the key
                // String key = workerState.getTypeNetwork() == NAIVE ? getKeyNBC(findIndex(workerState.getDatasetSchema(),counter.getNodeName().get(0)),counter) : getKey(counter);

                // Update the drift vector
                workerState.getDriftVector().update(counter.getCounterKey());

                // Update the counter,testing purposes
                counter.setCounter(counter.getCounter()+1L);

            }
        }

        // Update the drift vector
        workerState.updateDriftVector();

    }*/


    // For Bayesian Network(BN)
    // Get the key from Counter. The Counter in this case treat as parameter
    // The key is used for hashing.The key is a combination of nodeNames,nodeValues,nodeParentsNames and nodeParentsValues
    // The formation of key is the following : <nodeNames,nodeValues,nodeParentsNames,nodeParentsValues>
    public static String getKey(Counter counter){

        String key;

        // Get the node name
        ArrayList<String> names = new ArrayList<>(counter.getNodeName());

        // Append the node values
        names.addAll(counter.getNodeValue());

        // Append the node parents names if exists
        if( counter.getNodeParents() != null ){ names.addAll(counter.getNodeParents()); }

        // Append the node parents values if exists
        if( counter.getNodeParentValues() != null ){ names.addAll(counter.getNodeParentValues()); }

        // Trim all whitespaces
        key = convertToString(names).replace(" ","");

        return key;
    }


    // For Naive Bayes classifiers(NBC)
    // Another option for key is the following form : <index,attr_value,class_value>
    public static String getKeyNBC(int index,Counter counter){

        String key;

        // Get the attribute value
        ArrayList<String> attrValue = new ArrayList<>();

        // Add the index
        attrValue.add(String.valueOf(index));

        // Add the attribute value
        attrValue.addAll(counter.getNodeValue());

        // Get the class value
        if(counter.getNodeParents() != null ) attrValue.addAll(counter.getNodeParentValues());

        // Trim all whitespaces
        key = convertToString(attrValue).replace(" ","");

        return key;
    }


    // Estimate the counters(CountMin-sketch)
    public static void estimateCM(WorkerState workerState,KeyedCoProcessFunction<?,?,?,?>.Context ctx) throws IOException {

        int vlb = 0; // Number of violation lower bound
        int vub = 0; // Number of violation lower bound

        for (Counter counter : workerState.getCounters().values()){

            // Get key
            // String key = getKey(counter);

            // Get the estimation using murmur3
            double estimatedCount = workerState.getDriftVector().estimate(counter.getCounterKey());

            // Update the lower and upper bounds violations
            if( estimatedCount < counter.getCounter() ) vlb++;
            if( estimatedCount > (counter.getCounter() + (0.0013315579227696406*workerState.getTotalCount()*131)) ) vub++;
        }

        // Collect some statistics
        ctx.output(worker_stats, "\nWorker " + ctx.getCurrentKey()
                                 + " , lower bound violations : " + vlb
                                 + " , upper bound violations : " + vub
                                 + " total count : " + workerState.getTotalCount());

    }

    // Send INCREMENT message
    public static void sendIncrement(WorkerState workerState,
                                     KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                     Collector<MessageFGM> out) throws IOException {

        // Create the message
        MessageFGM message = new MessageFGM(ctx.getCurrentKey().toString(),INCREMENT,System.currentTimeMillis(),workerState.getLocalCounter());
        // MessageFGM message = new MessageFGM(ctx.getCurrentKey().toString(),INCREMENT,ctx.timestamp(),workerState.getLocalCounter()); ???

        // Update the number of messages
        workerState.setNumOfMessages(workerState.getNumOfMessages()+1L);

        // Send the message
        out.collect(message);

        // Collect some statistics
        // collectMessages(ctx,INCREMENT);

    }

    // Send DRIFT vector
    public static void sendDriftVector(WorkerState workerState,
                                       KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                       Collector<MessageFGM> out) throws IOException {

        // Create the message
        MessageFGM message = new MessageFGM(ctx.getCurrentKey().toString(),DRIFT,System.currentTimeMillis(),workerState.getDrift());
        // MessageFGM message = new MessageFGM(ctx.getCurrentKey().toString(),DRIFT,ctx.timestamp(),workerState.getDrift()); ???

        // estimateCM(workerState,ctx); // Estimate the counts per round(testing purposes) - CM ???
        // workerState.resetParameters(); // Reset all the counters - CM ???

        // Reset the drift vector
        workerState.getDriftVector().resetVector();

        // Update the driftVector
        // workerState.updateDriftVector();

        // Update the number of messages
        workerState.setNumOfMessages(workerState.getNumOfMessages()+1L);

        // Send the message
        out.collect(message);

        // Collect some statistics
        // collectMessages(ctx,DRIFT);

    }

    // Send ZETA
    public static void sendZeta(WorkerState workerState,
                                KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                Collector<MessageFGM> out) throws IOException {


        // Update the zeta = lambda*phi(Xi/lambda), with the last drift vector Xi (verification purposes)
        double phi = workerState.getLambda()*workerState.getSafeZone().safeFunction(scaleArray(workerState.getDrift(),1/workerState.getLambda()),workerState.getEstimation());

        // Update the zeta
        workerState.setZeta(phi);

        // Create the message
        MessageFGM message = new MessageFGM(ctx.getCurrentKey().toString(),ZETA,System.currentTimeMillis(),workerState.getZeta());
        // MessageFGM message = new MessageFGM(ctx.getCurrentKey().toString(),ZETA,System.currentTimeMillis(),workerState.getZeta(),workerState.getParameters()); - testing purposes
        // MessageFGM message = new MessageFGM(ctx.getCurrentKey().toString(),ZETA,ctx.timestamp(),workerState.getZeta()); ???

        // Update the number of messages
        workerState.setNumOfMessages(workerState.getNumOfMessages()+1L);

        // Wait for the new round or new subround , that is wait for global estimated vector E or new quantum
        workerState.setSyncWorker(false);

        // Send the message
        out.collect(message);

        // Collect some statistics
        // collectMessages(ctx,ZETA);

    }



    // New round
    public static void newRound(WorkerState workerState,MessageFGM message) throws IOException {

        // Receive the estimated vector E , using for calculate the phi function
        workerState.setEstimatedVector(message.getVector());

        // Update the estimatedVector
        // workerState.updateEstimatedVector();

        // Initialize the lambda
        workerState.setLambda(1.0d);

        // Calculate the phi(0)( only for verification )
        double phi0 = workerState.getSafeZone().safeFunction(workerState.getDriftVector().getEmptyInstance(),workerState.getEstimation());

        // Update the quantum(only the first subround of round the quantum = -phi(0)/2, available via the estimated vector E)
        workerState.setQuantum(-phi0/2);

        // Update the initial zeta
        workerState.setInitialZeta(phi0);

        // Update the zeta
        workerState.setZeta(phi0);

        // Reset the local counter ci
        workerState.setLocalCounter(0);

        // Wait to finish the round
        // Update the sync flag
        workerState.setSyncWorker(true);

    }

    // New subround
    public static void newSubRound(WorkerState workerState,MessageFGM message) throws IOException {

        // At the beginning of first subround of round the quantum = -phi(0)/2
        // If call this function , then we are in second subround of current round

        // Update the quantum
        workerState.setQuantum(message.getValue());

        // Initialize the zeta,act as the initial zeta = lambda*phi(Xi/lambda) during the subround
        workerState.setInitialZeta(workerState.getLambda()*workerState.getSafeZone().safeFunction(scaleArray(workerState.getDrift(),1/workerState.getLambda()),workerState.getEstimation()));

        // Update the zeta
        workerState.setZeta(workerState.getInitialZeta());

        // Reset the local counter ci
        workerState.setLocalCounter(0);

        // Update the sync flag
        workerState.setSyncWorker(true);
    }

    // The process of subround on worker side
    public static void subRound(WorkerState workerState,
                                KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                Collector<MessageFGM> out) throws IOException {

        // Updates on drift vector
        // Wait for updates
        if(!workerState.getSyncWorker()) return;

        // Empty drift vector(no updates),then do nothing (happen after the beginning of new round)
        if(compareArrays(workerState.getDrift(),workerState.getDriftVector().getEmptyInstance())) return;

        // Get quantum
        double quantum = workerState.getQuantum();

        // Get the initial zeta(at the start of subround)
        double zeta = workerState.getInitialZeta();

        // Calculate the phi = lambda*phi(Xi/lambda) based on last drift vector Xi
        double phi = workerState.getLambda()*workerState.getSafeZone().safeFunction(scaleArray(workerState.getDrift(),1/workerState.getLambda()),workerState.getEstimation());

        // Update the zeta
        workerState.setZeta(phi);

        // Check if quantum is zero
        if( quantum == 0 ) ctx.output(worker_stats,"Quantum is zero !!!");

        // Find the maximum between local counter ci and the quantity phi(Xi)-zi / quantum
        double max = Math.max(workerState.getLocalCounter(),(phi-zeta)/quantum);

        // Increment on local counter
        if( max > workerState.getLocalCounter() ){

            // Update the local counter
            workerState.setLocalCounter((int) max);

            // Send increment message to coordinator
            sendIncrement(workerState,ctx,out);
        }


    }

    // Rebalancing
    public static void handleLambda(WorkerState workerState,MessageFGM message) throws IOException {

        // Update the lambda
        workerState.setLambda(message.getValue());

    }


    // *************************************************************************
    //                                 Metrics
    // *************************************************************************

    // Collect statistics about sent messages and write to side output
    public static void collectMessages(KeyedCoProcessFunction<?,?,?,?>.Context ctx,TypeMessage typeMessage){

        /*ctx.output(worker_stats, "\nWorker " + ctx.getCurrentKey()
                                    + " , send a message to coordinator"
                                    + " , type_message : " + typeMessage
                                    + " at " + new Timestamp(System.currentTimeMillis())+"\n");*/

    }

    // Collect statistics about the messages received from coordinator
    public static void collectReceivedMessages(WorkerState workerState,KeyedCoProcessFunction<?,?,?,?>.Context ctx,MessageFGM message) throws IOException {

        /*ctx.output(worker_stats,"\nWorker "  + ctx.getCurrentKey()
                                    + " : receive a message "
                                    + " , type_message : " + message.getTypeMessage()
                                    + " , total_count : " + workerState.getTotalCount()
                                    + " , message_timestamp : " + new Timestamp(message.getTimestamp())
                                    + " at " + new Timestamp(System.currentTimeMillis()));*/
    }

    // Collect statistics about worker state
    public static void collectWorkerState(WorkerState workerState,KeyedCoProcessFunction<?,?,?,?>.Context ctx){
        // ctx.output(worker_stats, "\n"+"Worker " + ctx.getCurrentKey().toString()+" => "+workerState.toString());
    }

    // Collect worker statistics and write to side output
    public static <T> void collectStatistics(KeyedCoProcessFunction<?,?,?,?>.Context ctx,Counter counter,T input, boolean received) {

        // Assign a timer service
        TimerService timerService = ctx.timerService();

        // Current processing time
        long currentTimestamp = System.currentTimeMillis();

        StringBuilder str = new StringBuilder();
        str.append("Worker ").append(ctx.getCurrentKey().toString()).append(" => ");

        if(received){ str.insert(0,"\nReceived message , ").append(" , type_message : ").append(input); }

        ctx.output(worker_stats, str
                                 + " " + input.toString()
                                 + " " + counter.getRandomizedCounter()
                                 + " , ctx_timestamp : " + new Timestamp(ctx.timestamp() == null ? 0 : ctx.timestamp())
                                 + " , current_processing_time : " + new Timestamp(timerService.currentProcessingTime())
                                 + " , current watermark : " + new Timestamp(timerService.currentWatermark())
                                 + " at " + new Timestamp(currentTimestamp)
                                 + (received ? "\n" : ""));
    }

}
