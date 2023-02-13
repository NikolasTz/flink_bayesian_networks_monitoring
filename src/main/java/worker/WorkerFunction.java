package worker;

import datatypes.NodeBN_schema;
import datatypes.counter.Counter;
import datatypes.input.Input;
import datatypes.message.OptMessage;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import state.WorkerState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static config.InternalConfig.TypeCounter.*;
import static job.JobKafka.worker_stats;
import static state.State.hashKey;
import static utils.MathUtils.*;
import static utils.Util.*;
import static config.InternalConfig.TypeMessage;
import static config.InternalConfig.TypeNetwork.*;
import static config.InternalConfig.TypeMessage.*;


public class WorkerFunction {

    // Increment all counters
    public static void handleInput(WorkerState workerState, KeyedCoProcessFunction<?,?,?,?>.Context ctx, Input input, Collector<OptMessage> out) throws IOException {


        // ctx.output(worker_stats, "\nWorker " + ctx.getCurrentKey()+" input : "+input.getValue());

        // Increment the total count
        Counter totalCount = workerState.getTotalCount();
        totalCount.setCounter(totalCount.getCounter()+1L);
        workerState.setTotalCount(totalCount);

        // Increment the numbers of messages
        boolean incrementMessage;

        // Increment the counters
        if(workerState.getTypeNetwork() == NAIVE){ incrementMessage = incrementCountersNBC(input,workerState,ctx,out); }
        else{ incrementMessage = incrementCounters(input,workerState,ctx,out); }

        // Update the state
        // workerState.updateWorkerState();

        // Exact counters , one message size of numOfNodes per input
        if( workerState.getTypeCounter() == EXACT ){ workerState.setNumMessages(workerState.getNumMessages()+1L); }
        else if( incrementMessage ){
            // One messages( consists of merging the resulting increments for all counters into a single message ) per input
            workerState.setNumMessages(workerState.getNumMessages()+1L);
        }

        if( totalCount.getCounter() % 5000 == 0 ){ System.out.println("Worker "+ctx.getCurrentKey()+" processing 5000 tuples"); }

    }

    // Find and increment all the counters based on input for BNs
    public static boolean incrementCounters(Input input,
                                            WorkerState workerState,
                                            KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                            Collector<OptMessage> out) throws IOException {

        // Values of input
        List<String> inputValues = Arrays.asList(input.getValue().replaceAll(" ","").split(","));

        // Increment the numbers of messages
        boolean incrementMessage = false;

        // Number of messages of counter
        long numMessages;

        // For each of node of BN or NBC
        for(NodeBN_schema node : workerState.getNodes()){

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

                // Get the number of messages before the increment
                numMessages = counter.getNumMessages();

                // Increment the counter
                incrementCounter(counter,input,workerState,ctx,out);

                // If the number of messages was changed(only one increment is enough)
                if(numMessages != counter.getNumMessages() && !incrementMessage) incrementMessage = true;


                // Parent counter
                // Get the key of parent counter
                String parentKey = (nameParents.toString()+valuesParents+","+name).replace(" ","");

                // Hashing the key and get the parent-counter
                Counter parentCounter = workerState.getCounters().get(hashKey(parentKey.getBytes()));

                // Get the number of messages before the increment
                numMessages = counter.getNumMessages();

                // Increment the parent counter
                incrementCounter(parentCounter,input,workerState,ctx,out);

                // If the number of messages was changed(only one increment is enough)
                if(numMessages != counter.getNumMessages() && !incrementMessage) incrementMessage = true;

            }
            else{

                // Find and append the value from input using the schema of dataset
                sb.append(",").append(inputValues.get(workerState.getDatasetSchema().get(name)));

                // Get the key
                String key = sb.toString().replace(" ","");

                // Hashing the key and get the counter
                Counter counter = workerState.getCounters().get(hashKey(key.getBytes()));

                // Get the number of messages before the increment
                numMessages = counter.getNumMessages();

                // Increment the counter
                incrementCounter(counter,input,workerState,ctx,out);

                // If the number of messages was changed(only one increment is enough)
                if(numMessages != counter.getNumMessages() && !incrementMessage) incrementMessage = true;

            }

        }

        return incrementMessage;
    }


    // Find and increment all the counters based on input for BNs
    public static boolean incrementCountersNBC(Input input,
                                               WorkerState workerState,
                                               KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                               Collector<OptMessage> out) throws IOException {

        // Values of input
        List<String> inputValues = Arrays.asList(input.getValue().replaceAll(" ","").split(","));

        // Increment the numbers of messages
        boolean incrementMessage = false;

        // Number of messages of counter
        long numMessages;

        // Convention : The last node of NodeBN_schema array correspond to the class node and for class counter/parameter => nodeParents and nodeParentsValues = null
        // Class node
        NodeBN_schema classNode = workerState.getNodes()[(int) (workerState.getNumOfNodes()-1)];
        int classIndex = workerState.getDatasetSchema().get(classNode.getName());
        String classValue = inputValues.get(classIndex);
        String classKey = (classIndex+","+classValue).replace(" ","");
        Counter classCounter = workerState.getCounters().get(hashKey(classKey.getBytes()));

        // Get the number of messages before the increment
        numMessages = classCounter.getNumMessages();

        // Increment the counter
        incrementCounter(classCounter,input,workerState,ctx,out);

        // If the number of messages was changed(only one increment is enough)
        if(numMessages != classCounter.getNumMessages()) incrementMessage = true;


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

            // Get the number of messages before the increment
            numMessages = attrCounter.getNumMessages();

            // Increment the counter
            incrementCounter(attrCounter,input,workerState,ctx,out);

            // If the number of messages was changed(only one increment is enough)
            if(numMessages != attrCounter.getNumMessages() && !incrementMessage) incrementMessage = true;

        }

        return incrementMessage;
    }

    // Check for increment - opt_v1
    // Increment the counters
    /*for (Counter counter : workerState.getWorkerState().getCounters().values()) {
        if( checkIncrement(inputValues,counter,workerState.getDatasetSchema()) ){

            // Get the number of messages before the increment
            numMessages = counter.getNumMessages();

            // Increment the counter
            incrementCounter(counter,input,workerState,ctx,out);

            // If the number of messages was changed(only one increment is enough)
            if(numMessages != counter.getNumMessages() && !incrementMessage) incrementMessage = true;
        }
    }*/
    public static boolean checkIncrement(List<String> inputValues, Counter counter, List<String> datasetSchema){

        int index;

        // Check the node values
        ArrayList<String> counterNames = counter.getNodeName(); // Names of node
        ArrayList<String> counterValues = counter.getNodeValue(); // Values of node
        for (int i=0;i<counterNames.size();i++){

            index = findIndex(datasetSchema,counterNames.get(i));
            if(!counterValues.get(i).equals(inputValues.get(index))) return false; // Not increment

        }

        // Check the parents node values(if exists)
        if( counter.isActualNode() && counter.getNodeParents() != null ){

            ArrayList<String> counterParentsNames = counter.getNodeParents(); // Parents names of node
            ArrayList<String> counterParentsValues = counter.getNodeParentValues(); // Parents values of node

            for (int i=0;i<counterParentsNames.size();i++){
                index = findIndex(datasetSchema,counterParentsNames.get(i));
                if(!counterParentsValues.get(i).equals(inputValues.get(index))) return false; // Not Increment
            }
        }

        return true;
    }

    // Increment the counter
    public static void incrementCounter(Counter counter,
                                        Input input,
                                        WorkerState workerState,
                                        KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                        Collector<OptMessage> out){

        // Check the type of counter
        if( workerState.getTypeCounter() == RANDOMIZED ){ incrementRandomizedCounter(counter,input,workerState,ctx,out); }
        else if( workerState.getTypeCounter() == EXACT ){ incrementExactCounter(counter,input,ctx,out); }
        else if( workerState.getTypeCounter() == CONTINUOUS ){ incrementContinuousCounter(counter,input,ctx,out); }
        else{ incrementDeterministicCounter(counter,input,workerState,ctx,out); } // DETERMINISTIC counter

    }


    // *************************************************************************
    //                              Randomized Counter
    // *************************************************************************

    // Randomized counter
    /*public static void incrementRandomizedCounter(Counter counter,
                                                  Input input,
                                                  WorkerState workerState,
                                                  KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                                  Collector<OptMessage> out){

        // Update the value of counter
        counter.setCounter(counter.getCounter()+1L);

        // When counter increment by one then send message with probability prob to coordinator
        long setupCounter = (workerState.getSetupCounter() == SetupCounter.ZERO) ? 0L : findValueProbHalved(counter.getEpsilon(),workerState.getNumOfWorkers(),counter.getResConstant())/workerState.getNumOfWorkers();
        if( counter.getCounter() > setupCounter && counter.getProb() > new Random().nextDouble() ){

            // Send the message to coordinator
            out.collect(createOptMessage(ctx.getCurrentKey().toString(),counter,INCREMENT,System.currentTimeMillis()));
            // out.collect(createOptMessage(ctx.getCurrentKey().toString(),counter,INCREMENT,input.getTimestamp()));
            // out.collect(createMessage(ctx.getCurrentKey().toString(),counter,INCREMENT,ctx.timestamp())); ???

            counter.setLastSentValue(counter.getCounter()); // Update the last sent value
            counter.setNumMessages(counter.getNumMessages()+1L);  // Update the number of messages
            counter.setLastTs(System.currentTimeMillis());  // Update the last timestamp

            // Collect some statistics
            collectMessages(ctx,counter,"",INCREMENT);
        }

        // Whenever counter doubles then send an additional message to coordinator
        double avoid_zero = counter.getCounterDoubles() == 0 ? 1d : counter.getCounterDoubles();
        if( (counter.getCounterDoubles() == 0 && counter.getCounter() == 2) || (counter.getCounter()/avoid_zero >= 2) ){

            // Send the message to coordinator
            out.collect(createOptMessage(ctx.getCurrentKey().toString(),counter,DOUBLES,System.currentTimeMillis()));
            // out.collect(createOptMessage(ctx.getCurrentKey().toString(),counter,DOUBLES,input.getTimestamp()));
            // out.collect(createMessage(ctx.getCurrentKey().toString(),counter,DOUBLES,ctx.timestamp())); ???

            counter.setCounterDoubles(counter.getCounter()); // Update the counter_doubles
            counter.setLastSentValue(counter.getCounter());  // Update the last sent value
            counter.setNumMessages(counter.getNumMessages()+1L); // Update the number of messages
            counter.setLastTs(System.currentTimeMillis());  // Update the last timestamp

            // Collect some statistics
            collectMessages(ctx,counter,"",DOUBLES);
        }

        // Collect statistics
        collectStatistics(ctx,counter,input,false);

    }*/
    public static void incrementRandomizedCounter(Counter counter,
                                                  Input input,
                                                  WorkerState workerState,
                                                  KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                                  Collector<OptMessage> out){

        // Update the value of counter
        counter.setCounter(counter.getCounter()+1L);

        // Counter must be synchronized
        if(counter.isSync()) {

            // When counter increment by one then send message with probability prob to coordinator
            // long setupCounter = (workerState.getSetupCounter() == SetupCounter.ZERO) ? 0L : findValueProbHalved(counter.getEpsilon(), workerState.getNumOfWorkers(), counter.getResConstant()) / workerState.getNumOfWorkers();
            // if (counter.getCounter() > setupCounter && counter.getProb() > new Random().nextDouble())
            if (counter.getProb() > new Random().nextDouble()) {

                // Send the message to coordinator
                out.collect(createOptMessage(ctx.getCurrentKey().toString(), counter, INCREMENT, System.currentTimeMillis()));
                // out.collect(createOptMessage(ctx.getCurrentKey().toString(),counter,INCREMENT,input.getTimestamp()));
                // out.collect(createMessage(ctx.getCurrentKey().toString(),counter,INCREMENT,ctx.timestamp())); ???

                counter.setLastSentValue(counter.getCounter()); // Update the last sent value
                counter.setNumMessages(counter.getNumMessages() + 1L);  // Update the number of messages
                counter.setLastTs(System.currentTimeMillis());  // Update the last timestamp

                // Collect some statistics
                // collectMessages(ctx, counter, "", INCREMENT);
            }

            // Whenever counter doubles then send an additional message to coordinator
            double avoid_zero = counter.getCounterDoubles() == 0 ? 1d : counter.getCounterDoubles();
            if ((counter.getCounterDoubles() == 0 && counter.getCounter() == 2) || (counter.getCounter() / avoid_zero >= 2)) {

                // Send the message to coordinator
                out.collect(createOptMessage(ctx.getCurrentKey().toString(), counter, DOUBLES, System.currentTimeMillis()));
                // out.collect(createOptMessage(ctx.getCurrentKey().toString(),counter,DOUBLES,input.getTimestamp()));
                // out.collect(createMessage(ctx.getCurrentKey().toString(),counter,DOUBLES,ctx.timestamp())); ???

                counter.setCounterDoubles(counter.getCounter()); // Update the counter_doubles
                counter.setLastSentValue(counter.getCounter());  // Update the last sent value
                counter.setNumMessages(counter.getNumMessages() + 1L); // Update the number of messages
                counter.setLastTs(System.currentTimeMillis());  // Update the last timestamp

                // Collect some statistics
                // collectMessages(ctx, counter, "", DOUBLES);
            }

        }

        // Collect statistics
        // collectStatistics(ctx,counter,input,false);

    }


    // *************************************************************************
    //                                 Exact Counter
    // *************************************************************************

    // Exact counter
    public static void incrementExactCounter(Counter counter,
                                             Input input,
                                             KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                             Collector<OptMessage> out){

        // Increment the counter
        counter.setCounter(counter.getCounter()+1L);

        // Send message to coordinator
        out.collect(createOptMessage(ctx.getCurrentKey().toString(),counter,INCREMENT,System.currentTimeMillis()));
        // out.collect(createOptMessage(ctx.getCurrentKey().toString(),counter,INCREMENT,input.getTimestamp()));
        // out.collect(createMessage(ctx.getCurrentKey().toString(),counter,INCREMENT,ctx.timestamp()));

        // Update the number of messages
        counter.setNumMessages(counter.getNumMessages()+1L);

        // Update the last timestamp
        counter.setLastTs(System.currentTimeMillis());

        // Collect some statistics
        // collectStatistics(ctx,counter,input,false);

    }


    // *************************************************************************
    //                              Continuous Counter
    // *************************************************************************

    // Continuous counter
    public static void incrementContinuousCounter(Counter counter,
                                                  Input input,
                                                  KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                                  Collector<OptMessage> out) {

        // Increment the counter
        counter.setCounter(counter.getCounter()+1L);

        // Check the condition counter > (1+epsilon)lastSentValue
        if( counter.getCounter() > (1+counter.getEpsilon())*counter.getLastSentValue() ){

            // Send message to coordinator
            out.collect(createOptMessage(ctx.getCurrentKey().toString(),counter,INCREMENT,System.currentTimeMillis()));
            // out.collect(createOptMessage(ctx.getCurrentKey().toString(),counter,INCREMENT,input.getTimestamp()));
            // out.collect(createMessage(ctx.getCurrentKey().toString(),counter,INCREMENT,ctx.timestamp())); ???

            counter.setLastSentValue(counter.getCounter()); // Update the last send value
            counter.setNumMessages(counter.getNumMessages()+1L);  // Update the number of messages
            counter.setLastTs(System.currentTimeMillis());  // Update the last timestamp

            // Collect some statistics
            // collectMessages(ctx,counter,"",INCREMENT);
        }

        // Collect some statistics
        // collectStatistics(ctx,counter,input,false);
    }


    // *************************************************************************
    //                            Deterministic Counter
    // *************************************************************************

    // Deterministic counter
    public static void incrementDeterministicCounter(Counter counter,
                                                     Input input,
                                                     WorkerState workerState,
                                                     KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                                     Collector<OptMessage> out){

        // Update the value of counter. Act as total count
        counter.setCounter(counter.getCounter()+1L);

        // Update the local drift
        counter.setLastSentValue(counter.getLastSentValue()+1L);

        // if counter is synchronized(counter.isSync()) and counter >= ((epsilon*condition)/workers)
        if( counter.isSync() && counter.getLastSentValue() > calculateCondition(counter.getEpsilon(),counter.getLastBroadcastValue(),workerState.getNumOfWorkers()) ){

            // Update the timestamp
            counter.setLastTs(System.currentTimeMillis());

            // Send the message to coordinator(worker,local increment-drift)
            OptMessage message = new OptMessage(ctx.getCurrentKey().toString(),counter,INCREMENT,counter.getLastSentValue(),counter.getLastBroadcastValue());
            message.setTimestamp(System.currentTimeMillis()); // Update the timestamp
            // message.setTimestamp(input.getTimestamp()); // Update the timestamp or message.setTimestamp(ctx.timestamp()); ???
            out.collect(message);

            // Reset the local drift of counter
            counter.setLastSentValue(0L);

            // Update the number of messages
            counter.setNumMessages(counter.getNumMessages()+1L);

            // Collect some statistics about messages
            // collectMessages(ctx,counter,"",INCREMENT);

        }

        // Collect some statistics
        // collectStatistics(ctx,counter,input,false);
    }


    // *************************************************************************
    //                             Utils-Statistics
    // *************************************************************************

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

    // Create the message that will be sent , using the optimized version of Message class
    public static OptMessage createOptMessage(String key, Counter counter, TypeMessage typeMessage, long timestamp){
        OptMessage optMessage = new OptMessage(key,counter,typeMessage,counter.getCounter(),counter.getLastBroadcastValue());
        optMessage.setTimestamp(timestamp); // Update the timestamp
        return optMessage;
    }

    // Collect statistics about messages and write to side output
    public static void collectMessages(KeyedCoProcessFunction<?,?,?,?>.Context ctx,Counter counter,String probability, TypeMessage typeMessage){

        /*ctx.output(worker_stats, "\nWorker " + ctx.getCurrentKey()
                                    + probability
                                    + " , send a message to coordinator"
                                    + " , type_message : " + typeMessage
                                    + " " + counter.getRandomizedCounter()
                                    + " at " + new Timestamp(System.currentTimeMillis())+"\n");*/

    }

    // Collect worker statistics and write to side output
    public static <T> void collectStatistics(KeyedCoProcessFunction<?,?,?,?>.Context ctx,Counter counter,T input, boolean received) {

        // Assign a timer service
        // TimerService timerService = ctx.timerService();

        // Current processing time
        // long currentTimestamp = System.currentTimeMillis();

        // StringBuilder str = new StringBuilder();
        // str.append("Worker ").append(ctx.getCurrentKey().toString()).append(" => ");

        // if(received){ str.insert(0,"\nReceived message , ").append(" , type_message : ").append(input); }

        /* ctx.output(worker_stats, str
                                    // + " " + input.toString()
                                    + " " + counter.getRandomizedCounter()
                                    // + " , ctx_timestamp : " + new Timestamp(ctx.timestamp() == null ? 0 : ctx.timestamp())
                                    // + " , current_processing_time : " + new Timestamp(timerService.currentProcessingTime())
                                    // + " , current watermark : " + new Timestamp(timerService.currentWatermark())
                                    + " at " + new Timestamp(currentTimestamp)
                                    + (received ? "\n" : "")); */
    }


    // *************************************************************************
    //                               Handle Messages
    // *************************************************************************

    // Handle optimized message from coordinator
    public static void handleMessage(WorkerState workerState,
                                     OptMessage message,
                                     KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                     Collector<OptMessage> out) throws IOException {

        // Find the counter
        Counter counter = workerState.getCounters().get(message.getKeyMessage());

        // Check the type of message
        if( message.getTypeMessage() == UPDATE ){ handleUpdate(workerState,counter,message,ctx,out); }
        else if( message.getTypeMessage() == DRIFT ){ handleDrift(counter,message,ctx,out); }
        else if( message.getTypeMessage() == COUNTER ){ handleRandomizedCounter(workerState,counter,message,ctx,out); }

        // Update the state
        // workerState.updateWorkerState();

    }

    // Handle update optimized message from coordinator
    public static void handleUpdate(WorkerState workerState,
                                    Counter counter,
                                    OptMessage message,
                                    KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                    Collector<OptMessage> out) throws IOException {

        // Check the type of counter
        if( workerState.getTypeCounter() == RANDOMIZED ){ handleRandomizedUpdate(workerState,counter,message,ctx,out); }
        else{ handleDeterministicUpdate(workerState,counter,message,ctx,out); } // DETERMINISTIC counter
    }


    // *************************************************************************
    //                           Messages-Randomized Counter
    // *************************************************************************

    // Handle update optimized message from coordinator , refer to randomized counter
    public static void handleRandomizedUpdate(WorkerState workerState,
                                              Counter counter,
                                              OptMessage message,
                                              KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                              Collector<OptMessage> out) throws IOException {


        // Update message from coordinator
        // Update the last broadcast value , means that the last broadcast value changed by factor between 2 and 4

        // New round begins
        counter.setSync(true);

        // Update last broadcast value
        counter.setLastBroadcastValue(message.getLastBroadcastValue());

        // Update the probability
        if( counter.getLastBroadcastValue() > calculate(workerState.getNumOfWorkers(),counter.getEpsilon(),counter.getResConstant()) ) {

            // Find the next power of 2 that is smaller or equal than eps*n_hat/sqrt(workers)*resConstant
            double power = highestPowerOf2(counter.getEpsilon(),counter.getLastBroadcastValue(),workerState.getNumOfWorkers(),counter.getResConstant());

            if(power <= 1d) power = 1d;

            // Calculate the new probability
            double new_prob = 1.0 / power;

            // If probability halved then send message to coordinator to adjust the ni(counter)
            if (counter.getProb()/new_prob >= 2) {

                // Adjustment
                counter.setProb(new_prob);  // Update the new probability

                // With probability = 1/2 send the counter-ni to coordinator
                if (new Random().nextDouble() < 0.5) {

                    // Send an increment message to coordinator
                    // Initialize the message
                    message.setTypeMessage(INCREMENT);
                    message.setValue(counter.getCounter());
                    message.setLastBroadcastValue(counter.getLastBroadcastValue());
                    message.setTimestamp(System.currentTimeMillis()); // or message.setTimestamp(ctx.timestamp())

                    // Send the messages
                    out.collect(message);

                    counter.setLastSentValue(counter.getCounter()); // Update the last sent value
                    counter.setLastTs(System.currentTimeMillis()); // Update the timestamp
                    // collectMessages(ctx,counter," , probability halved , no losing flip ",INCREMENT); // Collect some statistics

                }
                // If we loose the flip
                else {

                    // Flip a coin with new probability until we succeed and count the number of failures
                    long num_failures = 0L;
                    Random rand = new Random();

                    while (rand.nextDouble() > counter.getProb()) {
                        // Increment the number of failures
                        num_failures += 1;
                    }

                    // At least one flip
                    if (num_failures == 0) num_failures = 1;

                    // Send an increment message to coordinator where adjusted_counter = counter_value-num_failures
                    long adjusted = (counter.getCounter() - num_failures) < 0 ? 0L : (counter.getCounter() - num_failures);

                    // Initialize the message
                    message.setTypeMessage(INCREMENT);
                    message.setValue(adjusted);
                    message.setLastBroadcastValue(counter.getLastBroadcastValue());
                    message.setTimestamp(System.currentTimeMillis()); // or  message.setTimestamp(ctx.timestamp()) ???

                    // Send the message
                    out.collect(message);

                    counter.setLastSentValue(adjusted); // Update the last sent value
                    counter.setLastTs(System.currentTimeMillis()); // Update the timestamp
                    // collectMessages(ctx,counter," , probability halved , losing flip , num_failures : "+ num_failures,INCREMENT); // Collect some statistics

                }

                counter.setNumMessages(counter.getNumMessages()+1L); // Update the number of messages
                workerState.setNumMessages(workerState.getNumMessages()+1L); // Update the total messages
            }
            else {
                // Update and refine the new probability
                counter.setProb(new_prob);
                if( (counter.getCounter() - counter.getLastSentValue()) > 1L/counter.getProb() ){

                    // Send the message to coordinator
                    out.collect(createOptMessage(ctx.getCurrentKey().toString(), counter, INCREMENT, System.currentTimeMillis()));

                    counter.setLastSentValue(counter.getCounter()); // Update the last sent value
                    counter.setNumMessages(counter.getNumMessages() + 1L);  // Update the number of messages
                    counter.setLastTs(System.currentTimeMillis());  // Update the last timestamp

                    // Collect some statistics
                    // collectMessages(ctx, counter, "", INCREMENT);
                }
            }

        }

        // Collect some statistics
        // collectStatistics(ctx,counter,message,true);
    }

    // Handle counter optimized message from coordinator , refer only to randomized counter
    public static void handleRandomizedCounter(WorkerState workerState,
                                               Counter counter,
                                               OptMessage message,
                                               KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                               Collector<OptMessage> out){

        // Send the local counter to the coordinator
        OptMessage counterMessage = new OptMessage(ctx.getCurrentKey().toString(),counter,COUNTER,counter.getCounter(),counter.getLastBroadcastValue());
        counterMessage.setTimestamp(System.currentTimeMillis()); // Update the timestamp or driftMessage.setTimestamp(ctx.timestamp()) ???
        out.collect(counterMessage);

        // Update the number of messages
        counter.setNumMessages(counter.getNumMessages()+1L);

        // Update sync
        counter.setSync(false);

        // Update the timestamp
        counter.setLastTs(System.currentTimeMillis());

        // Collect some statistics
        // collectStatistics(ctx,counter,message,true);


    }


    // *************************************************************************
    //                          Messages-Deterministic Counter
    // *************************************************************************

    // Handle update optimized message from coordinator , refer to deterministic counter
    public static void handleDeterministicUpdate(WorkerState workerState,
                                                 Counter counter,
                                                 OptMessage message,
                                                 KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                                 Collector<OptMessage> out) {

        // New round begins
        counter.setSync(true);

        // Update condition
        counter.setLastBroadcastValue(message.getLastBroadcastValue());

        // Subroutine of increment
        // subroutineIncrementDC(counter,workerState,ctx,out);

        // Collect some statistics
        // collectStatistics(ctx,counter,message,true);

    }

    // Handle drift optimized messages from coordinator , refer to deterministic counter
    public static void handleDrift(Counter counter,
                                   OptMessage message,
                                   KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                   Collector<OptMessage> out) {


        // Send the local drift of counter to coordinator
        OptMessage driftMessage = new OptMessage(ctx.getCurrentKey().toString(),counter,DRIFT,counter.getLastSentValue(),counter.getLastBroadcastValue());
        driftMessage.setTimestamp(System.currentTimeMillis()); // Update the timestamp or driftMessage.setTimestamp(ctx.timestamp()) ???
        out.collect(driftMessage);

        // Update the number of messages
        counter.setNumMessages(counter.getNumMessages()+1L);

        // Update sync
        counter.setSync(false);

        // Reset the local drift
        counter.setLastSentValue(0L);

        // Update the timestamp
        counter.setLastTs(System.currentTimeMillis());

        // Collect some statistics
        // collectStatistics(ctx,counter,message,true);

    }


    // *************************************************************************
    //                              Subroutine processes
    // *************************************************************************

    // Subroutine process for RC and DC counter
    public static void subroutineProcess(WorkerState workerState,
                                         KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                         Collector<OptMessage> out) throws IOException {

        if( workerState.getTypeCounter() == RANDOMIZED ){
            for(Counter counter : workerState.getWorkerState().getCounters().values()){
                subroutineRandomizedCounter(counter,workerState,ctx,out);
                // workerState.updateWorkerState();
            }
        }
        else if( workerState.getTypeCounter() == DETERMINISTIC ){
            for(Counter counter : workerState.getWorkerState().getCounters().values()){
                subroutineDeterministicCounter(counter,workerState,ctx,out);
                // workerState.updateWorkerState();
            }
        }

    }

    // Subroutine process of randomized counter(RC)
    public static void subroutineRandomizedCounter(Counter counter,
                                                   WorkerState workerState,
                                                   KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                                   Collector<OptMessage> out){
        // Counter synchronized
        if(counter.isSync()) {

            // When counter increment by one then send message with probability prob to coordinator
            // long setupCounter = (workerState.getSetupCounter() == SetupCounter.ZERO) ? 0L : findValueProbHalved(counter.getEpsilon(), workerState.getNumOfWorkers(), counter.getResConstant()) / workerState.getNumOfWorkers();
            if ( counter.getProb() > new Random().nextDouble() ) {

                // Send the message to coordinator
                out.collect(createOptMessage(ctx.getCurrentKey().toString(), counter, INCREMENT, System.currentTimeMillis()));
                // out.collect(createOptMessage(ctx.getCurrentKey().toString(),counter,INCREMENT,input.getTimestamp()));
                // out.collect(createMessage(ctx.getCurrentKey().toString(),counter,INCREMENT,ctx.timestamp())); ???

                counter.setLastSentValue(counter.getCounter()); // Update the last sent value
                counter.setNumMessages(counter.getNumMessages() + 1L);  // Update the number of messages
                counter.setLastTs(System.currentTimeMillis());  // Update the last timestamp

                // Collect some statistics
                // collectMessages(ctx, counter, "", INCREMENT);
            }
            /*else{
                if( (counter.getCounter() - counter.getLastSentValue()) > 1L/counter.getProb() ){

                    // Send the message to coordinator
                    out.collect(createOptMessage(ctx.getCurrentKey().toString(), counter, INCREMENT, System.currentTimeMillis()));
                    // out.collect(createOptMessage(ctx.getCurrentKey().toString(),counter,INCREMENT,input.getTimestamp()));
                    // out.collect(createMessage(ctx.getCurrentKey().toString(),counter,INCREMENT,ctx.timestamp())); ???

                    counter.setLastSentValue(counter.getCounter()); // Update the last sent value
                    counter.setNumMessages(counter.getNumMessages() + 1L);  // Update the number of messages
                    counter.setLastTs(System.currentTimeMillis());  // Update the last timestamp

                    // Collect some statistics
                    collectMessages(ctx, counter, "", INCREMENT);
                }
            }*/

            // Whenever counter doubles then send an additional message to coordinator
            double avoid_zero = counter.getCounterDoubles() == 0 ? 1d : counter.getCounterDoubles();
            if ((counter.getCounterDoubles() == 0 && counter.getCounter() == 2) || (counter.getCounter() / avoid_zero >= 2)) {

                // Send the message to coordinator
                out.collect(createOptMessage(ctx.getCurrentKey().toString(), counter, DOUBLES, System.currentTimeMillis()));
                // out.collect(createOptMessage(ctx.getCurrentKey().toString(),counter,DOUBLES,input.getTimestamp()));
                // out.collect(createMessage(ctx.getCurrentKey().toString(),counter,DOUBLES,ctx.timestamp())); ???

                counter.setCounterDoubles(counter.getCounter()); // Update the counter_doubles
                counter.setLastSentValue(counter.getCounter());  // Update the last sent value
                counter.setNumMessages(counter.getNumMessages() + 1L); // Update the number of messages
                counter.setLastTs(System.currentTimeMillis());  // Update the last timestamp

                // Collect some statistics
                // collectMessages(ctx, counter, "", DOUBLES);
            }

        }

        // Collect statistics
        // collectStatistics(ctx,counter,input,false);

    }

    // Subroutine process of deterministic counter(DC)
    public static void subroutineDeterministicCounter(Counter counter,
                                                      WorkerState workerState,
                                                      KeyedCoProcessFunction<?,?,?,?>.Context ctx,
                                                      Collector<OptMessage> out){

        // if counter is synchronized(counter.isSync()) and counter >= ((epsilon*condition)/workers)
        if( counter.isSync() && counter.getLastSentValue() > calculateCondition(counter.getEpsilon(),counter.getLastBroadcastValue(),workerState.getNumOfWorkers()) ){

            // Update the timestamp
            counter.setLastTs(System.currentTimeMillis());

            // Send the message to coordinator(worker,local increment-drift)
            OptMessage message = new OptMessage(ctx.getCurrentKey().toString(),counter,INCREMENT,counter.getLastSentValue(),counter.getLastBroadcastValue());
            message.setTimestamp(System.currentTimeMillis()); // Update the timestamp or message.setTimestamp(ctx.timestamp()); ???
            out.collect(message);

            // Reset the local drift of counter
            counter.setLastSentValue(0L);

            // Update the number of messages
            counter.setNumMessages(counter.getNumMessages()+1L);

            // Collect some statistics about messages
            // collectMessages(ctx,counter,"",INCREMENT);

        }

        // Collect some statistics
        // collectStatistics(ctx,counter,input,false);
    }

}
