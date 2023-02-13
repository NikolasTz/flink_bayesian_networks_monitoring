package coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import config.CBNConfig;
import datatypes.NodeBN_schema;
import datatypes.counter.Counter;
import datatypes.input.Input;
import config.InternalConfig.TypeMessage;
import datatypes.message.OptMessage;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import state.CoordinatorState;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static datatypes.NodeBN_schema.findIndexNode;
import static datatypes.counter.Counter.findCardinality;
import static config.InternalConfig.TypeMessage.*;
import static job.JobKafka.coordinator_stats;
import static operators.Source.QuerySource.findProbability;
import static state.State.hashKey;
import static utils.MathUtils.*;
import static utils.Util.*;
import static config.InternalConfig.TypeCounter.*;

public class CoordinatorFunction {

    // Handle increment message
    public static void handleIncrement(CoordinatorState coordinatorState,
                                       CoProcessFunction<?,?,?>.Context ctx,
                                       OptMessage message,
                                       Collector<OptMessage> out) throws Exception {

        if( coordinatorState.getTypeCounter() == RANDOMIZED ){ handleRandomizedIncrement(coordinatorState,ctx,out,message); }
        else if( coordinatorState.getTypeCounter() == EXACT ){ handleExactIncrement(coordinatorState,ctx,message); }
        else if( coordinatorState.getTypeCounter() == CONTINUOUS ){ handleContinuousIncrement(coordinatorState,ctx,message); }
        else{ handleDeterministicIncrement(coordinatorState,ctx,message,out); }

    }


    // *************************************************************************
    //                              Randomized Counter
    // *************************************************************************

    // INCREMENT message
    /* public static void handleRandomizedIncrement(CoordinatorState coordinatorState,
                                                 CoProcessFunction<?,?,?>.Context ctx,
                                                 Collector<OptMessage> out,
                                                 OptMessage message) throws Exception {

        // Get the counter
        Counter counter = coordinatorState.getCounters().get(message.getKeyMessage());

        // Update the last sent value from worker
        counter.setLastSentValues(message.getWorker(),message.getValue());

        // Update the timestamp
        counter.setLastTs(message.getTimestamp());

        // Update the overhead of messages
        if( counter.getLastBroadcastValue() != message.getLastBroadcastValue() && counter.getProb() <= 1 ){
            coordinatorState.setOverheadMessages(coordinatorState.getOverheadMessages()+1L);
        }

        // Call subroutine doubles
        subRoutineDoubles(coordinatorState,ctx,counter,out);

        // Collect some statistics
        collectStatisticsCord(ctx,message,counter);

        // Update the coordinator state
        // coordinatorState.updateCoordinatorState();
    }*/
    public static void handleRandomizedIncrement(CoordinatorState coordinatorState,
                                                 CoProcessFunction<?,?,?>.Context ctx,
                                                 Collector<OptMessage> out,
                                                 OptMessage message) throws Exception {

        // Get the counter
        Counter counter = coordinatorState.getCounters().get(message.getKeyMessage());

        // Update the last sent value from worker
        counter.setLastSentValues(message.getWorker(),message.getValue());

        // Update the timestamp
        counter.setLastTs(message.getTimestamp());

        // Update the overhead of messages
        if( counter.getLastBroadcastValue() != message.getLastBroadcastValue() && counter.getProb() <= 1 ){
            coordinatorState.setOverheadMessages(coordinatorState.getOverheadMessages()+1L);
        }

        // Call subroutine doubles
        if(counter.isSync()) subRoutineDoubles(coordinatorState,ctx,counter,out);

        // Collect some statistics
        // collectStatisticsCord(ctx,message,counter);

        // Update the coordinator state
        // coordinatorState.updateCoordinatorState();
    }

    // DOUBLES message subroutine
    /*public static void subRoutineDoubles(CoordinatorState coordinatorState,
                                         CoProcessFunction<?,?,?>.Context ctx,
                                         Counter counter,
                                         Collector<OptMessage> out) throws Exception {

        // Calculate the sum of last sent values
        long total_sent_values = sumOfLastSentValues(counter);

        // When the n' is changed by a factor between two and four then new round begins
        double avoid_zero = counter.getLastBroadcastValue() == 0 ? 1d : counter.getLastBroadcastValue();
        if ( (avoid_zero == 1.0 && total_sent_values > 2) || (total_sent_values / avoid_zero) >= 2 ) {

            if ( total_sent_values > 2*calculate(coordinatorState.getNumOfWorkers(), counter.getEpsilon(), counter.getResConstant()) ) {

                // New round begins

                // Update the last broadcast value(n_dash)
                counter.setLastBroadcastValue(total_sent_values);

                // Send message to workers
                broadcastUpdate(coordinatorState, counter, out);

                // Update the number of messages sent from coordinator
                coordinatorState.setNumMessagesSent(coordinatorState.getNumMessagesSent() + coordinatorState.getNumOfWorkers());

                // Update the probability( last broadcast value => n_hat )
                // Find the next power of 2 that is smaller than epsilon*n_hat/sqrt(workers)
                double power = highestPowerOf2(counter.getEpsilon(), total_sent_values, coordinatorState.getNumOfWorkers(), counter.getResConstant());
                if (power <= 1d) power = 1d;

                // Update the new probability
                double new_prob = 1.0 / power;
                counter.setProb(new_prob);

                // Update the number of round
                updateNumOfRound(counter);

                // Collect some statistics about messages
                // collectMessagesCord(ctx,UPDATE,message,counter);
            }
        }

        // Update the coordinator state
        // coordinatorState.updateCoordinatorState();
    }*/
    public static void subRoutineDoubles(CoordinatorState coordinatorState,
                                         CoProcessFunction<?,?,?>.Context ctx,
                                         Counter counter,
                                         Collector<OptMessage> out) throws Exception {

        // Calculate the sum of last sent values
        long total_sent_values = sumOfLastSentValues(counter);

        // When the n' is changed by a factor between two and four then new round begins
        double avoid_zero = counter.getLastBroadcastValue() == 0 ? 1d : counter.getLastBroadcastValue();
        if ( (avoid_zero == 1.0 && total_sent_values > 2) || (total_sent_values / avoid_zero) >= 2 ) {

            // if ( total_sent_values > 2*calculate(coordinatorState.getNumOfWorkers(), counter.getEpsilon(), counter.getResConstant()) ) {

                // Broadcast COUNTER message for all workers to receive the counters
                broadcastCounter(coordinatorState, counter, out);

                // Update the number of messages sent from coordinator
                coordinatorState.setNumMessagesSent(coordinatorState.getNumMessagesSent() + coordinatorState.getNumOfWorkers());

                // Update the sync and wait for local counter
                counter.setSync(false);

                // Reset the temporary counter of COUNTER messages
                counter.setCounter(0L);

                // Collect some statistics about messages
                // collectMessagesCord(ctx,UPDATE,message,counter);
            // }
        }

        // Update the coordinator state
        // coordinatorState.updateCoordinatorState();
    }

    // DOUBLES message
    /*public static void handleDoubles(CoordinatorState coordinatorState,
                                     CoProcessFunction<?,?,?>.Context ctx,
                                     OptMessage message,
                                     Collector<OptMessage> out) throws Exception {

        // Get the counter
        Counter counter = coordinatorState.getCounters().get(message.getKeyMessage());

        // Update the timestamp
        counter.setLastTs(message.getTimestamp());

        // Update the ni'
        counter.setLastSentUpdates(message.getWorker(), message.getValue());

        // Calculate the n' => sum{ni'} at given time
        long total_updates = sumOfUpdates(counter);

        // Update the last sent value
        counter.setLastSentValues(message.getWorker(), message.getValue());

        // When the n' is changed by a factor between two and four then new round begins
        double avoid_zero = counter.getLastBroadcastValue() == 0 ? 1d : counter.getLastBroadcastValue();
        if ( (avoid_zero == 1.0 && total_updates > 2) || (total_updates / avoid_zero) >= 2 ) {

            // New round begins

            // Update the last broadcast value(n_dash)
            counter.setLastBroadcastValue(total_updates);

            // Send n_dash after the probability changed from 1
            if ( total_updates > 2*calculate(coordinatorState.getNumOfWorkers(), counter.getEpsilon(), counter.getResConstant()) ){

                // Send message to workers
                broadcastUpdate(coordinatorState,counter,out);

                // Update the number of messages sent from coordinator
                coordinatorState.setNumMessagesSent(coordinatorState.getNumMessagesSent() + coordinatorState.getNumOfWorkers());

                // Update the probability( last broadcast value => n_hat )
                // Find the next power of 2 that is smaller than epsilon*n_hat/sqrt(workers)
                double power = highestPowerOf2(counter.getEpsilon(),total_updates,coordinatorState.getNumOfWorkers(),counter.getResConstant());
                if (power <= 1d) power = 1d;

                // Update the new probability
                double new_prob = 1.0 / power;
                counter.setProb(new_prob);

            }

            // Update the number of round
            updateNumOfRound(counter);

            // Collect some statistics about messages
            collectMessagesCord(ctx,UPDATE,message,counter);

        }

        // Update the coordinator state
        // coordinatorState.updateCoordinatorState();

    }*/
    public static void handleDoubles(CoordinatorState coordinatorState,
                                     CoProcessFunction<?,?,?>.Context ctx,
                                     OptMessage message,
                                     Collector<OptMessage> out) throws Exception {

        // Get the counter
        Counter counter = coordinatorState.getCounters().get(message.getKeyMessage());

        // Update the overhead of messages
        if( counter.getLastBroadcastValue() != message.getLastBroadcastValue() && counter.getProb() <= 1 ){
            coordinatorState.setOverheadMessages(coordinatorState.getOverheadMessages()+1L);
        }

        // Update the timestamp
        counter.setLastTs(message.getTimestamp());

        // Update the ni'
        counter.setLastSentUpdates(message.getWorker(), message.getValue());

        // Update the last sent value
        counter.setLastSentValues(message.getWorker(), message.getValue());

        // Counter must be synchronized
        if(counter.isSync()) {

            // Calculate the n' => sum{ni'} at given time
            long total_updates = sumOfUpdates(counter);

            // Calculate the sum of last sent values
            // long total_sent_values = sumOfLastSentValues(counter);

            // When the n' is changed by a factor between two and four then new round begins
            double avoid_zero = counter.getLastBroadcastValue() == 0 ? 1d : counter.getLastBroadcastValue();
            if ( (avoid_zero == 1.0 && total_updates > 2) || (total_updates / avoid_zero) >= 2 ) {

                // Send n_dash after the probability changed from 1
                //if ( total_updates > 2*calculate(coordinatorState.getNumOfWorkers(), counter.getEpsilon(), counter.getResConstant()) ) {

                    // Broadcast COUNTER message for all workers to receive the counters
                    broadcastCounter(coordinatorState, counter, out);

                    // Update the number of messages sent from coordinator
                    coordinatorState.setNumMessagesSent(coordinatorState.getNumMessagesSent() + coordinatorState.getNumOfWorkers());

                    // Update the sync and wait for local counter
                    counter.setSync(false);

                    // Reset the temporary counter of COUNTER messages
                    counter.setCounter(0L);
                //}

            }
        }

        // Update the coordinator state
        // coordinatorState.updateCoordinatorState();

    }

    // COUNTER message
    public static void handleCounter(CoordinatorState coordinatorState,
                                     CoProcessFunction<?,?,?>.Context ctx,
                                     OptMessage message,
                                     Collector<OptMessage> out) throws Exception {

        // Get the counter
        Counter counter = coordinatorState.getCounters().get(message.getKeyMessage());

        // Update the overhead of messages
        if( counter.getLastBroadcastValue() != message.getLastBroadcastValue() && counter.getProb() <= 1 ){
            coordinatorState.setOverheadMessages(coordinatorState.getOverheadMessages()+1L);
        }

        // Update the timestamp
        counter.setLastTs(message.getTimestamp());

        // Update the last sent value from worker
        counter.setLastSentValues(message.getWorker(),message.getValue());

        // Update the ni'
        counter.setLastSentUpdates(message.getWorker(), message.getValue());

        // Update the temporary counter of COUNTER messages
        counter.setCounter(counter.getCounter()+1L);

        // Check the condition
        if(counter.getCounter() == coordinatorState.getNumOfWorkers()){

            // New round begins

            // Calculate the n' => sum{ni'} at given time
            long total_updates = sumOfUpdates(counter);

            // Update the last broadcast value(n_dash)
            counter.setLastBroadcastValue(total_updates);

            // Broadcast an update message to workers
            broadcastUpdate(coordinatorState,counter,out);

            // Update the number of messages sent from coordinator
            coordinatorState.setNumMessagesSent(coordinatorState.getNumMessagesSent() + coordinatorState.getNumOfWorkers());

            // Update the probability( last broadcast value => n_hat )
            // Find the next power of 2 that is smaller than epsilon*n_hat/sqrt(workers)
            double power = highestPowerOf2(counter.getEpsilon(),total_updates,coordinatorState.getNumOfWorkers(),counter.getResConstant());
            if (power <= 1d) power = 1d;

            // Update the new probability
            double new_prob = 1.0 / power;
            counter.setProb(new_prob);

            // Update the number of round
            updateNumOfRound(counter);

            // Start a new round,update sync
            counter.setSync(true);

            // Reset the temporary counter of COUNTER messages
            counter.setCounter(0L);

        }

        // Update the coordinator state
        // coordinatorState.updateCoordinatorState();

    }

    // Broadcast an update message to all workers
    public static void broadcastUpdate(CoordinatorState coordinatorState, Counter counter, Collector<OptMessage> out){

        OptMessage message;

        for (int i=0;i<coordinatorState.getNumOfWorkers();i++){

            // Initialize the message
            message = new OptMessage(String.valueOf(i),counter,UPDATE,0,counter.getLastBroadcastValue());
            message.setTimestamp(System.currentTimeMillis()); // Update the timestamp

            // Send the message
            out.collect(message);
        }
    }

    // Broadcast an increment message to all workers
    public static void broadcastCounter(CoordinatorState coordinatorState, Counter counter, Collector<OptMessage> out){

        OptMessage message;

        for (int i=0;i<coordinatorState.getNumOfWorkers();i++){

            // Initialize the message
            message = new OptMessage(String.valueOf(i),counter,COUNTER,0,0);
            message.setTimestamp(System.currentTimeMillis()); // Update the timestamp

            // Send the message
            out.collect(message);
        }
    }


    // *************************************************************************
    //                              Exact Counter
    // *************************************************************************

    // INCREMENT message
    public static void handleExactIncrement(CoordinatorState coordinatorState,
                                            CoProcessFunction<?,?,?>.Context ctx,
                                            OptMessage message) throws Exception {

        // Get the counter
        Counter counter = coordinatorState.getCounters().get(message.getKeyMessage());

        // Update the last sent value from worker
        counter.setLastSentValues(message.getWorker(),message.getValue());

        // Update the timestamp
        counter.setLastTs(message.getTimestamp());

        // Collect some statistics
        // collectStatisticsCord(ctx,message,counter);

        // Update the coordinator state
        // coordinatorState.updateCoordinatorState();
    }


    // *************************************************************************
    //                              Continuous Counter
    // *************************************************************************

    // INCREMENT message
    public static void handleContinuousIncrement(CoordinatorState coordinatorState,
                                                 CoProcessFunction<?,?,?>.Context ctx,
                                                 OptMessage message) throws Exception {

        // Get the counter
        Counter counter = coordinatorState.getCounters().get(message.getKeyMessage());

        // Update the last sent value from worker
        counter.setLastSentValues(message.getWorker(),message.getValue());

        // Update the timestamp
        counter.setLastTs(message.getTimestamp());

        // Collect some statistics
        // collectStatisticsCord(ctx,message,counter);

        // Update the coordinator state
        // coordinatorState.updateCoordinatorState();
    }


    // *************************************************************************
    //                              Deterministic Counter
    // *************************************************************************

    // INCREMENT message
    public static void handleDeterministicIncrement(CoordinatorState coordinatorState,
                                                    CoProcessFunction<?,?,?>.Context ctx,
                                                    OptMessage message,
                                                    Collector<OptMessage> out) throws Exception {

        // Get the counter
        Counter counter = coordinatorState.getCounters().get(message.getKeyMessage());

        // Update the timestamp
        counter.setLastTs(message.getTimestamp());

        // Overhead from additional messages
        /*if( !counter.isSync() || ( calculateCondition(counter.getEpsilon(),counter.getLastBroadcastValue(),coordinatorState.getNumOfWorkers())+1 != message.getValue() ) ){

            // Update the overhead
            if( (calculateCondition(counter.getEpsilon(),counter.getLastBroadcastValue(),coordinatorState.getNumOfWorkers())+1) != message.getValue() ){
                coordinatorState.setOverheadMessages(coordinatorState.getOverheadMessages()+1L);
            }

            // Update the estimator
            if(!counter.isSync()) counter.setCounter(counter.getCounter()+message.getValue());
        }*/
        if( !counter.isSync() || counter.getLastBroadcastValue() != message.getLastBroadcastValue() ) {

            // Update the overhead
            if ( counter.getLastBroadcastValue() != message.getLastBroadcastValue() ) {
                coordinatorState.setOverheadMessages(coordinatorState.getOverheadMessages() + 1L);
            }

            // Update the estimator
            if (!counter.isSync()) counter.setCounter(counter.getCounter() + message.getValue());
        }

        // INCREMENT message
        if( counter.isSync() ){

            // Update the number of messages
            counter.setNumMessages(counter.getNumMessages()+1L);

            // Update the estimator
            counter.setCounter(counter.getCounter()+message.getValue());

            // Check the condition
            if( counter.getNumMessages() == coordinatorState.getNumOfWorkers() ){

                /*if( calculateCondition(counter.getEpsilon(),counter.getCounter(),coordinatorState.getNumOfWorkers()) > 1){

                    // Broadcast DRIFT message for all workers to receive the counters
                    broadcastDrift(coordinatorState,counter,out);

                    // Update the number of messages sent from coordinator
                    coordinatorState.setNumMessagesSent(coordinatorState.getNumMessagesSent()+coordinatorState.getNumOfWorkers());

                    // Update the sync and wait for local counter
                    counter.setSync(false);
                }*/

                // Broadcast DRIFT message for all workers to receive the counters
                broadcastDrift(coordinatorState,counter,out);

                // Update the number of messages sent from coordinator
                coordinatorState.setNumMessagesSent(coordinatorState.getNumMessagesSent()+coordinatorState.getNumOfWorkers());

                // Reset the number of messages
                counter.setNumMessages(0L);

                // Update the sync and wait for local counter
                counter.setSync(false);

                // Reset last broadcast value waiting the messages from workers and updated when received the DRIFT messages from workers
                // Last sent value act as last broadcast tmp value
                // counter.setLastSentValue(coordinatorState.getCondition());
                counter.setLastSentValue(0L);

                // Collect some statistics about messages
                // collectMessagesCord(ctx,DRIFT,message,counter);

            }

        }

        // Collect some statistics
        // collectStatisticsCord(ctx,message,counter);

        // Update the coordinator state
        // coordinatorState.updateCoordinatorState();
    }

    // DRIFT message
    public static void handleDrift(CoordinatorState coordinatorState,
                                   CoProcessFunction<?,?,?>.Context ctx,
                                   OptMessage message,
                                   Collector<OptMessage> out) throws Exception {

        // Get the counter
        Counter counter = coordinatorState.getCounters().get(message.getKeyMessage());

        // Update the timestamp
        counter.setLastTs(message.getTimestamp());

        // Update the value that will be transmitted to workers
        counter.setLastSentValue(counter.getLastSentValue()+message.getValue());

        // Update the number of messages
        counter.setNumMessages(counter.getNumMessages()+1L);

        // Check the condition
        if( counter.getNumMessages() == coordinatorState.getNumOfWorkers() ){

            // Update the last broadcast value
            counter.setLastBroadcastValue(counter.getCounter()+counter.getLastSentValue());

            // Update the estimator
            counter.setCounter(counter.getLastBroadcastValue());

            // Send message to workers
            broadcastUpdate(coordinatorState,counter,out);

            // Update the number of messages sent from coordinator
            coordinatorState.setNumMessagesSent(coordinatorState.getNumMessagesSent()+coordinatorState.getNumOfWorkers());

            // Reset the number of messages
            counter.setNumMessages(0L);

            // Update the number of round
            // The field counterDoubles act as number of round for deterministic counter
            counter.setCounterDoubles(counter.getCounterDoubles()+1L);

            // Start a new round,update sync
            counter.setSync(true);

            // Reset the last sent value
            counter.setLastSentValue(0L);

            // Collect some statistics about messages
            // collectMessagesCord(ctx,UPDATE,message,counter);
        }

        // Collect some statistics
        // collectStatisticsCord(ctx,message,counter);

        // Update the coordinator state
        // coordinatorState.updateCoordinatorState();

    }

    // Broadcast an drift message to all workers
    public static void broadcastDrift(CoordinatorState coordinatorState, Counter counter, Collector<OptMessage> out){

        OptMessage message;

        for (int i=0;i<coordinatorState.getNumOfWorkers();i++){

            // Initialize the message
            message = new OptMessage(String.valueOf(i),counter,DRIFT,0,0);
            message.setTimestamp(System.currentTimeMillis()); // Update the timestamp

            // Send the message
            out.collect(message);
        }
    }


    // *************************************************************************
    //                                  Utils
    // *************************************************************************

    // Calculate the sum of last updates(ni') for a specific counter
    public static long sumOfUpdates(Counter counter){

        AtomicLong sum = new AtomicLong();
        counter.getLastSentUpdates().values().forEach(sum::addAndGet);
        return sum.get();

    }

    // Calculate the sum of last sent values(ni_dash) for a specific counter
    public static long sumOfLastSentValues(Counter counter){

        AtomicLong sum = new AtomicLong();
        counter.getLastSentValues().values().forEach(sum::addAndGet);
        return sum.get();

    }

    // Update the number of round for a specific counter
    public static void updateNumOfRound(Counter counter){ counter.setNumMessages(counter.getNumMessages()+1L); }


    // *************************************************************************
    //                                 Parameters
    // *************************************************************************

    // Bayesian Network(BN)

    // Estimate all the parameters of Bayesian Network(BN)
    public static void estimateParameters(CoordinatorState coordinatorState, CoProcessFunction<?,?,?>.Context ctx) throws Exception {

        double parameter;
        Counter counter;

        for (long key: coordinatorState.getCounters().keySet()){

            counter = coordinatorState.getCounter(key);

            if(counter.isActualNode()){

                // Estimate the parameter
                parameter = estimateParameter(coordinatorState,counter);

                // Get the truth parameter
                double truthParameter = counter.getTrueParameter();

                // Collect some statistics about parameters
                ctx.output(coordinator_stats,"Parameter : " + parameter
                                                               + " " + counter.getParameter()
                                                               + " , error : " + Math.abs(parameter-truthParameter));
            }

        }

    }

    // Estimate the parameter of Bayesian Network(BN)
    public static double estimateParameter(CoordinatorState coordinatorState, Counter counter) throws Exception {

        long estimator_node;
        long estimator_parent = 0L;
        long parent_index = -1L;

        ArrayList<Long> indexes = null;
        Counter parent_counter;

        // Check for parents node
        if(counter.getNodeParents() != null){
            // Find the parent node
            parent_index = findParentNode(coordinatorState,counter);
        }
        else{
            // Find all indexes of node for the counter
            indexes = findIndexesCounter(coordinatorState,counter);
        }


        // Parameter = estimator_node / estimator_parent

        // Calculate the estimator of node
        if( coordinatorState.getTypeCounter() == DETERMINISTIC ) estimator_node = counter.getCounter();
        else estimator_node = calculateEstimator(counter);

        // Calculate the estimator of parent
        if( parent_index != -1L ){

            parent_counter = coordinatorState.getCounter(parent_index);

            // Check the type of counter
            if( coordinatorState.getTypeCounter() == DETERMINISTIC ) estimator_parent = parent_counter.getCounter();
            else estimator_parent = calculateEstimator(parent_counter);
        }
        else{
            Counter nodeCounter;
            for(Long indexNode : Objects.requireNonNull(indexes) ){
                nodeCounter = coordinatorState.getCounter(indexNode);

                // Check the type of counter
                if( coordinatorState.getTypeCounter() == DETERMINISTIC ) estimator_parent += nodeCounter.getCounter();
                else estimator_parent += calculateEstimator(nodeCounter);
            }
        }


        // Laplace smoothing for probability of parameter(only for zero parameters)
        return laplaceSmoothing(estimator_node,estimator_parent,findCardinality(new ArrayList<>(coordinatorState.getCoordinatorState().getCounters().values()),counter),1.0d);

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

    // Find all the indexes of search counter , that all the counters with the same node name(BN)
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

    // Calculate the randomized estimator(ni_hat)
    public static long calculateEstimator(Counter counter){

        // Estimator
        AtomicLong estimator = new AtomicLong();

        counter.getLastSentValues().values().forEach( value -> {
            // If ni_hat exists
            if( value != 0 ){
                // Calculate the new estimation => ni_hat - 1 + 1/prob
                estimator.addAndGet((long) (value - 1 + (1.0 / counter.getProb())));
            }

        });

        return estimator.get();
    }

    // Estimate the logarithm of joint probability based on truth parameters of Bayesian Network(BN) - Method A(without true parameters on counters)
    public static double estimateJointProbTruth(CoordinatorState coordinatorState, Input input, CBNConfig config) throws IOException {

        // Estimate the joint probability from scratch based on config
        ObjectMapper objectMapper = new ObjectMapper();

        // Unmarshall JSON object as array of NodeBN_schema
        NodeBN_schema[] nodes = objectMapper.readValue(config.BNSchema(), NodeBN_schema[].class);

        // Get the schema of dataset
        List<String> datasetSchema = new ArrayList<>(coordinatorState.getDatasetSchema().keySet());

        // Get the value of input
        List<String> values = Arrays.asList(input.getValue().replaceAll(" ","").split(","));

        // Calculate the probability node by node
        // Log to avoid underflow problems
        ArrayList<String> inputValues = new ArrayList<>();
        // double prob = 1.0d;
        double logProb = 0d;

        // Parsing the nodes based on the schema of dataset
        for(int i=0;i<datasetSchema.size();i++){

            inputValues.add(values.get(i));
            int index = findIndexNode(datasetSchema.get(i),nodes);

            // Update the probability
            if( index != -1 ){
                // prob = prob * findProbability(inputValues,datasetSchema,nodes[index]);
                logProb += log(findProbability(inputValues,datasetSchema,nodes[index]));
            }
        }

        return logProb;
    }

    // Estimate the logarithm of joint probability based on truth parameters of Bayesian Network(BN) - Method B
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
        long estimator_node;
        long estimator_parent = 0L;

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

                // Calculate the estimation of counter based on type of counter
                if( coordinatorState.getTypeCounter() == DETERMINISTIC ) estimator_node = counter.getCounter();
                else estimator_node = calculateEstimator(counter);


                // Estimator parent
                // Get the key
                String parentKey = (nameParents.toString()+valuesParents+","+name).replace(" ","");

                // Hashing the key and get the parent-counter
                Counter parentCounter = coordinatorState.getCounters().get(hashKey(parentKey.getBytes()));

                // Calculate the estimation of parent-counter based on type of counter
                if( coordinatorState.getTypeCounter() == DETERMINISTIC ) estimator_parent = parentCounter.getCounter();
                else estimator_parent = calculateEstimator(parentCounter);


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

                // Calculate the estimation of counter based on type of counter
                if( coordinatorState.getTypeCounter() == DETERMINISTIC ) estimator_node = counter.getCounter();
                else estimator_node = calculateEstimator(counter);


                // Estimator parent
                // For each value of node
                String parent_key;
                for(String parent_value : node.getValues()){

                    // Get the key
                    parent_key = (name+","+parent_value).replace(" ","");

                    // Hashing the key and get the counter
                    Counter parentCounter = coordinatorState.getCounters().get(hashKey(parent_key.getBytes()));

                    // Calculate the estimation of parent-counter based on type of counter
                    if( coordinatorState.getTypeCounter() == DETERMINISTIC ) estimator_parent += parentCounter.getCounter();
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


    // Naive Bayes Classifier(NBC)

    // Get the estimation for all parameters of Naive Bayes Classifier(NBC)
    public static void getEstimationNBC(CoordinatorState coordinatorState) throws IOException{

        // Estimated total count
        double estimatedTotalCount = 0;

        // For each parameter , calculate the estimation
        for (Counter counter : coordinatorState.getCounters().values()) {

            // Get the estimation
            double estimation;

            if( coordinatorState.getTypeCounter() == DETERMINISTIC ) estimation = counter.getCounter();
            else estimation = calculateEstimator(counter);

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

    // Estimate the class of input(NBC),using MAP(Maximum a Posterior) that is the class with the highest probability
    public static String getEstimatedClass(CoordinatorState coordinatorState, Input input) throws Exception {

        // For each class value calculate the probability and keep the max
        // In Naive Bayes Classifier the attributes are mutually independent(conditional independent given the class)
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

            // Get the class counter
            Counter classCounter = coordinatorState.getCounter(hashKey(classKey.getBytes()));

            // Update the estimated total count
            if( coordinatorState.getTypeCounter() == DETERMINISTIC ) estimatedTotalCount += classCounter.getCounter();
            else estimatedTotalCount += calculateEstimator(classCounter);
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
            if( coordinatorState.getTypeCounter() == DETERMINISTIC ) classEstimator = classCounter.getCounter();
            else classEstimator = calculateEstimator(classCounter);

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
                if( coordinatorState.getTypeCounter() == DETERMINISTIC ) attrEstimator = attrCounter.getCounter();
                else attrEstimator = calculateEstimator(attrCounter);

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
        double avgGT = 0d;
        ctx.output(coordinator_stats,"\nProcessing BN queries : "+queries.size()+"\n");

        // Processing the queries
        for(Input query : queries){

            double truthProb = estimateJointProbTruth(coordinatorState,query);
            double estProb = estimateJointProbEst(coordinatorState,query);
            double error = estProb-truthProb;

            // Collect some statistics
            ctx.output(coordinator_stats,"Input : " + query
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
        ctx.output(coordinator_stats,"Average error : " + avgError/queries.size()
                                                           + " , lower violations : " + lbv
                                                           + " , upper violations : " + ubv);

        // Reset the end of stream and end of worker counter
        coordinatorState.setEndOfStreamCounter(0);
        coordinatorState.setEndOfWorkerCounter(0);
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

            ctx.output(coordinator_stats,"Input : " + query + " , estimated class : " + classValue);

        }

        // Collect some statistics
        System.out.println("\nProcessed queries : "+queries.size()+"\n");

        // Reset the end of stream and end of worker counter
        coordinatorState.setEndOfStreamCounter(0);
        coordinatorState.setEndOfWorkerCounter(0);
    }

    // *************************************************************************
    //                                 Metrics
    // *************************************************************************

    // Write some statistics for coordinator to side output
    public static void collectStatisticsCord(CoProcessFunction<?,?,?>.Context ctx, OptMessage message,Counter counter){

        // Assign a timer service
        // TimerService timerService = ctx.timerService();

        // Current timestamp
        // long currentTimestamp = System.currentTimeMillis();

        /* ctx.output(coordinator_stats,"Coordinator "
                                        + " " + message
                                        + " , " + counter
                                        // + " , ctx_timestamp : " + new Timestamp(ctx.timestamp() == null ? 0 : ctx.timestamp())
                                        // + " , current_processing_time : " + new Timestamp(timerService.currentProcessingTime())
                                        // + " , current watermark : " + new Timestamp(timerService.currentWatermark())
                                        + " at " + new Timestamp(currentTimestamp) ); */

    }


    // Write the messages which sent or received from/to workers to side output
    public static void collectMessagesCord(CoProcessFunction<?,?,?>.Context ctx,
                                           TypeMessage typeMessage,
                                           OptMessage message,
                                           Counter counter){

        /* ctx.output(coordinator_stats,"\nReceived and send messages from/to workers "
                                        + " , new round begins "
                                        + " , type_message_rec : " + message.getTypeMessage()
                                        + " , type_message_sent : " + typeMessage
                                        + " " + message
                                        + " " + counter
                                        + " at " + new Timestamp(System.currentTimeMillis())+"\n");*/
    }

    // Print map state
    public <K,V> String printMapState(MapState<K,V> mapState) throws Exception {

        StringBuilder str = new StringBuilder();
        str.append("[");

        for( Map.Entry<K,V> entry : mapState.entries() ){
            str.append("(").append(entry.getKey()).append(",").append(entry.getValue().toString()).append(")").append(",");
        }

        if( str.toString().length() > 1) str.deleteCharAt(str.lastIndexOf(","));
        str.append("]");
        return str.toString();

    }


}
