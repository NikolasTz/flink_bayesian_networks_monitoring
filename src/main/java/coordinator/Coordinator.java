package coordinator;

import config.CBNConfig;
import datatypes.input.Input;
import datatypes.message.OptMessage;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import state.CoordinatorState;

import java.sql.Timestamp;
import java.util.List;

import static config.InternalConfig.TypeNetwork.NAIVE;
import static coordinator.CoordinatorFunction.*;
import static config.InternalConfig.TypeMessage.*;
import static job.JobKafka.coordinator_stats;
import static state.CoordinatorState.AreQueriesReady;

public class Coordinator extends CoProcessFunction<OptMessage,Input,OptMessage> {

    private CoordinatorState coordinatorState;
    private final CBNConfig config;
    private transient ListState<Input> queries;

    // Constructor
    public Coordinator(CBNConfig config){ this.config = config; }

    @Override
    // Initialize state
    public void open(Configuration parameters){

        coordinatorState = new CoordinatorState(getRuntimeContext(),config);

        // Initialize the queries
        queries = getRuntimeContext().getListState(new ListStateDescriptor<>("queries", Types.POJO(Input.class)));
    }

    @Override
    public void processElement1(OptMessage message, Context ctx, Collector<OptMessage> out) throws Exception {

        // Initialization
        if( coordinatorState.getCoordinatorState() == null ){

            // Initialize the state of coordinator
            coordinatorState.initializeCoordinatorState(config.BNSchema());

            // Print coordinator state
            ctx.output(coordinator_stats,"Initialization \n");
            coordinatorState.printCoordinatorState(ctx);
            ctx.output(coordinator_stats,"Coordinator => "+config+"\n");

            // Runtime
            ctx.output(coordinator_stats,"Coordinator received first message at "+ new Timestamp(System.currentTimeMillis())+" , "+System.currentTimeMillis()+"\n");
        }

        // Update the number of messages sent from Workers
        coordinatorState.setNumMessagesSentWorkers(coordinatorState.getNumMessagesSentWorkers()+1L);

        // Check the type of message
        // INCREMENT
        if ( message.getTypeMessage() == INCREMENT ) { handleIncrement(coordinatorState,ctx,message,out); }

        // DOUBLES
        else if( message.getTypeMessage() == DOUBLES ){ handleDoubles(coordinatorState,ctx,message,out); }

        // DRIFT
        else if( message.getTypeMessage() == DRIFT ){ handleDrift(coordinatorState,ctx,message,out); }

        // COUNTER
        else if( message.getTypeMessage() == COUNTER ){ handleCounter(coordinatorState,ctx,message,out); }

        // END_OF_STREAM
        else if( message.getTypeMessage() == END_OF_STREAM ){

            // Update the endOfStreamCounter
            coordinatorState.setEndOfStreamCounter(coordinatorState.getEndOfStreamCounter()+1);

            // Collect some statistics
            ctx.output(coordinator_stats,"\nCoordinator state => numMessagesSent : " + coordinatorState.getNumMessagesSent()
                                                                +" overheadMessages : " + coordinatorState.getOverheadMessages()
                                                                +" numMessagesSentWorkers : " + coordinatorState.getNumMessagesSentWorkers()+"\n");

            // Check if stream is finished to all workers
            // && coordinatorState.getEndOfWorkerCounter() == coordinatorState.getNumOfWorkers()
            if( coordinatorState.getEndOfStreamCounter() == coordinatorState.getNumOfWorkers()
                && AreQueriesReady(queries,config.queriesSize()) ){

                System.out.println("END OF STREAM");

                // Register a timer
                // ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis()+ Time.milliseconds(100L).toMilliseconds());

                // Collect some statistics
                ctx.output(coordinator_stats,"\nCoordinator state => numMessagesSent : " + coordinatorState.getNumMessagesSent()
                                                +" overheadMessages : " + coordinatorState.getOverheadMessages()
                                                +" numMessagesSentWorkers : " + coordinatorState.getNumMessagesSentWorkers()+"\n");

                // Runtime
                ctx.output(coordinator_stats,"Coordinator received last message at "+ new Timestamp(System.currentTimeMillis())+" , "+System.currentTimeMillis()+"\n");

                // or ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis()+ Time.minutes(2L).toMilliseconds()) and onTimer logic

                // if (coordinatorState.getTypeNetwork() == NAIVE) processingQueriesNBC((List<Input>) queries.get(), coordinatorState, ctx);
                // else processingQueriesBN((List<Input>) queries.get(), coordinatorState, ctx);
            }
        }

        // END_OF_WORKER
        else if( message.getTypeMessage() == END_OF_WORKER ){

            // Update the endOfWorkerCounter
            coordinatorState.setEndOfWorkerCounter(coordinatorState.getEndOfWorkerCounter()+1);

            // Check if stream and messages are finished to all workers
            if( coordinatorState.getEndOfStreamCounter() == coordinatorState.getNumOfWorkers()
                && coordinatorState.getEndOfWorkerCounter() == coordinatorState.getNumOfWorkers()
                && AreQueriesReady(queries,config.queriesSize())){

                System.out.println("END OF WORKER");

                // Register a timer for processing the available queries and collect statistics
                ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis()+ Time.milliseconds(100L).toMilliseconds());

                // Collect some statistics
                // ctx.output(coordinator_stats,"\nCoordinator state => numMessagesSent : " + coordinatorState.getNumMessagesSent()
                //                                 +" overheadMessages : " + coordinatorState.getOverheadMessages()
                //                                 +" numMessagesSentWorkers : " + coordinatorState.getNumMessagesSentWorkers()+"\n");

                // Runtime
                // ctx.output(coordinator_stats,"Coordinator received last message at "+ new Timestamp(System.currentTimeMillis())+" , "+System.currentTimeMillis()+"\n");

            }

        }

        // DETERMINISTIC for queries => if( coordinatorState.getTypeCounter() == InternalConfig.TypeCounter.DETERMINISTIC && coordinatorState.checkCountersSync() ) on END_OF_STREAM
        // and if( coordinatorState.getEndOfStreamCounter() == coordinatorState.getNumOfWorkers() && AreQueriesReady(queries,config.queriesSize()) && coordinatorState.checkCountersSync() ) on DRIFT

        // Estimate the parameters (estimate continuously or periodically with callback)
        // estimateParameters(coordinatorState,ctx);

        // Collect some statistics
        // ctx.output(coordinator_stats,"\nCoordinator state => numMessagesSent : " + coordinatorState.getNumMessagesSent()
        //                                +" overheadMessages : " + coordinatorState.getOverheadMessages()+"\n");

    }

    @Override
    public void processElement2(Input input, Context ctx, Collector<OptMessage> out) throws Exception {

        // Handle the queries/testing data

        // Estimate the parameters after the learning
        if( coordinatorState.getEndOfStreamCounter() == null){
            coordinatorState.setEndOfStreamCounter(0);
            coordinatorState.setEndOfWorkerCounter(0);
        }
        if( coordinatorState.getEndOfStreamCounter() < config.workers() || !AreQueriesReady(queries,config.queriesSize()-1) ){

            // TODO collect all the testing data(completed) or read after the learning(kafka topic or watermark) ???

            // Collect the queries
            queries.add(input);
        }
        else{

            // Collect the last query and start the processing
            queries.add(input);

            if( coordinatorState.getTypeNetwork() == NAIVE ) processingQueriesNBC((List<Input>) queries.get(),coordinatorState,ctx);
            else processingQueriesBN((List<Input>) queries.get(),coordinatorState,ctx);
        }

        // Register event time timer
        // ctx.timerService().registerEventTimeTimer(1627851824999L); // First window
        // ctx.timerService().registerEventTimeTimer(0L);
        // ctx.timerService().registerProcessingTimeTimer(currentTimestamp+500);

        // Collect some statistics
        /*ctx.output(coordinator_stats,"Testing dataset : "
                                        + " endOfStreamCounter : " + coordinatorState.getEndOfStreamCounter()
                                        + " , queries size : " + Iterators.size(queries.get().iterator()));
                                        // or + " , queries size : " + ((Collection<?>) queries.get()).size()*/

    }

    @Override
    public void onTimer(long timestamp,OnTimerContext ctx,Collector<OptMessage> out ) throws Exception {

        System.out.println("On timer Cord !!!");

        // Not synchronized
        if( !coordinatorState.checkCountersSync() ){
            System.out.println("All counters are not synchronized");

            // Register a processing time timer to checked it again
            ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + Time.milliseconds(100L).toMilliseconds());

            return;
        }

        // Collect some statistics
        ctx.output(coordinator_stats,"\nCoordinator state => numMessagesSent : " + coordinatorState.getNumMessagesSent()
                                        +" overheadMessages : " + coordinatorState.getOverheadMessages()
                                        +" numMessagesSentWorkers : " + coordinatorState.getNumMessagesSentWorkers()+"\n");

        // Print coordinator state
        ctx.output(coordinator_stats,"End of processing");
        coordinatorState.printCoordinatorState(ctx);

        // Processing the available queries
        if (coordinatorState.getTypeNetwork() == NAIVE) processingQueriesNBC((List<Input>) queries.get(), coordinatorState, ctx);
        else processingQueriesBN((List<Input>) queries.get(), coordinatorState, ctx);


        // Collect some statistics
        ctx.output(coordinator_stats,"\nCoordinator state => numMessagesSent : " + coordinatorState.getNumMessagesSent()
                                        +" overheadMessages : " + coordinatorState.getOverheadMessages()
                                        +" numMessagesSentWorkers : " + coordinatorState.getNumMessagesSentWorkers()+"\n");

        // Throw out a dummy exception and sleep for 10 sec
        Thread.sleep(10000L);
        System.out.println("Dummy exception : "+(1/0));

        // estimateParameters(coordinatorState,ctx);

    }


}
