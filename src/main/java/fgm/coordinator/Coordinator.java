package fgm.coordinator;

import config.FGMConfig;
import datatypes.input.Input;
import datatypes.message.MessageFGM;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import fgm.state.CoordinatorState;

import java.sql.Timestamp;
import java.util.List;

import static config.InternalConfig.TypeMessage.*;
import static fgm.coordinator.CoordinatorFunction.*;
import static config.InternalConfig.TypeNetwork.*;
import static fgm.job.JobFGM.coordinator_stats;
import static fgm.state.CoordinatorState.AreQueriesReady;
import static utils.MathUtils.compareArrays;

public class Coordinator extends CoProcessFunction<MessageFGM,Input,MessageFGM> {

    private CoordinatorState coordinatorState;
    private final FGMConfig config;
    private transient ListState<Input> queries;

    // Constructor
    public Coordinator(FGMConfig config){ this.config = config; }

    @Override
    // Initialize state
    public void open(Configuration parameters) {

        coordinatorState = new CoordinatorState(getRuntimeContext(), config);

        // Initialize the queries
        queries = getRuntimeContext().getListState(new ListStateDescriptor<>("queries", Types.POJO(Input.class)));
    }

    @Override
    public void processElement1(MessageFGM message, Context ctx, Collector<MessageFGM> out) throws Exception {

        // Initialization
        if( coordinatorState.getParameters() == null ){

            coordinatorState.initializeCoordinatorState(config);

            // Collect statistics
            coordinatorState.printCoordinatorState(ctx);
            ctx.output(coordinator_stats,"FGMConfig => "+config);

            // Runtime
            ctx.output(coordinator_stats,"Coordinator received first message at "+ new Timestamp(System.currentTimeMillis())+" , "+System.currentTimeMillis()+"\n");
        }

        // Processing messages from workers

        // DRIFT message
        if( message.getTypeMessage() == DRIFT ){

            // Collect some statistics
            // collectRecMessages(ctx,message);

            // Send global estimated vector E to workers when has received all drift vectors Xi
            handleDrift(coordinatorState,message,ctx,out);

            // Update the number of drift messages
            coordinatorState.setNumOfDriftMessages(coordinatorState.getNumOfDriftMessages()+1L);

            // Collect some statistics
            // collectCordState(coordinatorState,ctx);
        }
        // INCREMENT message
        else if( message.getTypeMessage() == INCREMENT ){

            // Collect some statistics
            // collectRecMessages(ctx,message);

            // Handle the increment message
            // During the subround process
            handleIncrement(coordinatorState,message,ctx,out);

            // Update the number of increment messages
            coordinatorState.setNumOfIncrementMessages(coordinatorState.getNumOfIncrementMessages()+1L);

            // Collect some statistics
            // collectCordState(coordinatorState,ctx);

        }
        // ZETA message
        else if( message.getTypeMessage() == ZETA ){

            // Collect some statistics
            // collectRecMessages(ctx,message);

            // The end of subround or round
            handleZeta(coordinatorState,message,ctx,out);

            // Update the number of zeta messages
            coordinatorState.setNumOfZetaMessages(coordinatorState.getNumOfZetaMessages()+1L);

            // Collect some statistics
            // collectCordState(coordinatorState,ctx);
        }
        // END_OF_STREAM
        else if( message.getTypeMessage() == END_OF_STREAM ){

            // Update the endOfStreamCounter
            coordinatorState.setEndOfStreamCounter(coordinatorState.getEndOfStreamCounter()+1);

            // Check if stream have been finished to all workers
            if( coordinatorState.getEndOfStreamCounter() == coordinatorState.getNumOfWorkers() && AreQueriesReady(queries,config.queriesSize()) ){

                // Register a timer
                ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis()+ Time.milliseconds(100L).toMilliseconds());

                // or ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis()+ Time.minutes(2L).toMilliseconds()) and onTimer logic

                // if (coordinatorState.getTypeNetwork() == NAIVE) processingQueriesNBC((List<Input>) queries.get(), coordinatorState, ctx);
                // else processingQueriesBN((List<Input>) queries.get(), coordinatorState, ctx);

                // Runtime
                ctx.output(coordinator_stats,"Coordinator received last message at "+ new Timestamp(System.currentTimeMillis())+" , "+System.currentTimeMillis()+"\n");
            }
        }
    }

    @Override
    public void processElement2(Input input, Context ctx, Collector<MessageFGM> out) throws Exception {


        // Start function used as warm up of the FGM protocol
        if( coordinatorState.getParameters() == null ){

            coordinatorState.initializeCoordinatorState(config);

            // Collect statistics
            coordinatorState.printCoordinatorState(ctx);
            ctx.output(coordinator_stats,"FGMConfig(Messages) => "+config);

            // Runtime
            ctx.output(coordinator_stats,"Coordinator received first message at "+ new Timestamp(System.currentTimeMillis())+" , "+System.currentTimeMillis()+"\n");
            // start(coordinatorState,ctx,config,input);
        }
        if( coordinatorState.getEndOfStreamCounter() < config.workers() || !AreQueriesReady(queries,config.queriesSize()-1) ){

            // Collect queries
            queries.add(input);
        }
        else{

            // Collect queries
            queries.add(input);

            List<Input> test = (List<Input>) queries.get();
            System.out.println("Queries size : "+test.size()+" and endOfStreamCounter : "+coordinatorState.getEndOfStreamCounter());

            // Register a processing time timer
            ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + Time.milliseconds(100L).toMilliseconds());

            // Processing queries
            /*if(coordinatorState.getTypeNetwork() == NAIVE){
                processingQueriesNBC((List<Input>) queries.get(),coordinatorState,ctx);
                // Collect some statistics
            }
            else{

                // Bayesian Network
                processingQueriesBN((List<Input>) queries.get(),coordinatorState,ctx);

                // estimateParameters(coordinatorState,ctx);
            }*/

        }

    }

    @Override
    public void onTimer(long timestamp,OnTimerContext ctx,Collector<MessageFGM> out) throws Exception {

        // Broadcast drift message , only for the start of FGM protocol , warmup round
        if( compareArrays(coordinatorState.getEstimation(),coordinatorState.getEstimatedVector().getEmptyInstance()) && coordinatorState.getEndOfStreamCounter() == 0 ){

            System.out.println("Warmup : Timer timestamp as event time : " + new Timestamp(timestamp));
            broadcastDrift(coordinatorState,out);

            // Update the number of messages
            coordinatorState.setNumOfMessages(coordinatorState.getNumOfMessages()+coordinatorState.getNumOfWorkers());
        }
        else if( coordinatorState.getEndOfStreamCounter() == config.workers() && AreQueriesReady(queries,config.queriesSize()) ){

            System.out.println("On timer Cord !!!");

            if( !coordinatorState.getSyncCord() ){

                System.out.println("Protocol is not synchronized yet");

                // Register a processing time timer to checked it again
                ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis()+ Time.milliseconds(100L).toMilliseconds());

                return;
            }

            // Collect some statistics
            ctx.output(coordinator_stats,"End of processing");
            coordinatorState.printCoordinatorState(ctx);

            // estimateParameters(coordinatorState,ctx);
            if (coordinatorState.getTypeNetwork() == NAIVE) processingQueriesNBC((List<Input>) queries.get(), coordinatorState, ctx);
            else processingQueriesBN((List<Input>) queries.get(), coordinatorState, ctx);

            ctx.output(coordinator_stats, "\nCoordinator state => numMessagesSent : " + coordinatorState.getNumOfMessages()
                                             +" , numOfSentDriftMessages : " + coordinatorState.getNumOfSentDriftMessages()
                                             +" , numOfSentEstMessages : " + coordinatorState.getNumOfSentEstMessages()
                                             +" , numOfSentQuantumMessages : " + coordinatorState.getNumOfSentQuantumMessages()
                                             +" , numOfSentZetaMessages : " + coordinatorState.getNumOfSentZetaMessages()
                                             +" , numOfSentLambdaMessages : " + coordinatorState.getNumOfSentLambdaMessages()
                                             +" , numOfDriftMessages : " + coordinatorState.getNumOfDriftMessages()
                                             +" , numOfIncrementMessages : " + coordinatorState.getNumOfIncrementMessages()
                                             +" , numOfZetaMessages : " + coordinatorState.getNumOfZetaMessages()
                                             +" , numOfRound : " + coordinatorState.getNumOfRounds() +"\n");

            // Throw out a dummy exception and sleep for 10 sec
            Thread.sleep(10000L);
            System.out.println("Dummy exception : "+(1/0));
        }
        else{

            System.out.println("Nothing is ready");

            // Register a processing time timer to checked it again
            ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis()+ Time.milliseconds(100L).toMilliseconds());
        }
    }

}
