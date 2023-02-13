package fgm.worker;

import config.FGMConfig;
import datatypes.input.Input;
import datatypes.message.MessageFGM;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import fgm.state.WorkerState;

import java.io.IOException;
import java.sql.Timestamp;

import static config.InternalConfig.TypeMessage.*;
import static fgm.job.JobFGM.worker_stats;
import static fgm.worker.WorkerFunction.*;

public class Worker extends KeyedCoProcessFunction<String,Input,MessageFGM,MessageFGM> {

    private WorkerState workerState;
    private final FGMConfig config;

    // Constructor
    public Worker(FGMConfig config){ this.config = config; }

    @Override
    // Initialize state
    public void open(Configuration parameters){ workerState = new WorkerState(getRuntimeContext(),config); }

    @Override
    // Input from source
    // Output to the coordinator
    public void processElement1(Input input, Context ctx, Collector<MessageFGM> out) throws IOException {

        // Initialization
        if( workerState.getParameters() == null ){

            workerState.initializeWorkerState(config);

            // Collect some statistics
            workerState.printWorkerState(ctx);
            ctx.output(worker_stats,"Worker "+ctx.getCurrentKey()+" => "+config);

            // Update the lastTs( for window computation )
            workerState.setLastTs(input.getTimestamp());

            // Throughput
            ctx.output(worker_stats,"Worker "+ctx.getCurrentKey()+" => Throughput ");
            ctx.output(worker_stats,"Worker "+ctx.getCurrentKey()+" receive first event at "+new Timestamp(System.currentTimeMillis())+" , "+System.currentTimeMillis()+"\n");

            // Timer
            workerState.setTimer(System.currentTimeMillis());
            ctx.output(worker_stats,"Worker "+ctx.getCurrentKey()+" timer"+" , "+System.currentTimeMillis()+" , total count "+workerState.getTotalCount()+"\n");
        }

        // END_OF_STREAM
        if( input.getValue().equals("EOF") ){

            // Update the end of stream counter
            workerState.setEndOfStreamCounter(workerState.getEndOfStreamCounter()+1);

            if(workerState.getEndOfStreamCounter() == config.sourcePartition()) {

                // Throughput
                ctx.output(worker_stats, "Worker " + ctx.getCurrentKey() + " last event at " + new Timestamp(System.currentTimeMillis()) + " , " + System.currentTimeMillis() + " , total count " + workerState.getTotalCount() + "\n");

                // Send END_OF_STREAM message
                out.collect(new MessageFGM(ctx.getCurrentKey(), END_OF_STREAM, System.currentTimeMillis(), 0));

                // Collect some statistics
                System.out.println("Worker " + ctx.getCurrentKey()
                                             + " received EOF "
                                             + " , total count : " + workerState.getTotalCount()
                                             + " ,  numMessages : " + workerState.getNumOfMessages());

                ctx.output(worker_stats, "Worker " + ctx.getCurrentKey()
                                                      + " received EOF "
                                                      + " , total count : " + workerState.getTotalCount()
                                                      + " ,  numMessages : " + workerState.getNumOfMessages());

                // Register a processing time timer
                ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + Time.milliseconds(100L).toMilliseconds());

                // Reset the end of stream counter
                workerState.setEndOfStreamCounter(0);

            }

            return;
        }

        // Per 500 tuples, calculate the throughput
        if(workerState.getTotalCount() % 500 == 0 ){
            ctx.output(worker_stats,"Worker "+ctx.getCurrentKey()+" receive events at "+new Timestamp(System.currentTimeMillis())+" , "+System.currentTimeMillis()+" , total count "+workerState.getTotalCount()+"\n");
        }

        // Per 100 ms , emit the processed tuples
        if((System.currentTimeMillis() - workerState.getTimer()) >= 100L){
            ctx.output(worker_stats,"Worker "+ctx.getCurrentKey()+" timer"+" , "+workerState.getTimer()+" , total count "+workerState.getTotalCount()+"\n");
            workerState.setTimer(System.currentTimeMillis());
        }

        // Process the input
        handleInput(workerState,ctx,input,out);

    }

    @Override
    // Input from feedback source
    // When receive a new message then update the sending condition
    public void processElement2(MessageFGM message, Context ctx, Collector<MessageFGM> out) throws IOException {

        // DRIFT message
        if (message.getTypeMessage() == DRIFT) {

            // Collect some statistics
            // collectReceivedMessages(workerState,ctx,message);

            // Send the drift vector
            sendDriftVector(workerState, ctx, out);

            // Collect some statistics
            // collectWorkerState(workerState,ctx);

        }
        // QUANTUM message
        else if (message.getTypeMessage() == QUANTUM) {

            // Collect some statistics
            // collectReceivedMessages(workerState,ctx,message);

            // New subround begin
            newSubRound(workerState, message);

            // Subround process
            subRound(workerState,ctx,out);

            // Collect some statistics
            // collectWorkerState(workerState,ctx);

        }
        // Global ESTIMATE message
        else if (message.getTypeMessage() == ESTIMATE) {

            // Collect some statistics
            // collectReceivedMessages(workerState,ctx,message);

            // New round begin
            newRound(workerState, message);
            subRound(workerState,ctx,out); // Subround process,  maybe not needed ???

            // Collect some statistics
            // collectWorkerState(workerState,ctx);
        }
        // ZETA message
        else if (message.getTypeMessage() == ZETA) {

            // Collect some statistics
            // collectReceivedMessages(workerState,ctx,message);

            // The end of subround
            // Send zeta to coordinator
            sendZeta(workerState, ctx, out);

            // Collect some statistics
            // collectWorkerState(workerState,ctx);

        }
        // LAMBDA message
        else if(message.getTypeMessage() == LAMBDA){

            // Collect some statistics
            // collectReceivedMessages(workerState,ctx,message);

            // Rebalancing ???
            handleLambda(workerState,message);
            sendZeta(workerState,ctx,out);

            // Collect some statistics
            // collectWorkerState(workerState,ctx);

        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<MessageFGM> out ) throws IOException {

        // Not synchronized protocol
        if( !workerState.getSyncWorker() ){

            // System.out.println("Worker "+ctx.getCurrentKey()+" : Protocol is not synchronized");

            // Register a processing time timer to checked it again
            ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis()+ Time.milliseconds(100L).toMilliseconds());

            return;
        }

        // Collect some statistics about worker
        ctx.output(worker_stats, "Worker "+ctx.getCurrentKey()
                                            + " => Total count : " + workerState.getTotalCount()
                                            + " , numMessages : " + workerState.getNumOfMessages()+"\n");

        ctx.output(worker_stats, "Worker "+ctx.getCurrentKey()+" onTimer worker !!!");
        ctx.output(worker_stats, "Worker "+ctx.getCurrentKey()+" End of process ");
        workerState.printWorkerState(ctx);


        ctx.output(worker_stats, "Worker "+ctx.getCurrentKey()
                                    + " => Total count : " + workerState.getTotalCount()
                                    + " , numMessages : " + workerState.getNumOfMessages()+"\n");

    }

}
