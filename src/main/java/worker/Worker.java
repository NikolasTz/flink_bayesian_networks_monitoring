package worker;

import config.CBNConfig;
import datatypes.input.Input;
import datatypes.message.OptMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import state.WorkerState;

import java.io.IOException;
import java.sql.Timestamp;

import static config.InternalConfig.TypeMessage.END_OF_STREAM;
import static config.InternalConfig.TypeMessage.END_OF_WORKER;
import static job.JobKafka.worker_stats;
import static worker.WorkerFunction.*;

public class Worker extends KeyedCoProcessFunction<String,Input,OptMessage,OptMessage>{

    private WorkerState workerState;
    private final CBNConfig config;

    // Constructor
    public Worker(CBNConfig config){ this.config = config; }

    @Override
    // Initialize state
    public void open(Configuration parameters){ workerState = new WorkerState(getRuntimeContext(),config); }

    @Override
    // Input from source
    // Output to the coordinator
    public void processElement1(Input input, Context ctx, Collector<OptMessage> out) throws IOException {

        // Initialize worker state
        if(workerState.getWorkerState() == null){

            // Initialization
            workerState.initializeWorkerState(config.BNSchema());

            // Print worker state
            ctx.output(worker_stats,"Worker "+ctx.getCurrentKey()+" => Initialization ");
            workerState.printWorkerState(ctx);
            ctx.output(worker_stats,"Worker "+ctx.getCurrentKey()+" => "+config+"\n");

            // Throughput
            ctx.output(worker_stats,"Worker "+ctx.getCurrentKey()+" => Throughput ");
            ctx.output(worker_stats,"Worker "+ctx.getCurrentKey()+" receive first event at "+new Timestamp(System.currentTimeMillis())+" , "+System.currentTimeMillis()+"\n");

            // Timer
            workerState.setTimer(System.currentTimeMillis());
            ctx.output(worker_stats,"Worker "+ctx.getCurrentKey()+" timer"+" , "+System.currentTimeMillis()+" , total count "+workerState.getTotalCount().getCounter()+"\n");
        }

        // Check the end of stream => One solution(Write EOF to file and FlatMapperInput-completed)-done , second solution Watermark ???
        if( input.getValue().equals("EOF") ){

            // Update the end of stream counter
            workerState.setEndOfStreamCounter(workerState.getEndOfStreamCounter()+1);

            if(workerState.getEndOfStreamCounter() == config.sourcePartition()) {

                // Throughput
                ctx.output(worker_stats, "Worker " + ctx.getCurrentKey() + " receive last event at " + new Timestamp(System.currentTimeMillis()) + " , " + System.currentTimeMillis() + " , total count " + workerState.getTotalCount().getCounter() + "\n");

                // Send END_OF_STREAM message
                OptMessage endOfStream = new OptMessage(ctx.getCurrentKey(), 0, END_OF_STREAM, 0, 0);
                endOfStream.setTimestamp(System.currentTimeMillis());
                out.collect(endOfStream);

                // Collect some statistics
                System.out.println("Worker " + ctx.getCurrentKey()
                                             + " received EOF"
                                             + " , total count : " + workerState.getTotalCount().getCounter()
                                             + " , numMessages : " + workerState.getNumMessages());

                ctx.output(worker_stats, "Worker " + ctx.getCurrentKey()
                                                      + " received EOF"
                                                      + " , total count : " + workerState.getTotalCount().getCounter()
                                                      + " , numMessages : " + workerState.getNumMessages());

                // Register a processing time timer
                ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + Time.milliseconds(100L).toMilliseconds());

                // Reset the end of stream counter
                workerState.setEndOfStreamCounter(0);

            }

            return;
        }

        // Per 500 tuples, calculate the throughput
        if( workerState.getTotalCount().getCounter() % 500 == 0 ){
            ctx.output(worker_stats,"Worker "+ctx.getCurrentKey()+" receive events at "+new Timestamp(System.currentTimeMillis())+" , "+System.currentTimeMillis()+" , total count "+workerState.getTotalCount().getCounter()+"\n");
        }

        // Per 100 ms , emit the processed tuples
        if((System.currentTimeMillis() - workerState.getTimer()) >= 100L){
            ctx.output(worker_stats,"Worker "+ctx.getCurrentKey()+" timer"+" , "+workerState.getTimer()+" , total count "+workerState.getTotalCount().getCounter()+"\n");
            workerState.setTimer(System.currentTimeMillis());
        }

        // Process the input
        handleInput(workerState,ctx,input,out);
        /*ctx.output(worker_stats, "Worker "+ctx.getCurrentKey()
                                    + " => Total count : " + workerState.getTotalCount().getCounter()
                                    + " , numMessages : " + workerState.getNumMessages()+"\n");*/

    }

    @Override
    // Input from feedback source
    // When receive a new message then update the sending condition
    public void processElement2(OptMessage message, Context ctx, Collector<OptMessage> out) throws IOException {

        // Initialize worker state
        if(workerState.getWorkerState() == null){

            // Initialization
            workerState.initializeWorkerState(config.BNSchema());

            // Print worker state
            ctx.output(worker_stats,"Worker "+ctx.getCurrentKey()+" => Initialization from Messages");
            workerState.printWorkerState(ctx);
            ctx.output(worker_stats,"Worker "+ctx.getCurrentKey()+" => "+config+"\n");

            // Throughput
            ctx.output(worker_stats,"Worker "+ctx.getCurrentKey()+" => Throughput ");
            ctx.output(worker_stats,"Worker "+ctx.getCurrentKey()+" receive first event at "+new Timestamp(System.currentTimeMillis())+" , "+System.currentTimeMillis()+"\n");
        }

        // Handle messages
        handleMessage(workerState, message, ctx, out);

        /*ctx.output(worker_stats, "Worker "+ctx.getCurrentKey()
                                    + " => Total count : " + workerState.getTotalCount().getCounter()
                                    + " , numMessages : " + workerState.getNumMessages()+"\n");*/
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OptMessage> out ) throws IOException {

        // Not synchronized counters
        if( !workerState.checkCountersSync() ){
            // System.out.println("Workers : All counters are not synchronized");

            // Register a processing time timer to checked it again
            ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis()+ Time.milliseconds(100L).toMilliseconds());

            return;
        }

        // Call the subroutine process for RC and DC counters
        subroutineProcess(workerState,ctx,out);

        // Send END_OF_WORKER message
        OptMessage endOfWorker = new OptMessage(ctx.getCurrentKey(), 0, END_OF_WORKER, 0, 0);
        endOfWorker.setTimestamp(System.currentTimeMillis());
        out.collect(endOfWorker);

        // Collect some statistics about worker
        System.out.println("Worker " + ctx.getCurrentKey()
                                     + " send END_OF_WORKER "
                                     + " , total count : " + workerState.getTotalCount().getCounter()
                                     + " , numMessages : " + workerState.getNumMessages()
                                     + " , at  " + System.currentTimeMillis());

        ctx.output(worker_stats,"Worker " + ctx.getCurrentKey()
                                             + " send END_OF_WORKER "
                                             + " , total count : " + workerState.getTotalCount().getCounter()
                                             + " , numMessages : " + workerState.getNumMessages()
                                             + " , at  " + System.currentTimeMillis());

        // Collect some statistics about worker
        ctx.output(worker_stats, "Worker "+ctx.getCurrentKey()
                                             + " => Total count : " + workerState.getTotalCount().getCounter()
                                             + " , numMessages : " + workerState.getNumMessages()+"\n");

        ctx.output(worker_stats, "Worker "+ctx.getCurrentKey()+" onTimer worker !!!");
        ctx.output(worker_stats, "Worker "+ctx.getCurrentKey()+" End of process ");
        workerState.printWorkerState(ctx);


        ctx.output(worker_stats, "Worker "+ctx.getCurrentKey()
                                             + " => Total count : " + workerState.getTotalCount().getCounter()
                                             + " , numMessages : " + workerState.getNumMessages()+"\n");


    }

}