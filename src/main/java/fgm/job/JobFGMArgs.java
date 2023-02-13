package fgm.job;

import config.DefaultParameters;
import config.FGMConfig;
import datatypes.input.Input;
import datatypes.message.MessageFGM;
import fgm.coordinator.Coordinator;
import fgm.worker.Worker;
import operators.Assigner.StringAssigner;
import operators.Assigner.InputAssigner;
import operators.Assigner.QueryAssigner;
import operators.Assigner.MessageFGMAssigner;
import operators.CustomTumblingWindow;
import operators.Filter;
import operators.Mapper.FlatMapperStrToInput;
import operators.Source.ClassificationSource;
import operators.Source.QuerySource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import static config.InternalConfig.TypeNetwork.NAIVE;
import static config.InternalConfig.coordinatorKey;
import static utils.KafkaUtils.*;
import static fgm.job.JobFGM.*;

public class JobFGMArgs {

    public static void main(String[] args) throws Exception {

        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set up the parameters( get the file path from argument0 )
        ParameterTool params = ParameterTool.fromArgs(args);

        // Set parallelism
        env.setParallelism(params.getInt("parallelism", DefaultParameters.parallelism));

        // Set time characteristic as Event Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Set watermark interval for event time notion to 1 millisecond
        env.getConfig().setAutoWatermarkInterval(1);

        // Make parameters available in the web interface and all rich-functions
        env.getConfig().setGlobalJobParameters(params);

        // Setup Configuration
        FGMConfig config = new FGMConfig(params);


        // Read from input source(ascending timestamps)
        // Return a triplet => value,worker,event_timestamp
        SingleOutputStreamOperator<Input> inputStream;
        if(config.enableInputString()){
            inputStream =   env
                            .addSource(simpleConsumer(params).setStartFromEarliest())
                            // .assignTimestampsAndWatermarks(new StringAssigner()).setStartFromEarliest())
                            .setParallelism(params.getInt("sourceParallelism", params.getInt("parallelism", DefaultParameters.parallelism)))
                            .flatMap(new FlatMapperStrToInput(config.workers()))
                            .setParallelism(params.getInt("flatMapParallelism", params.getInt("parallelism", DefaultParameters.parallelism)))
                            .assignTimestampsAndWatermarks(new InputAssigner())
                            .setParallelism(params.getInt("flatMapParallelism", params.getInt("parallelism", DefaultParameters.parallelism)))
                            .name("Input Source");
        }
        else{
            inputStream =   env
                            .addSource(consumerInput(params)
                            .assignTimestampsAndWatermarks(new InputAssigner()).setStartFromEarliest())
                            .setParallelism(params.getInt("sourceParallelism", params.getInt("parallelism", DefaultParameters.parallelism)))
                            .name("Input Source");
        }


        // Read from feedback source via kafka consumer(ascending timestamps)
        DataStream<MessageFGM> feedbackSource =  env
                                                 .addSource(consumerMessageFGM(params)
                                                 .assignTimestampsAndWatermarks(new MessageFGMAssigner()))
                                                 .setParallelism(params.getInt("feedbackParallelism", params.getInt("parallelism", DefaultParameters.parallelism)))
                                                 .name("Feedback Source");


        // Initialization source for coordinator - query source
        // SingleOutputStreamOperator<Input> initSource = env.addSource(consumerQuery(params).setStartFromEarliest()).assignTimestampsAndWatermarks(new InputAssigner()).name("Query Source");
        SingleOutputStreamOperator<Input> querySource;
        if( config.typeNetwork() == NAIVE ){
            querySource = env.addSource(new ClassificationSource(config))
                             .setParallelism(params.getInt("querySourceParallelism", 1))
                             .assignTimestampsAndWatermarks(new QueryAssigner())
                             .setParallelism(params.getInt("querySourceParallelism", 1))
                             .name("Query Source");
        }
        else{
            querySource = env.addSource(consumerQuery(params) // addSource(new QuerySource(config))
                             .assignTimestampsAndWatermarks(new QueryAssigner()).setStartFromEarliest())
                             .setParallelism(params.getInt("querySourceParallelism", 1))
                             .name("Query Source");
        }


        // Windowing
        SingleOutputStreamOperator<Input> windowed;
        if(config.enableWindow()){

            // Windowing the inputStream before the ingress to workers
            windowed =  inputStream
                        .keyBy(Input::getKey) // Key by number of workers
                        .process(new CustomTumblingWindow.TumblingEventTimeWindow(Time.milliseconds(config.windowSize()))); // Window's size

            // Get the late events(if exist)
            windowed.getSideOutput(late_events).print();

            // Write window statistics
            windowed.getSideOutput(window_stats).writeAsText("window_stats.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("Window statistics");

        }
        else{ windowed = inputStream; }


        // Workers
        // Connect Input and Feedback source
        SingleOutputStreamOperator<MessageFGM> workerStream =   windowed
                                                                .connect(feedbackSource)
                                                                .keyBy(Input::getKey, MessageFGM::getWorker)
                                                                .process(new Worker(config))
                                                                .setParallelism(params.getInt("workersParallelism", params.getInt("parallelism", DefaultParameters.parallelism)))
                                                                .name("Workers");

        // Collect Workers statistics
        if(config.enableWorkerStats()){
            Filter filter = new Filter(config.workers());
            filter.filterAndWriteWorkers(workerStream.getSideOutput(worker_stats),params.get("workerPath",DefaultParameters.workerPath));
            // workerStream.getSideOutput(worker_stats).addSink(workerStatsProducer(params)).name("Worker statistics");
        }


        // Coordinator
        // Connect Worker stream and Query source
        SingleOutputStreamOperator<MessageFGM> coordinatorStream =  workerStream
                                                                    .connect(querySource)
                                                                    .keyBy(message -> coordinatorKey, message -> coordinatorKey)
                                                                    .process(new Coordinator(config))
                                                                    .setParallelism(params.getInt("coordinatorParallelism", 1))
                                                                    .name("Coordinator");

        // Collect Coordinator statistics
        coordinatorStream.getSideOutput(coordinator_stats)
                         .writeAsText(params.get("coordinatorPath",DefaultParameters.coordinatorPath), FileSystem.WriteMode.OVERWRITE)
                         .setParallelism(1)
                         .name("Coordinator statistics");

        // Rebalancing
        SingleOutputStreamOperator<MessageFGM> feedbackStream = coordinatorStream.map(x -> x)
                                                                                 .returns(TypeInformation.of(MessageFGM.class))
                                                                                 .setParallelism(params.getInt("feedbackParallelism", params.getInt("parallelism", DefaultParameters.parallelism)))
                                                                                 .name("Feedback Stream");

        // Close the iteration,viz write to the feedback source
        feedbackStream//.rebalance()
                        .addSink(producerMessageFGM(params))
                        .setParallelism(params.getInt("feedbackParallelism", params.getInt("parallelism", DefaultParameters.parallelism)))
                        .name("Feedback Sink");

        // Print the execution plan
        // System.out.println(env.getExecutionPlan());

        // Trigger the execution of program(plus parameters)
        env.execute("JOB_FGM");


    }
}
