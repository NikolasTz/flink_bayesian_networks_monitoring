package fgm.job;

import config.DefaultParameters;
import config.FGMConfig;
import datatypes.input.Input;
import datatypes.message.MessageFGM;
import fgm.coordinator.Coordinator;
import fgm.worker.Worker;
import operators.Assigner.MessageFGMAssigner;
import operators.Assigner.SimpleAssigner;
import operators.Assigner.InputAssigner;
import operators.CustomTumblingWindow;
import operators.Mapper.MapperInput;
import operators.Source.QuerySource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import static config.InternalConfig.coordinatorKey;
import static utils.KafkaUtils.*;

public class JobFGM {

    public static final OutputTag<String> coordinator_stats = new OutputTag<String>("coordinator_stats"){};
    public static final OutputTag<String> worker_stats = new OutputTag<String>("worker_stats"){};
    public static final OutputTag<String> late_events = new OutputTag<String>("late_events"){};
    public static final OutputTag<String> window_stats = new OutputTag<String>("window_stats"){};

    public static void main(String[] args) throws Exception {

        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set up the parameters( get the file path from argument0 )
        ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);

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
        SingleOutputStreamOperator<Input> inputStream = env
                                                        .addSource(simpleConsumer(params)
                                                        .assignTimestampsAndWatermarks(new SimpleAssigner()).setStartFromEarliest())
                                                        .map(new MapperInput(config.workers()))
                                                        .name("Input Source");

        // Read from feedback source via kafka consumer(ascending timestamps)
        DataStream<MessageFGM> feedbackSource =  env
                                                .addSource(consumerMessageFGM(params))
                                                .assignTimestampsAndWatermarks(new MessageFGMAssigner())
                                                .name("Feedback Source");

        // Initialization source for coordinator - query source
        SingleOutputStreamOperator<Input> initSource =  env
                                                        .addSource(new QuerySource(config))
                                                        .assignTimestampsAndWatermarks(new InputAssigner())
                                                        .name("Query Source");


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
        SingleOutputStreamOperator<MessageFGM> workerStream = windowed
                                                              .connect(feedbackSource)
                                                              .keyBy(Input::getKey, MessageFGM::getWorker)
                                                              .process(new Worker(config))
                                                              .name("Workers");

        // Collect Workers statistics
        // filter.filterAndWriteWorkers(workerStream.getSideOutput(worker_stats),params.get("workerPath",DefaultParameters.workerPath));
        /*workerStream.getSideOutput(worker_stats)
                    .writeAsText("worker_stats.txt", FileSystem.WriteMode.OVERWRITE)
                    .setParallelism(1)
                    .name("Workers statistics");*/


        // Coordinator
        // Connect Worker stream and Query source
        SingleOutputStreamOperator<MessageFGM> coordinatorStream =  workerStream
                                                                    .connect(initSource)
                                                                    .keyBy(message -> coordinatorKey, message -> coordinatorKey)
                                                                    .process(new Coordinator(config))
                                                                    .setParallelism(1)
                                                                    .name("Coordinator");

        // Collect Coordinator statistics
        // writeAsText(encode(params.get("coordinatorPath",DefaultParameters.coordinatorPath)), FileSystem.WriteMode.OVERWRITE)
        coordinatorStream.getSideOutput(coordinator_stats)
                         .writeAsText(params.get("coordinatorPath",DefaultParameters.coordinatorPath), FileSystem.WriteMode.OVERWRITE)
                         .setParallelism(1)
                         .name("Coordinator statistics");


        // Close the iteration,viz write to the feedback source
        coordinatorStream.addSink(producerMessageFGM(params)).name("Feedback Sink");

        // Print the execution plan
        // System.out.println(env.getExecutionPlan());

        // Trigger the execution of program(plus parameters)
        env.execute("JOB_FGM");


    }
}
