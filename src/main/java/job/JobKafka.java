package job;

import config.CBNConfig;
import config.DefaultParameters;
import coordinator.Coordinator;
import datatypes.input.Input;
import datatypes.message.OptMessage;
import operators.Assigner.SimpleAssigner;
import operators.CustomTumblingWindow;
import operators.Mapper.MapperInput;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import worker.Worker;

import static config.InternalConfig.coordinatorKey;
import static utils.KafkaUtils.*;
import static operators.Assigner.OptMessageAssigner;
import static operators.Assigner.InputAssigner;
import static operators.Source.QuerySource;

public class JobKafka {

    // Using for statistics about window,workers and coordinator
    public static final OutputTag<String> late_events = new OutputTag<String>("late_events"){};
    public static final OutputTag<String> window_stats = new OutputTag<String>("window_stats"){};
    public static final OutputTag<String> coordinator_stats = new OutputTag<String>("coordinator_stats"){};
    public static final OutputTag<String> worker_stats = new OutputTag<String>("worker_stats"){};


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
        CBNConfig config = new CBNConfig(params);

        // Read from input source(ascending timestamps)
        // Return a triplet => value,worker,event_timestamp
        // Forward to workers
        SingleOutputStreamOperator<Input> inputStream = env
                                                        .addSource(simpleConsumer(params)
                                                        .assignTimestampsAndWatermarks(new SimpleAssigner()).setStartFromEarliest())
                                                        .map(new MapperInput(config.workers()))
                                                        .name("Input Source");


        // Read from feedback source via kafka consumer(ascending timestamps)
        // Feedback stream => forwarded to workers
        DataStream<OptMessage> feedbackSource =  env
                                                .addSource(consumerOptMessage(params))
                                                .assignTimestampsAndWatermarks(new OptMessageAssigner())
                                                .name("Feedback Source");


        // Initialization source for coordinator
        SingleOutputStreamOperator<Input> querySource = env
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

        // Filter the input(testing purposes)
        // Filter filter = new Filter(config.workers()); filter.filterAndWrite(windowed);

        // Workers
        // Connect Input and Feedback source
        SingleOutputStreamOperator<OptMessage> workerStream =  windowed
                                                               .connect(feedbackSource)
                                                               .keyBy(Input::getKey, OptMessage::getWorker)
                                                               .process(new Worker(config))
                                                               .name("Workers");

        // Collect Workers statistics
        // filter.filterAndWriteWorkers(workerStream.getSideOutput(worker_stats));
        // filter.filterAndWriteWorkers(workerStream.getSideOutput(worker_stats),params.get("workerPath",DefaultParameters.workerPath));
        workerStream.getSideOutput(worker_stats)
                    .filter((FilterFunction<String>) input -> input.contains("Worker 0"))
                    .writeAsText("worker_stats_0.txt", FileSystem.WriteMode.OVERWRITE)
                    .setParallelism(1)
                    .name("Workers statistics");


        // Coordinator
        // Connect Worker stream and Query source
        SingleOutputStreamOperator<OptMessage> coordinatorStream = workerStream
                                                                   .connect(querySource)
                                                                   .keyBy(message -> coordinatorKey, query -> coordinatorKey)
                                                                   .process(new Coordinator(config))
                                                                   .setParallelism(1)
                                                                   .name("Coordinator");


        // Collect Coordinator statistics
        // filter.filterAndWriteCoordinator(coordinatorStream.getSideOutput(coordinator_stats));
        // writeAsText(params.get("coordinatorPath",DefaultParameters.coordinatorPath), FileSystem.WriteMode.OVERWRITE)
        // writeAsText(encode(params.get("coordinatorPath",DefaultParameters.coordinatorPath)), FileSystem.WriteMode.OVERWRITE)
        coordinatorStream.getSideOutput(coordinator_stats)
                         .writeAsText("coordinator_stats.txt", FileSystem.WriteMode.OVERWRITE)
                         .setParallelism(1)
                         .name("Coordinator statistics");


        // Close the iteration,viz write to the feedback source
        coordinatorStream//.map(x -> x)
                         //.returns(Types.POJO(Message.class))
                         .addSink(producerOptMessage(params))
                         .name("Feedback Sink");


        // Print the execution plan
        // System.out.println(env.getExecutionPlan());

        // Trigger the execution of program(plus parameters)
        env.execute("JOB_KAFKA");
    }

}
