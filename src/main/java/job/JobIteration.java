package job;

import config.CBNConfig;
import config.DefaultParameters;
import coordinator.Coordinator;
import datatypes.input.Input;
import datatypes.message.OptMessage;
import operators.Assigner.SimpleAssigner;
import operators.Assigner.InputAssigner;
import operators.CustomTumblingWindow;
import operators.Mapper.MapperInput;
import operators.Source.QuerySource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import worker.Worker;

import static config.InternalConfig.coordinatorKey;
import static utils.KafkaUtils.simpleConsumer;
import static job.JobKafka.*;

public class JobIteration {

    public static final OutputTag<OptMessage> feedbackSource = new OutputTag<OptMessage>("feedbackSource"){};

    public static void main(String[] args) throws Exception {

        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set up the parameters
        ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);

        // Set parallelism( get the file path from argument0 )
        env.setParallelism(params.getInt("parallelism", DefaultParameters.parallelism));

        // Set time characteristic as Event Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Set watermark interval for event time notion to 1 millisecond
        env.getConfig().setAutoWatermarkInterval(1);

        // Make parameters available in the web interface and all rich-functions ???
        env.getConfig().setGlobalJobParameters(params);

        // Setup Configuration
        CBNConfig config = new CBNConfig(params);

        // Read from input source(ascending timestamps)
        // Return a triplet => value,worker,event_timestamp
        SingleOutputStreamOperator<Input> inputStream = env
                                                        .addSource(simpleConsumer(params)
                                                        .assignTimestampsAndWatermarks(new SimpleAssigner()).setStartFromEarliest())
                                                        .map(new MapperInput(config.workers()))
                                                        .name("Input Source");

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

        // Iterate
        IterativeStream.ConnectedIterativeStreams<Input, OptMessage> iteration = windowed.iterate().withFeedbackType(OptMessage.class);

        // Split the iteration head on input stream and message stream
        SingleOutputStreamOperator<Input> iterationInput = iteration.process(new CoProcessFunction<Input, OptMessage, Input>() {

            // Input stream
            @Override
            public void processElement1(Input input, Context ctx, Collector<Input> out) { out.collect(input); }

            // Feedback via side output
            @Override
            public void processElement2(OptMessage message, Context ctx, Collector<Input> out) { ctx.output(feedbackSource, message); }

        }).name("Iteration Head");

        // Workers
        SingleOutputStreamOperator<OptMessage> workerStream =  iterationInput
                                                               .connect(iterationInput.getSideOutput(feedbackSource))
                                                               .keyBy(Input::getKey, OptMessage::getWorker)
                                                               .process(new Worker(config))
                                                               .name("Workers");

        // Collect Workers statistics
        // filter.filterAndWriteWorkers(workerStream.getSideOutput(worker_stats),params.get("workerPath",DefaultParameters.workerPath));
        workerStream.getSideOutput(worker_stats)
                    .filter((FilterFunction<String>) input -> input.contains("Worker 0"))
                    .writeAsText("worker_stats_0.txt", FileSystem.WriteMode.OVERWRITE)
                    .setParallelism(1)
                    .name("Workers statistics");


        // Coordinator
        SingleOutputStreamOperator<OptMessage> coordinatorStream = workerStream
                                                                   .connect(querySource)
                                                                   .keyBy(message -> coordinatorKey, query -> coordinatorKey)
                                                                   .process(new Coordinator(config))
                                                                   .setParallelism(1)
                                                                   .name("Coordinator");

        // Collect Coordinator statistics
        coordinatorStream.getSideOutput(coordinator_stats)
                         .writeAsText(params.get("coordinatorPath",DefaultParameters.coordinatorPath), FileSystem.WriteMode.OVERWRITE)
                         .setParallelism(1)
                         .name("Coordinator statistics");



        // Feedback stream
        SingleOutputStreamOperator<OptMessage> feedbackStream = coordinatorStream.map(x -> x).setParallelism(4).name("Feedback");
        // returns(Types.POJO(Message.class)) ???


        // Close the iteration(iteration head) via feedbackStream
        iteration.closeWith(feedbackStream);

        // Print the execution plan
        // System.out.println(env.getExecutionPlan());

        // Trigger the execution of program(plus parameters)
        env.execute("JOB_ITERATION");
    }
}
