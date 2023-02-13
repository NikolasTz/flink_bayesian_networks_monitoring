package job;

import config.CBNConfig;
import config.DefaultParameters;
import coordinator.Coordinator;
import datatypes.input.Input;
import datatypes.message.OptMessage;
import operators.Assigner.InputAssigner;
import operators.Assigner.OptMessageAssigner;
import operators.CustomTumblingWindow;
import operators.Source.QuerySource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import worker.Worker;

import static config.InternalConfig.coordinatorKey;
import static job.JobKafka.*;
import static utils.KafkaUtils.*;

public class JobKafkaArgs {

    public static void main(String[] args) throws Exception {

        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set up the parameters( get up the parameters from arguments )
        // --server localhost:9092 --inputTopic sourceDFGHIJKLMNOPQRST --inputGroupId sourceDFGHIJKLMNOPQRST-group --feedbackTopic hepar2RTUVXY --feedbackGroupId hepar2RTUVXY-group --workers 10 --parallelism 4 --eps 0.1 --delta 0.25 --windowSize 3000 --enableWindow false
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
        CBNConfig config = new CBNConfig(params);

        // Read from input source(ascending timestamps)
        // Return a triplet => value,worker,event_timestamp
        // Forward to workers
        SingleOutputStreamOperator<Input> inputStream = env
                                                        .addSource(consumerInput(params)
                                                        .assignTimestampsAndWatermarks(new InputAssigner()).setStartFromEarliest())
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
        // Filter filter = new Filter(config.workers());
        // filter.filterAndWriteWorkers(workerStream.getSideOutput(worker_stats),params.get("workerPath",DefaultParameters.workerPath));
        workerStream.getSideOutput(worker_stats).addSink(workerStatsProducer(params)).name("Worker statistics");

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
        coordinatorStream.getSideOutput(coordinator_stats).addSink(coordinatorStatsProducer(params)).name("Coordinator statistics");
        /*coordinatorStream.getSideOutput(coordinator_stats)
                         .writeAsText(params.get("coordinatorPath",DefaultParameters.coordinatorPath), FileSystem.WriteMode.OVERWRITE)
                         .setParallelism(1)
                         .name("Coordinator statistics");*/

        // Close the iteration,viz write to the feedback source
        coordinatorStream.addSink(producerOptMessage(params)).name("Feedback Sink");

        // Print the execution plan
        // System.out.println(env.getExecutionPlan());

        // Trigger the execution of program(plus parameters)
        env.execute("JOB_KAFKA");
    }
}
