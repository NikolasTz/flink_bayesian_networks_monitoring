package fgm.job;

import config.DefaultParameters;
import config.FGMConfig;
import fgm.coordinator.Coordinator;
import datatypes.input.Input;
import datatypes.message.MessageFGM;
import fgm.worker.Worker;
import operators.Filter;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import static config.InternalConfig.coordinatorKey;
import static fgm.job.JobFGM.*;
import static job.JobKafka.worker_stats;
import static operators.CustomTumblingWindow.TumblingEventTimeWindow;
import static operators.Assigner.MessageFGMAssigner;
import static operators.Assigner.InputAssigner;
import static utils.KafkaUtils.*;

public class TestFGM {

    public static void main(String[] args) throws Exception {

        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set up the parameters
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
        SingleOutputStreamOperator<Input> inputStream = env
                                                        .addSource(consumerInput("sourceDFGHIJ",DefaultParameters.server,"sourceDFGHIJ-group").setStartFromEarliest())
                                                        // .readTextFile("datasets\\data_earthquake_50000").setParallelism(1)
                                                        // .flatMap(new Mapper.FlatMapperInput(config.workers())).setParallelism(1)
                                                        // .map(new MapperInput(config.workers()))
                                                        .name("Input Source");

        // Read from feedback source via kafka consumer(ascending timestamps)
        DataStream<MessageFGM> feedbackSource =  env
                                                .addSource(consumerMessageFGM(DefaultParameters.feedbackTopic, DefaultParameters.server, DefaultParameters.feedbackGroupId))
                                                .assignTimestampsAndWatermarks(new MessageFGMAssigner())
                                                .name("Feedback Source");

        // Initialization source for coordinator ???
        SingleOutputStreamOperator<Input> initSource =  env
                                                        .addSource(consumerInput("query46",DefaultParameters.server,"query46-group").setStartFromEarliest())
                                                        // .addSource(new QuerySource(config))
                                                        .assignTimestampsAndWatermarks(new InputAssigner())
                                                        .name("Query Source");

        // Windowing ???
        SingleOutputStreamOperator<Input> windowed;
        if(config.enableWindow()){

            // Windowing the inputStream before the ingress to workers
            windowed =  inputStream
                        .keyBy(Input::getKey) // Key by number of workers
                        .process(new TumblingEventTimeWindow(Time.milliseconds(config.windowSize()))); // Window's size

            // Get the late events(if exist)
            windowed.getSideOutput(late_events).print();

            // Write window statistics
            windowed.getSideOutput(window_stats).writeAsText("window_stats.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("Window statistics");

        }
        else{ windowed = inputStream; }

        // Workers
        SingleOutputStreamOperator<MessageFGM> workerStream = windowed
                                                              .connect(feedbackSource)
                                                              .keyBy(Input::getKey, MessageFGM::getWorker)
                                                              .process(new Worker(config))
                                                              .name("Workers");

        // Collect Workers statistics
        Filter filter = new Filter(config.workers());
        filter.filterAndWriteWorkers(workerStream.getSideOutput(worker_stats),params.get("workerPath",DefaultParameters.workerPath));
        /* workerStream.getSideOutput(worker_stats)
                    .writeAsText("worker_stats.txt", FileSystem.WriteMode.OVERWRITE)
                    .setParallelism(1)
                    .name("Workers statistics");*/


        // Coordinator
        SingleOutputStreamOperator<MessageFGM> coordinatorStream =  workerStream
                                                                   .connect(initSource)
                                                                   .keyBy(message -> coordinatorKey, message -> coordinatorKey)
                                                                   .process(new Coordinator(config))
                                                                   .setParallelism(1)
                                                                   .name("Coordinator");

        // Collect Coordinator statistics
        coordinatorStream.getSideOutput(JobFGM.coordinator_stats)
                         .writeAsText(params.get("coordinatorPath",DefaultParameters.coordinatorPath), FileSystem.WriteMode.OVERWRITE)
                         .setParallelism(1)
                         .name("Coordinator statistics");


        // Close the iteration,viz write to the feedback source
        coordinatorStream//.map(x -> x)
                         //.returns(Types.POJO(Message.class))
                         .addSink(producerMessageFGM(DefaultParameters.feedbackTopic,DefaultParameters.server,DefaultParameters.feedbackGroupId))
                         .name("Feedback Sink");

        // Print the execution plan
        // System.out.println(env.getExecutionPlan());

        // Trigger the execution of program(plus parameters)
        env.execute("JOB_FGM");

    }
}
