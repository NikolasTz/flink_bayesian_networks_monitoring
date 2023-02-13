package job;

import config.CBNConfig;
import config.DefaultParameters;
import coordinator.Coordinator;
import datatypes.input.Input;
import datatypes.message.OptMessage;
import operators.CustomTumblingWindow.TumblingEventTimeWindow;
import operators.Filter;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import worker.Worker;

import static config.InternalConfig.coordinatorKey;
import static operators.Source.QuerySource;
import static operators.Assigner.InputAssigner;
import static operators.Assigner.OptMessageAssigner;
import static job.JobKafka.*;
import static utils.KafkaUtils.*;

public class Test {

    public static void main(String[] args) throws Exception {

        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set up the parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        // ParameterTool params = ParameterTool.fromPropertiesFile(args); // Get parameters from file

        // Set parallelism
        env.setParallelism(params.getInt("parallelism",DefaultParameters.parallelism));

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
                                                        .addSource(consumerInput("sourceHEPAR2500K16QB",DefaultParameters.server,"sourceHEPAR2500K16QB-group").setStartFromEarliest())
                                                        //.readTextFile("datasets\\data_hepar20_5000").setParallelism(1)
                                                        //.addSource(simpleConsumer(DefaultParameters.input_topic,DefaultParameters.server,DefaultParameters.input_group_id)
                                                        //.assignTimestampsAndWatermarks(new SimpleAssigner()).setStartFromEarliest())
                                                        //.assignTimestampsAndWatermarks(new SimpleAssigner())
                                                        //.map(new MapperInput(config.workers()))
                                                        // .flatMap(new Mapper.FlatMapperInput(config.workers())).setParallelism(1)
                                                        .name("Input Source");


        // Read from feedback source via kafka consumer(ascending timestamps)
        DataStream<OptMessage> feedbackSource =  env
                                                .addSource(consumerOptMessage(DefaultParameters.feedbackTopic, DefaultParameters.server, DefaultParameters.feedbackGroupId))
                                                .assignTimestampsAndWatermarks(new OptMessageAssigner())
                                                .name("Feedback Source");


        // Initialization source for coordinator
        SingleOutputStreamOperator<Input> initSource = env
                                                       // .addSource(consumerInput("query2",DefaultParameters.server,"query2-group").setStartFromEarliest())
                                                       .addSource(new QuerySource(config))
                                                       .assignTimestampsAndWatermarks(new InputAssigner())
                                                       .name("Query Source");

        // Print initSource
        // initSource.print().setParallelism(1);
        // Filter filterInit = new Filter(config.workers()); filterInit.filterAndWriteQuerySource(initSource);

        // Windowing
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

        // Filter the input(testing purposes)
        // Filter filter = new Filter(config.workers()); filter.filterAndWrite(windowed);
        // filter.filterAndWrite(workerStream.getSideOutput(worker_stats));

        // Workers
        SingleOutputStreamOperator<OptMessage> workerStream =  windowed
                                                               .connect(feedbackSource)
                                                               .keyBy(Input::getKey, OptMessage::getWorker)
                                                               .process(new Worker(config))
                                                               .name("Workers");

        // Collect Workers statistics
        Filter filter = new Filter(config.workers());
        // filter.filterAndWrite(windowed); // Collect window statistics
        // filter.filterAndWriteWorkers(workerStream.getSideOutput(worker_stats)); // Collect worker statistics
        filter.filterAndWriteWorkers(workerStream.getSideOutput(worker_stats),params.get("workerPath",DefaultParameters.workerPath));
        /*workerStream.getSideOutput(worker_stats)
                    .filter((FilterFunction<String>) input -> input.contains("Worker 0"))
                    .writeAsText("worker_stats_0.txt", FileSystem.WriteMode.OVERWRITE)
                    .setParallelism(1)
                    .name("Workers statistics");*/


        // Coordinator
        SingleOutputStreamOperator<OptMessage> coordinatorStream = workerStream
                                                                   .connect(initSource)
                                                                   .keyBy(message -> coordinatorKey, message -> coordinatorKey)
                                                                   .process(new Coordinator(config))
                                                                   .setParallelism(1)
                                                                   .name("Coordinator");

        // Collect Coordinator statistics
        // filter.filterAndWriteCoordinator(coordinatorStream.getSideOutput(coordinator_stats));
        coordinatorStream.getSideOutput(coordinator_stats)
                         .writeAsText(params.get("coordinatorPath",DefaultParameters.coordinatorPath), FileSystem.WriteMode.OVERWRITE)
                         .setParallelism(1)
                         .name("Coordinator statistics");


        // Close the iteration,viz write to the feedback source
        coordinatorStream//.map(x -> x)
                         //.returns(Types.POJO(Message.class))
                         .addSink(producerOptMessage(DefaultParameters.feedbackTopic,DefaultParameters.server,DefaultParameters.feedbackGroupId))
                         .name("Feedback Sink");


        // Print the execution plan
        // System.out.println(env.getExecutionPlan());

        // Trigger the execution of program(plus parameters)
        env.execute("JOB_KAFKA");
    }


}
