package distcounter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import utils.KafkaUtils;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class DistributedCounter_2WD {

    // Using for statistics about window,workers and coordinator
    private static final OutputTag<String> lateEvents = new OutputTag<String>("late-events"){};
    private static final OutputTag<String> window_stats = new OutputTag<String>("window_stats"){};
    private static final OutputTag<String> coordinator_stats = new OutputTag<String>("coordinator_stats"){};
    private static final OutputTag<String> worker_stats = new OutputTag<String>("worker_stats"){};


    public static void main(String[] args) throws Exception {

        // A simple distributed deterministic counter with 2-way communication between coordinator and workers

        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set up the parameters
        ParameterTool params = ParameterTool.fromArgs(args);

        // Set parallelism
        env.setParallelism(4);

        // Set time characteristic as Event Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Set watermark interval for event time notion to 1 millisecond
        env.getConfig().setAutoWatermarkInterval(1);

        // Make parameters available in the web interface and all rich-functions
        env.getConfig().setGlobalJobParameters(params);

        // Read from input source(ascending timestamps)
        // Return a triplet => value,worker,event_timestamp
        DataStream<Tuple3<String,Integer,Long>> inputStream = env
                                                              .addSource(KafkaUtils.simpleConsumer(DefaultParameters.input_topic,DefaultParameters.server,DefaultParameters.input_group_id)
                                                              .assignTimestampsAndWatermarks(new SimpleAssigner()).setStartFromEarliest())
                                                              .map(new Mapper())
                                                              .name("Input Source");


        // Read from feedback source via kafka consumer(ascending timestamps)
        // setStartFromEarliest !!!
        DataStream<Tuple3<String, Integer, Long>> feedbackSource =  env
                                                                    .addSource(KafkaUtils.consumerWithKafkaDeserTs(DefaultParameters.feedback_topic, DefaultParameters.server, DefaultParameters.feedback_group_id))
                                                                    .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Integer, Long>>() {
                                                                        @Override
                                                                        public Watermark getCurrentWatermark() { return new Watermark(Long.MAX_VALUE); }

                                                                        @Override
                                                                        public long extractTimestamp(Tuple3<String, Integer, Long> input, long l) { return input.f2; }
                                                                    })
                                                                    .name("Feedback Source");


        // Initialization source for coordinator
        // setStartFromEarliest !!!
        SingleOutputStreamOperator<Tuple2<String, Integer>> initSource = env
                                                                        //.addSource(consumerWithKafkaDeser(DefaultParameters.init_input_topic, DefaultParameters.server, DefaultParameters.init_group_id))
                                                                        .addSource(new SourceFunction<Tuple2<String, Integer>>() {

                                                                            @Override
                                                                            public void run(SourceContext<Tuple2<String, Integer>> sourceCtx) {
                                                                                sourceCtx.collect(new Tuple2<>("Init",0));
                                                                            }

                                                                            @Override
                                                                            public void cancel() { }
                                                                        })
                                                                        .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Integer>>(){

                                                                            @Override
                                                                            public long extractTimestamp(Tuple2<String, Integer> input, long l) { return 0; }

                                                                            @Override
                                                                            public Watermark getCurrentWatermark() { return new Watermark(Long.MIN_VALUE); }
                                                                        })
                                                                        .name("Init Source");

        // Print initSource
        // initSource.print().setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> windowed;
        if(DefaultParameters.enable_window){

            // Windowing the inputStream before the ingress to workers
            windowed =  inputStream
                        .keyBy(1) // Key by number of workers
                        .process(new TumblingEventTimeWindow(Time.milliseconds(DefaultParameters.window_size))); // Window's size

            // Get the late events(if exist)
            windowed.getSideOutput(lateEvents).print();

            // Write window statistics
            windowed.getSideOutput(window_stats).writeAsText("window_stats.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("Window statistics");

        }
        else{
            windowed = (SingleOutputStreamOperator<Tuple3<String, Integer, Long>>) inputStream;
        }

        // Workers
        SingleOutputStreamOperator<Tuple2<String, Integer>> workerStream =  windowed.connect(feedbackSource).keyBy(1, 1).process(new Worker()).name("Workers");

        // Write worker statistics
        //workerStream.getSideOutput(worker_stats).writeAsText("worker_stats.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("Workers statistics");
        workerStream.getSideOutput(worker_stats).filter((FilterFunction<String>) input -> input.contains("(0)")).writeAsText("worker_stats_0.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("Workers statistics");
        workerStream.getSideOutput(worker_stats).filter((FilterFunction<String>) input -> input.contains("(1)")).writeAsText("worker_stats_1.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("Workers statistics");
        workerStream.getSideOutput(worker_stats).filter((FilterFunction<String>) input -> input.contains("(2)")).writeAsText("worker_stats_2.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("Workers statistics");
        workerStream.getSideOutput(worker_stats).filter((FilterFunction<String>) input -> input.contains("(3)")).writeAsText("worker_stats_3.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("Workers statistics");

        // Coordinator
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> coordinatorStream = workerStream.connect(initSource).keyBy(input -> "0", input -> "0").process(new Coordinator()).setParallelism(1).name("Coordinator");

        // Write coordinator statistics
        coordinatorStream.getSideOutput(coordinator_stats).writeAsText("coordinator_stats.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("Coordinator statistics");

        // Close the iteration,viz write to the feedback source
        coordinatorStream//.map(x -> x)
                         //.returns(Types.TUPLE(Types.STRING,Types.INT,Types.LONG))
                         .addSink(KafkaUtils.producerWithKafkaSerTs(DefaultParameters.feedback_topic,DefaultParameters.server,DefaultParameters.feedback_group_id))
                         .name("Feedback Sink");


        // Print the execution plan
        // System.out.println(env.getExecutionPlan());


        // Trigger the execution of program
        env.execute("DC_2WD");


    }


    // *************************************************************************
    //                              USER FUNCTIONS
    // *************************************************************************


    /**
     * Mapper which generate a triplet with first field correspond to value,second field correspond to worker and third field correspond to event timestamp
     */
    public static final class Mapper extends RichMapFunction<String, Tuple3<String,Integer,Long>> {

        private transient int parallelism;

        public void open(Configuration config){

            // Get the parallelism
            parallelism =  getRuntimeContext().getExecutionConfig().getParallelism();
        }

        @Override
        public Tuple3<String,Integer,Long> map(String input) {

            String[] elements = input.split(",");
            return new Tuple3<>(elements[0], new Random().nextInt(parallelism),Long.parseLong(elements[1]));
        }

    }

    // Hand's God
    public static class AssignerTimestampWatermark implements AssignerWithPeriodicWatermarks<String>{

        // Current maximum timestamp
        private long currentMaxTimestamp; // Started timestamp,1627851824578L;

        // Some variables
        private long lastEmittedWatermark;
        private long lastEndOfWindow;
        private long lastCurrentProcessingTime;
        private boolean changed;
        private LinkedList<Long> endsOfWindows;

        private boolean updated = false; // For smoothness watermarking(all windows)
        private long threshold = 1000; // Threshold waiting for emitting watermarks

        // Constructors
        public AssignerTimestampWatermark(){

            currentMaxTimestamp = 1627851824578L;
            lastEmittedWatermark = 0L;
            lastEndOfWindow = (currentMaxTimestamp - (currentMaxTimestamp % DefaultParameters.window_size) + DefaultParameters.window_size - 1);
            lastCurrentProcessingTime = System.currentTimeMillis();
            changed = false;
            endsOfWindows = new LinkedList<>();
        }

        public AssignerTimestampWatermark(long startedTs,long window_size){

            currentMaxTimestamp = startedTs;
            lastEmittedWatermark = 0L;
            lastEndOfWindow = (currentMaxTimestamp - (currentMaxTimestamp % window_size) + window_size - 1);
            lastCurrentProcessingTime = System.currentTimeMillis();
            changed = false;
            endsOfWindows = new LinkedList<>();

        }

        @Override
        public long extractTimestamp(String input, long l) {

            // Update currentMaxTimestamp
            currentMaxTimestamp = Math.max(Long.parseLong(input.split(",")[1]),currentMaxTimestamp);

            // Compute end of window
            long endOfWindow = (currentMaxTimestamp - (currentMaxTimestamp % DefaultParameters.window_size) + DefaultParameters.window_size - 1);

            // Add to endsOfWindows if does not exist
            if( !endsOfWindows.contains(endOfWindow) ) endsOfWindows.add(endOfWindow);

            return currentMaxTimestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {

            long  currentTime = System.currentTimeMillis();

            // Change "null" watermark when received all elements for a specific window and keep it for a specific amount of milliseconds
            // Compute the end of window using currentMaxTimestamp
            long endOfWindow = (currentMaxTimestamp - (currentMaxTimestamp % DefaultParameters.window_size) + DefaultParameters.window_size - 1);

            // If changed the end of window
            if( endOfWindow > lastEndOfWindow && !changed ){

                // Update the last emitted watermark
                lastEmittedWatermark = lastEndOfWindow;

                // Update end of window
                lastEndOfWindow = endOfWindow;

                // Update current processing time
                lastCurrentProcessingTime = currentTime;

                // Update the changed
                changed = true;

            }
            else if ( changed ){

                // Keep the same watermark for a specific amount of processing time after the change
                if( currentTime - lastCurrentProcessingTime >= threshold ){

                    // Reduce the threshold
                    // threshold = (threshold - 100) > 0 ? (threshold - 100) : 0 ;

                    // Update the changed
                    changed = false;
                }

            }
            else{
                // Burst emission watermark mode for last windows(without updated,with updated => smoothness)
                if( lastEmittedWatermark != 0){
                    // Pop all timestamps which smaller than lastEmittedWatermark and update lastEmittedWatermark
                    long lastTs = 0L;
                    if( endsOfWindows.size() != 0 && !updated ) {
                        while (true) {
                            if (endsOfWindows.peek() <= lastEmittedWatermark) {

                                // Last end of window
                                if( endsOfWindows.size() == 1) lastTs = endsOfWindows.peek();

                                // Pop the timestamp
                                endsOfWindows.poll();

                                // Empty list => break the loop
                                if( endsOfWindows.size() == 0) break;
                            }
                            else break;
                        }
                        lastEmittedWatermark = lastTs == 0 ? endsOfWindows.peek() : lastTs;

                        // Update lastCurrentProcessingTime,updated
                        lastCurrentProcessingTime = currentTime;
                        updated = true;
                    }
                    else{
                        // Change on watermark then stay for specific amount of milliseconds(e.g. 1000 ms) => smoothness,more latency
                        if( currentTime - lastCurrentProcessingTime >= 1000 ){ updated=false; }
                    }
                }
            }

            // Emit a new Watermark
            return new Watermark(lastEmittedWatermark);
        }


    }

    // Generate all watermarks and emitted with delay
    public static class GeneratedWatermarks implements AssignerWithPeriodicWatermarks<String>{

        // Current maximum timestamp
        private long currentMaxTimestamp; // Started timestamp,1627851824578L;

        // Some variables
        private final long startTs;
        private final long endTs;
        private long lastEmittedWatermark;
        private long lastCurrentProcessingTime;
        private boolean changed;
        private final LinkedList<Long> endsOfWindows;
        private boolean first;

        // Constructor
        public GeneratedWatermarks(){

            currentMaxTimestamp = 1627851824578L;
            startTs = 1627851824578L;
            endTs = 1627851874568L;

            lastEmittedWatermark = 0L;
            lastCurrentProcessingTime = System.currentTimeMillis();
            changed = false;
            first = true;
            endsOfWindows = new LinkedList<>();
        }

        public GeneratedWatermarks(long start,long end){

            currentMaxTimestamp = start;
            startTs = start;
            endTs = end;
            lastEmittedWatermark = 0L;
            lastCurrentProcessingTime = System.currentTimeMillis();
            changed = false;
            first = true;
            endsOfWindows = new LinkedList<>();
        }

        @Override
        public long extractTimestamp(String input, long l) {

            // Update currentMaxTimestamp
            currentMaxTimestamp = Math.max(Long.parseLong(input.split(",")[1]),currentMaxTimestamp);

            return currentMaxTimestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {

            long  currentTime = System.currentTimeMillis();

            // Generate all watermarks for all windows the first time
            if(first){

                // Find the end of all windows
                long end = (endTs - (endTs % DefaultParameters.window_size) + DefaultParameters.window_size - 1);
                long start = (startTs - (startTs % DefaultParameters.window_size) + DefaultParameters.window_size - 1);

                // Add the timestamp 0
                endsOfWindows.add(0L);

                while(start <= end){
                    endsOfWindows.add(start);
                    start+=DefaultParameters.window_size;
                }

                // Update first
                first = false;
            }

            if( endsOfWindows.size() > 0 ){

                if(!changed){

                    // Update the watermark
                    lastEmittedWatermark = endsOfWindows.poll();

                    // Update lastCurrentProcessingTime
                    lastCurrentProcessingTime = currentTime;

                    // Update the changed
                    changed = true;

                }
                else{
                    if( currentTime - lastCurrentProcessingTime >= 1000 ){
                        // Update the changed
                        changed = false;
                    }
                }

            }

            // Emit a new Watermark
            return new Watermark(lastEmittedWatermark);
        }

    }

    // Some assigner for input source
    public static class SimpleAssigner implements AssignerWithPeriodicWatermarks<String>{

        private long currentMaxTimestamp;

        // Constructors
        public SimpleAssigner(){ this.currentMaxTimestamp = 0L; }

        public SimpleAssigner(Long startedTimestamp){ this.currentMaxTimestamp = startedTimestamp; }

        @Override
        public Watermark getCurrentWatermark() { return new Watermark(currentMaxTimestamp); }

        @Override
        public long extractTimestamp(String input, long l) {
            currentMaxTimestamp = Math.max(Long.parseLong(input.split(",")[1]), currentMaxTimestamp);
            return currentMaxTimestamp;
        }
    }

    // Ascending Assigner
    public static class SimpleAscendingAssigner extends AscendingTimestampExtractor<String> {
        @Override
        public long extractAscendingTimestamp(String input) { return Long.parseLong(input.split(",")[1]); }
    }

    // Event time tumbling window
    public static final class TumblingEventTimeWindow extends KeyedProcessFunction<Tuple,Tuple3<String,Integer,Long>,Tuple3<String,Integer,Long>> {

        // Window size to milliseconds
        private final long windowSizeMsec;

        // Keyed, managed state, with an entry for each window, keyed by the window's end time
        // MapState => key correspond to window's end time and value correspond to elements which belongs to this event time window
        private transient MapState<Long, List<Tuple3<String,Integer,Long>>> window_state;

        // Constructor
        public TumblingEventTimeWindow(Time windowSize){ this.windowSizeMsec = windowSize.toMilliseconds(); }

        @Override
        public void open(Configuration config){

            // Initialize the descriptor for window state

            // Map state
            MapStateDescriptor<Long, List<Tuple3<String,Integer,Long>>> mapDesc =  new MapStateDescriptor<>("window_state", TypeInformation.of(Long.class), Types.LIST(Types.TUPLE(Types.STRING,Types.INT,Types.LONG)) );

            window_state = getRuntimeContext().getMapState(mapDesc);
        }

        @Override
        public void processElement(Tuple3<String, Integer, Long> input, Context ctx, Collector<Tuple3<String, Integer, Long>> out) throws Exception {


            // Get a time service
            TimerService timerService = ctx.timerService();

            // Find the end of window for this input
            long endOfWindow = (input.f2 - (input.f2 % windowSizeMsec) + windowSizeMsec - 1);

            // Late elements => collect to side output
            if( input.f2 < timerService.currentWatermark() ){

                System.out.println("Late element , timestamp :"+ new Timestamp(input.f2));

                // Emit late element to side output
                ctx.output(lateEvents,"Late element , timestamp :"+ new Timestamp(input.f2));
            }
            else{

                // Register a timer/callback for when the window has been completed
                ctx.timerService().registerEventTimeTimer(endOfWindow);

                List<Tuple3<String, Integer, Long>> list;

                // Get the window state of input
                if( window_state.get(endOfWindow) == null ){

                    // Initialize the list for this key
                    list = new ArrayList<>();

                }
                else{ list = window_state.get(endOfWindow); }

                // Add element to list and change the timestamp to end_window
                input.f2 = endOfWindow;
                list.add(input);

                // Update the state
                window_state.put(endOfWindow,list);

            }

            // Current processing time
            long currentTimestamp = System.currentTimeMillis();

            // Emit to side output
            ctx.output(window_stats,"Window-Worker " + ctx.getCurrentKey()
                                    + " , timestamp_input : " + new Timestamp(input.f2)
                                    + " , endOfWindow : " + new Timestamp(endOfWindow)
                                    + " , ctx_timestamp : " + new Timestamp(ctx.timestamp() == null ? 0 : ctx.timestamp())
                                    + " , current_processing_time : "+ new Timestamp(timerService.currentProcessingTime())
                                    + " , current watermark : "+ new Timestamp(timerService.currentWatermark())
                                    + " at " + new Timestamp(currentTimestamp));



        }

        @Override
        public void onTimer(long timestamp,OnTimerContext ctx,Collector<Tuple3<String, Integer, Long>> out) throws Exception {

            // Get the state correspond to this timestamp
            List<Tuple3<String, Integer, Long>> window_res = window_state.get(timestamp);

            // Emit the results
            for (Tuple3<String, Integer, Long> tuple : window_res) { out.collect(tuple); }

            // Remove window
            window_state.remove(timestamp);

            // Get a timer service
            TimerService timerService = ctx.timerService();

            // Current processing time
            long currentTimestamp = System.currentTimeMillis();

            ctx.output(window_stats,"\nOn timer-Worker "   + ctx.getCurrentKey()
                    + " , timestamp : " + new Timestamp(timestamp)
                    + " , ctx_timestamp : " + new Timestamp(ctx.timestamp() == null ? 0 : ctx.timestamp())
                    + " , current_processing_time : " + new Timestamp(timerService.currentProcessingTime())
                    + " , current watermark : " + new Timestamp(timerService.currentWatermark())
                    + " , window's record size : " + window_res.size()
                    + " at " + new Timestamp(currentTimestamp)+"\n");

        }
    }

    // Workers
    public static final class Worker extends KeyedCoProcessFunction<Tuple,Tuple3<String, Integer, Long>,Tuple3<String, Integer, Long>,Tuple2<String, Integer>> {

        // The state of each worker , just a simple counter
        private transient ValueState<Long> counter;

        // Condition for send message to coordinator
        private transient ValueState<Integer> condition; // Initialize inside open or with message

        // Total count worker
        private transient ValueState<Long> total_count;

        // Last timestamp
        private transient ValueState<Long> last_ts;

        // Number of messages
        private transient ValueState<Long> num_messages;

        // Sync
        private transient ValueState<Boolean> sync; // Not used

        // Epsilon
        private final double epsilon = DefaultParameters.eps; // Initialize from properties/configuration

        // Number of workers
        private final int workers = DefaultParameters.num_workers;  // Initialize from properties/configuration

        @Override
        // Initialize state
        // Get parameters from config
        public void open(Configuration config) {

            // Initialize the descriptor of each state

            // Counter
            ValueStateDescriptor<Long> valDesc = new ValueStateDescriptor<>("counter", Long.class);
            counter = getRuntimeContext().getState(valDesc);

            // Condition
            ValueStateDescriptor<Integer> valDesc_con = new ValueStateDescriptor<>("condition", Integer.class);
            condition = getRuntimeContext().getState(valDesc_con);

            // Total count
            ValueStateDescriptor<Long> valDesc_total = new ValueStateDescriptor<>("total_count", Long.class);
            total_count = getRuntimeContext().getState(valDesc_total);

            // Last timestamp
            ValueStateDescriptor<Long> valDesc_ts = new ValueStateDescriptor<>("last_ts", Long.class);
            last_ts = getRuntimeContext().getState(valDesc_ts);

            // Number of messages
            ValueStateDescriptor<Long> valDesc_messages = new ValueStateDescriptor<>("num_messages",Long.class);
            num_messages = getRuntimeContext().getState(valDesc_messages);

            // Sync
            ValueStateDescriptor<Boolean> valDesc_sync = new ValueStateDescriptor<>("sync",Boolean.class);
            sync = getRuntimeContext().getState(valDesc_sync);

        }

        @Override
        // Input from source
        // Output to the coordinator
        public void processElement1(Tuple3<String, Integer, Long> input, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

            // Initialize the state of worker
            if (counter.value() == null) {

                // Initialize the state of worker
                counter.update(0L);

                // Initialize the value of condition
                condition.update(DefaultParameters.condition);

                // Initialize the total count
                total_count.update(0L);

                // Initialize the last timestamp
                last_ts.update(input.f2);

                // Initialize the number of messages
                num_messages.update(0L);

                // Initialize sync
                sync.update(true);
            }

            // Update the value of counter
            counter.update(counter.value()+1L);

            // Update the total count
            total_count.update(total_count.value()+1L);

            // if the timestamp has changed or counter >= ((epsilon*condition)/workers)
            if( input.f2.longValue() != last_ts.value() || counter.value() >= ((epsilon*condition.value())/workers) ){

                // Update the timestamp
                last_ts.update(input.f2);

                // Check the condition for sending message to coordinator && sync.value()
                if( counter.value() >= ((epsilon*condition.value())/workers) ){

                    // Send the message to coordinator(worker,counter_value) - local drift
                    out.collect(new Tuple2<>("INCREMENT,"+ctx.getCurrentKey().getField(0).toString(),counter.value().intValue()));

                    // Reset the counter
                    counter.update(0L);

                    // Update the number of messages
                    num_messages.update(num_messages.value()+1L);
                }

                // Print some statistics
                ctx.output(worker_stats, "\nWorker " + ctx.getCurrentKey()
                                            + " , send a message to coordinator "
                                            + " , type_message : " + "INCREMENT"
                                            + " , condition_value : " + condition.value()
                                            + " , counter : " + counter.value()
                                            + " , total_count : " + total_count.value()
                                            + " , num_messages : " + num_messages.value()
                                            + " , sync : " + sync.value()
                                            + " at " + new Timestamp(System.currentTimeMillis())+"\n");

            }


            // Assign a timer service
            TimerService timerService = ctx.timerService();

            // Current processing time
            long currentTimestamp = System.currentTimeMillis();

            ctx.output(worker_stats,"Worker " + ctx.getCurrentKey()
                                        + " , last_ts : " + new Timestamp(last_ts.value())
                                        + " , timestamp_input : " + new Timestamp(input.f2)
                                        + " , ctx_timestamp : " + new Timestamp(ctx.timestamp() == null ? 0 : ctx.timestamp())
                                        + " , current_processing_time : " + new Timestamp(timerService.currentProcessingTime())
                                        + " , current watermark : " + new Timestamp(timerService.currentWatermark())
                                        + " , condition_value : " + condition.value()
                                        + " , counter : " + counter.value()
                                        + " , total_count : " + total_count.value()
                                        + " , num_messages : " + num_messages.value()
                                        + " , sync : " + sync.value()
                                        + " at " + new Timestamp(currentTimestamp));


        }

        @Override
        // Input from feedback source
        // When receive a new message then update the sending condition
        public void processElement2(Tuple3<String, Integer, Long> input, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

            // Messages from coordinator

            // Update the last broadcast value
            if( input.f0.split(",")[0].equals("UPDATE") ){

                // New round begins
                sync.update(true);

                // Update condition
                condition.update(Integer.valueOf(input.f0.split(",")[1]));

            }
            // Send local counter
            else{

                // Send the local counter
                out.collect(new Tuple2<>("COUNTER,"+ctx.getCurrentKey().getField(0).toString(),total_count.value().intValue()));

                // Update the number of messages
                num_messages.update(num_messages.value()+1L);

                // Update sync
                sync.update(false);

                // Reset the counter/ni - local drift
                counter.update(0L);
            }


            // Assign a timer service
            TimerService timerService = ctx.timerService();

            // Current processing time
            long currentTimestamp = System.currentTimeMillis();

            // Print some statistics
            ctx.output(worker_stats,"\nReceived message , Worker " + ctx.getCurrentKey()
                                        + " , type_message : " + input
                                        + " , timestamp_input : " + new Timestamp(input.f2)
                                        + " , ctx_timestamp : " + new Timestamp(ctx.timestamp() == null ? 0 : ctx.timestamp())
                                        + " , current_processing_time : "+ new Timestamp(timerService.currentProcessingTime())
                                        + " , current watermark : " + new Timestamp(timerService.currentWatermark())
                                        + " , condition_value : " + condition.value()
                                        + " , counter : " + counter.value()
                                        + " , total_count : " + total_count.value()
                                        + " , num_messages : " + num_messages.value()
                                        + " , sync : " + sync.value()
                                        + " at " + new Timestamp(currentTimestamp)+"\n");

        }

        // On-timer
        //@Override
        //public void onTimer(long timestamp,OnTimerContext ctx,Collector<Tuple2<String, Integer>> out ){ }

    }

    // Coordinator
    public static final class Coordinator extends CoProcessFunction<Tuple2<String, Integer>,Tuple2<String, Integer>,Tuple3<String, Integer, Long>> {

        // Last broadcast condition value
        private transient ValueState<Integer> last_broadcast_cond;

        // Last broadcast tmp condition value
        private transient ValueState<Integer> last_broadcast_tmp;

        // Number of workers
        private final int num_workers = DefaultParameters.num_workers;

        // Number of messages to coordinator
        private transient ValueState<Long> num_messages;

        // Number of rounds
        private transient ValueState<Integer> num_round;

        // Number of messages sent from coordinator
        private transient ValueState<Long> num_messages_sent;

        // Estimator of counter
        private transient ValueState<Long> estimator;

        // Sync
        private transient ValueState<Boolean> sync_cord;

        // Number of messages lost
        private transient ValueState<Long> overhead_messages;


        @Override
        // Initialize state
        // Get parameters from config
        public void open(Configuration config) {

            // Initialize the descriptor of coordinator state

            // Last broadcast value
            ValueStateDescriptor<Integer> valDesc_broad = new ValueStateDescriptor<>("last_broadcast_cond", Integer.class);
            last_broadcast_cond = getRuntimeContext().getState(valDesc_broad);

            // Last broadcast value tmp
            ValueStateDescriptor<Integer> valDesc_broad_tmp = new ValueStateDescriptor<>("last_broadcast_tmp", Integer.class);
            last_broadcast_tmp = getRuntimeContext().getState(valDesc_broad_tmp);

            // Number of messages
            ValueStateDescriptor<Long> valDesc_mess = new ValueStateDescriptor<>("num_messages", Long.class);
            num_messages = getRuntimeContext().getState(valDesc_mess);

            // Counter new round
            ValueStateDescriptor<Integer> valDesc_num_round = new ValueStateDescriptor<>("num_round", Integer.class);
            num_round = getRuntimeContext().getState(valDesc_num_round);

            // Number of messages sent from coordinator
            ValueStateDescriptor<Long> valDesc_sent = new ValueStateDescriptor<>("num_messages_sent", Long.class);
            num_messages_sent = getRuntimeContext().getState(valDesc_sent);

            // Estimator
            ValueStateDescriptor<Long> valDesc_est = new ValueStateDescriptor<>("estimator", Long.class);
            estimator = getRuntimeContext().getState(valDesc_est);

            // Initialize sync
            ValueStateDescriptor<Boolean> valDesc_syncC = new ValueStateDescriptor<>("sync_cord", Boolean.class);
            sync_cord = getRuntimeContext().getState(valDesc_syncC);

            // Initialize sync
            ValueStateDescriptor<Long> valDesc_overhead = new ValueStateDescriptor<>("overhead_messages", Long.class);
            overhead_messages = getRuntimeContext().getState(valDesc_overhead);

        }


        @Override
        // Input for workers
        // Output to feedback-source
        public void processElement1(Tuple2<String, Integer> input, Context ctx, Collector<Tuple3<String, Integer, Long>> out) throws Exception {

            // if received num_workers messages , then send message to workers with new condition

            // Initialization
            if( num_messages.value() == null ){

                // Initialize the number of messages,number of round,last_broadcast_cond,num_messages_sent
                num_messages.update(0L);
                num_round.update(0);
                num_messages_sent.update(0L);
                estimator.update((long) DefaultParameters.condition);
                last_broadcast_cond.update(DefaultParameters.condition);
                last_broadcast_tmp.update(DefaultParameters.condition);
                sync_cord.update(true);
                overhead_messages.update(0L);
            }

            // Overhead from additional messages
            if( input.f0.split(",")[0].equals("INCREMENT") && ( !sync_cord.value() || (int)((DefaultParameters.eps*last_broadcast_cond.value())/num_workers)+1 != input.f1 ) ){

                // Update the overhead
                overhead_messages.update(overhead_messages.value()+1L);

                // Update the estimator
                if(!sync_cord.value()) estimator.update(estimator.value()+input.f1);
            }

            // Check the type of message
            // INCREMENT
            if( input.f0.split(",")[0].equals("INCREMENT") && sync_cord.value() ){

                // Update the number of messages
                num_messages.update(num_messages.value()+1L);

                // Update the estimator
                estimator.update(estimator.value()+input.f1);

                // Check the condition
                if( num_messages.value() == num_workers ){

                    // Send message to workers to receive the counters
                    for (int i=0;i<num_workers;i++){ out.collect(new Tuple3<>("COUNTER",i,ctx.timestamp())); }

                    // Update the number of messages sent from coordinator
                    num_messages_sent.update(num_messages_sent.value()+num_workers);

                    // Reset the number of messages
                    num_messages.update(0L);

                    // Update the sync and wait for local counter
                    sync_cord.update(false);

                    // Reset last broadcast value waiting the messages from workers and updated when received the messages from workers
                    last_broadcast_tmp.update(DefaultParameters.condition);

                    // Print some statistics to side output
                    ctx.output(coordinator_stats,"\nSend message to workers "
                                                    + " , type_message : " + "COUNTER"
                                                    + " , last_broadcast_cond : " + last_broadcast_cond.value()
                                                    + " , num_messages : " + num_messages.value()
                                                    + " , sync : " + sync_cord.value()
                                                    + " at " + new Timestamp(System.currentTimeMillis())+"\n");

                }

            }
            // UPDATE
            else if ( input.f0.split(",")[0].equals("COUNTER") ){

                // Update the value for broadcast
                last_broadcast_tmp.update(last_broadcast_tmp.value()+input.f1);

                // Update the number of messages
                num_messages.update(num_messages.value()+1L);

                // Check the condition
                if( num_messages.value() == num_workers ){

                    // Update the last broadcast value
                    last_broadcast_cond.update(last_broadcast_tmp.value());

                    // Update the estimator
                    estimator.update(last_broadcast_cond.value().longValue());

                    // Send message to workers
                    for (int i=0;i<num_workers;i++){ out.collect(new Tuple3<>("UPDATE,"+last_broadcast_cond.value(),i,ctx.timestamp())); }

                    // Update the number of messages sent from coordinator
                    num_messages_sent.update(num_messages_sent.value()+num_workers);

                    // Reset the number of messages
                    num_messages.update(0L);

                    // Update the number of round
                    num_round.update(num_round.value()+1);

                    // Start a new round,update sync
                    sync_cord.update(true);

                    // Print some statistics to side output
                    ctx.output(coordinator_stats,"\nReceived and send messages from/to workers "
                                                    + " , new round begins "
                                                    + " , type_message_rec : " + input
                                                    + " , type_message_sent : " + "UPDATE"
                                                    + " , last_broadcast_cond : " + last_broadcast_cond.value()
                                                    + " , num_messages : " + num_messages.value()
                                                    + " , num_round : " + num_round.value()
                                                    + " , sync : " + sync_cord.value()
                                                    + " at " + new Timestamp(System.currentTimeMillis())+"\n");
                }


            }

            // Assign a timer service
            TimerService timerService = ctx.timerService();

            // Current timestamp
            long currentTimestamp = System.currentTimeMillis();

            // Context timestamp correspond to last sent timestamp of message
            System.out.println("Coordinator "
                                + " , ctx_timestamp : " + new Timestamp(ctx.timestamp() == null ? 0 : ctx.timestamp())
                                + " , current_processing_time : " + new Timestamp(timerService.currentProcessingTime())
                                + " , current watermark : " + new Timestamp(timerService.currentWatermark())
                                + " , last_broadcast_cond : " + last_broadcast_cond.value()
                                + " , last_broadcast_tmp : " + last_broadcast_tmp.value()
                                + " , num_round : " + num_round.value()
                                + " , num_messages : " + num_messages.value()
                                + " , num_messages_sent : " + num_messages_sent.value()
                                + " , estimator : " + estimator.value()
                                + " , type_message : " + input
                                + " , sync : " + sync_cord.value()
                                + " , overhead_messages : " + overhead_messages.value()
                                + " at " + new Timestamp(currentTimestamp));

            // Write some statistics to side output
            ctx.output(coordinator_stats,"Coordinator "
                                            + " , ctx_timestamp : " + new Timestamp(ctx.timestamp() == null ? 0 : ctx.timestamp())
                                            + " , current_processing_time : " + new Timestamp(timerService.currentProcessingTime())
                                            + " , current watermark : " + new Timestamp(timerService.currentWatermark())
                                            + " , last_broadcast_cond : " + last_broadcast_cond.value()
                                            + " , last_broadcast_tmp : " + last_broadcast_tmp.value()
                                            + " , num_round : " + num_round.value()
                                            + " , num_messages : " + num_messages.value()
                                            + " , num_messages_sent : " + num_messages_sent.value()
                                            + " , estimator : " + estimator.value()
                                            + " , type_message : " + input
                                            + " , sync : " + sync_cord.value()
                                            + " , overhead_messages : " + overhead_messages.value()
                                            + " at " + new Timestamp(currentTimestamp));
        }

        @Override
        // Input for initialization
        public void processElement2(Tuple2<String, Integer> input, Context ctx, Collector<Tuple3<String, Integer, Long>> out) throws Exception {

            // Initialize some variables

            // Register event time timer
            ctx.timerService().registerEventTimeTimer(1627851824999L); // First window
            // ctx.timerService().registerEventTimeTimer(0L);

            // Register processing time timer
            // ctx.timerService().registerProcessingTimeTimer(currentTimestamp+500);


            // Assign a timer service
            TimerService timerService = ctx.timerService();

            // Current timestamp
            long currentTimestamp = System.currentTimeMillis();

            // Print some statistics
            ctx.output(coordinator_stats,"Coordinator "
                                            + " , received messages from init source "
                                            + " , ctx_timestamp : " + new Timestamp(ctx.timestamp() == null ? 0 : ctx.timestamp())
                                            + " , current_processing_time : " + new Timestamp(timerService.currentProcessingTime())
                                            + " , current watermark : " + new Timestamp(timerService.currentWatermark())
                                            + " , last_broadcast_cond : " + last_broadcast_cond.value()
                                            + " , last_broadcast_tmp : " + last_broadcast_tmp.value()
                                            + " , num_round : " + num_round.value()
                                            + " , num_messages : " + num_messages.value()
                                            + " , num_messages_sent : " + num_messages_sent.value()
                                            + " , estimator : " + estimator.value()
                                            + " , type_message : " + input
                                            + " , sync : " + sync_cord.value()
                                            + " , overhead_messages : " + overhead_messages.value()
                                            + " at " + new Timestamp(currentTimestamp));

        }

        @Override
        public void onTimer(long timestamp,OnTimerContext ctx,Collector<Tuple3<String, Integer, Long>> out ) throws IOException {

            // Get a timer service
            TimerService timerService = ctx.timerService();

            // Current processing time
            long currentTimestamp = System.currentTimeMillis();

            // Print some statistics about coordinator
            ctx.output(coordinator_stats,"\nOn timer-Coordinator "
                                            + " , timestamp : " + new Timestamp(timestamp)
                                            + " , ctx_timestamp : " + new Timestamp(ctx.timestamp() == null ? 0 : ctx.timestamp())
                                            + " , current_processing_time : " + new Timestamp(timerService.currentProcessingTime())
                                            + " , current watermark : " + new Timestamp(timerService.currentWatermark())
                                            + " , estimator : " + estimator.value()
                                            + " , last_broadcast_cond : " + last_broadcast_cond.value()
                                            + " , last_broadcast_tmp : " + last_broadcast_tmp.value()
                                            + " , num_messages_sent : " + num_messages_sent.value()
                                            + " , overhead_messages : " + overhead_messages.value()
                                            + " , num_round : " + num_round.value()
                                            + " at " + new Timestamp(currentTimestamp)+"\n");

            // Send message to workers
            // for (int i=0;i<num_workers;i++){ out.collect(new Tuple3<>("COUNTER",i,ctx.timestamp())); }

            // Update
            // num_messages_sent.update((long) num_workers);

        }

    }

}
