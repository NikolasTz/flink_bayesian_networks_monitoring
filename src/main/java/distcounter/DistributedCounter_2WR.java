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
import utils.MathUtils;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static utils.MathUtils.highestPowerOf2;

public class DistributedCounter_2WR {

    // Using for statistics about window,workers and coordinator
    private static final OutputTag<String> lateEvents = new OutputTag<String>("late-events"){};
    private static final OutputTag<String> window_stats = new OutputTag<String>("window_stats"){};
    private static final OutputTag<String> coordinator_stats = new OutputTag<String>("coordinator_stats"){};
    private static final OutputTag<String> worker_stats = new OutputTag<String>("worker_stats"){};

    public static void main(String[] args) throws Exception {

        // A simple distributed randomized counter with 2-way communication between coordinator and workers

        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set up the parameters
        ParameterTool params = ParameterTool.fromArgs(args); // fromPropertiesFile() to get parameters from file !!!

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
        // Exact counter !!!
        // Buffering !!!
        // Bayesian Network => worker,coordinator,operator package , message class !!!
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
        env.execute("DC_2WR");

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
    public static class AssignerTimestampWatermark implements AssignerWithPeriodicWatermarks<String> {

        // Current maximum timestamp
        private long currentMaxTimestamp; // Started timestamp,1627851824578L;

        // Some variables
        private long lastEmittedWatermark;
        private long lastEndOfWindow;
        private long lastCurrentProcessingTime;
        private boolean changed;
        private final LinkedList<Long> endsOfWindows;

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

            // Print some statistics
            /*System.out.println("Element"
                                + " , value : " + input.split(",")[0]
                                + " , timestamp : " + new Timestamp(Long.parseLong(input.split(",")[1]))
                                + " , currentMaxTimestamp : " + new Timestamp(currentMaxTimestamp)
                                + " at " + new Timestamp(System.currentTimeMillis()) );*/

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

            // Print some statistics
            /*System.out.println("Emit watermark : "
                                + " currentMaxTimestamp : " + new Timestamp(currentMaxTimestamp)
                                + " , lastEmittedWatermark : " + new Timestamp(lastEmittedWatermark)
                                + " , lastEndOfWindow : " + new Timestamp(lastEndOfWindow)
                                + " , lastCurrentProcessingTime : " + new Timestamp(lastCurrentProcessingTime)
                                + " , endOfWindow : " + new Timestamp(endOfWindow)
                                + " , changed " + changed
                                + " , endsOfWindows " + endsOfWindows
                                + " at " + new Timestamp(currentTime) + " , long_format : " + currentTime);*/

            // Emit a new Watermark
            return new Watermark(lastEmittedWatermark);
        }


    }

    // Generate all watermarks and emitted with delay(for testing purposes)
    public static class GeneratedWatermarks implements AssignerWithPeriodicWatermarks<String>{

        // Current maximum timestamp
        private long currentMaxTimestamp; // Started timestamp

        // Some variables
        private final long startTs;
        private final long endTs;
        private long lastEmittedWatermark;
        private long lastCurrentProcessingTime;
        private boolean changed;
        private final LinkedList<Long> endsOfWindows;
        private boolean first;
        private final long threshold_waiting;
        private final long window_size;

        // Constructor
        public GeneratedWatermarks(){

            // Default values
            currentMaxTimestamp = 1627851824578L;
            startTs = 1627851824578L;
            endTs = 1627851874568L; // 5K=1627851874568L , 50K = 1627852324568L

            threshold_waiting = 1000L;
            window_size = DefaultParameters.window_size;
            lastEmittedWatermark = 0L;
            lastCurrentProcessingTime = System.currentTimeMillis();
            changed = false;
            first = true;
            endsOfWindows = new LinkedList<>();
        }

        public GeneratedWatermarks(long start,long end,long window_size,long threshold){

            currentMaxTimestamp = start;
            startTs = start;
            endTs = end;
            lastEmittedWatermark = 0L;
            lastCurrentProcessingTime = System.currentTimeMillis();
            changed = false;
            first = true;
            endsOfWindows = new LinkedList<>();
            this.window_size = window_size;
            threshold_waiting = threshold;
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
                long end = (endTs - (endTs % window_size) + window_size - 1);
                long start = (startTs - (startTs % window_size) + window_size - 1);

                // Add the timestamp 0
                endsOfWindows.add(0L);

                while(start <= end){
                    endsOfWindows.add(start);
                    start+=window_size;
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
                    if( currentTime - lastCurrentProcessingTime >= threshold_waiting ){
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
                ctx.output(lateEvents,"Late element , "+input);
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

    // Processing time tumbling window
    public static final class TumblingProcessingTimeWindow extends KeyedProcessFunction<Tuple,Tuple3<String,Integer,Long>,Tuple3<String,Integer,Long>> {

        // Window size to milliseconds
        private final long windowSizeMsec;

        // Keyed, managed state, with an entry for each window, keyed by the window's end time
        // MapState => key correspond to window's end time and value correspond to elements which belongs to this event time window
        private transient MapState<Long, List<Tuple3<String,Integer,Long>>> window_state;

        // Constructor
        public TumblingProcessingTimeWindow(Time windowSize){ this.windowSizeMsec = windowSize.toMilliseconds(); }

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
            long endOfWindow = (ctx.timerService().currentProcessingTime() - (ctx.timerService().currentProcessingTime() % windowSizeMsec) + windowSizeMsec - 1);

            // Register a timer/callback for when the window has been completed
            ctx.timerService().registerProcessingTimeTimer(endOfWindow);

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
    public static final class Worker extends KeyedCoProcessFunction<Tuple,Tuple3<String, Integer, Long>,Tuple3<String, Integer, Long>, Tuple2<String, Integer>> {

        // The state of each worker , just a simple counter
        private transient ValueState<Long> counter; // ni , increment whenever receive a new element

        // Used to check when the local counter doubles
        private transient ValueState<Long> counter_doubles;

        // Last broadcast value from coordinator
        private transient ValueState<Integer> last_broadcast_value; // n_dash => last broadcast value from coordinator

        // Last sent value from worker
        private transient ValueState<Long> last_sent_value; // ni_dash => last sent value from worker

        // Last timestamp
        private transient ValueState<Long> last_ts;

        // Number of messages
        private transient ValueState<Long> num_messages;

        // Sync
        private transient ValueState<Boolean> sync;

        // Probability
        private transient ValueState<Double> prob;

        // Epsilon
        private final double epsilon = DefaultParameters.eps; // Initialize from properties/configuration

        // Delta
        private final double delta = DefaultParameters.delta; // Initialize from properties/configuration

        // Rescaling constant
        private final double res_constant = 1.0d; // With res_constant=1 , the default delta is 0.25

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

            // Counter doubles
            ValueStateDescriptor<Long> valDesc_doubles = new ValueStateDescriptor<>("counter_doubles", Long.class);
            counter_doubles = getRuntimeContext().getState(valDesc_doubles);

            // Last broadcast value from coordinator
            ValueStateDescriptor<Integer> valDesc_br = new ValueStateDescriptor<>("last_broadcast_value", Integer.class);
            last_broadcast_value = getRuntimeContext().getState(valDesc_br);

            // Last sent value from worker
            ValueStateDescriptor<Long> valDesc_sent = new ValueStateDescriptor<>("last_sent_value", Long.class);
            last_sent_value = getRuntimeContext().getState(valDesc_sent);

            // Last timestamp
            ValueStateDescriptor<Long> valDesc_ts = new ValueStateDescriptor<>("last_ts", Long.class);
            last_ts = getRuntimeContext().getState(valDesc_ts);

            // Number of messages
            ValueStateDescriptor<Long> valDesc_messages = new ValueStateDescriptor<>("num_messages",Long.class);
            num_messages = getRuntimeContext().getState(valDesc_messages);

            // Sync
            ValueStateDescriptor<Boolean> valDesc_sync = new ValueStateDescriptor<>("sync",Boolean.class);
            sync = getRuntimeContext().getState(valDesc_sync);

            // Probability
            ValueStateDescriptor<Double> valDesc_prob = new ValueStateDescriptor<>("prob",Double.class);
            prob = getRuntimeContext().getState(valDesc_prob);

        }

        @Override
        // Input from source
        // Output to the coordinator
        public void processElement1(Tuple3<String, Integer, Long> input, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

            // Initialize the state of worker
            if (counter.value() == null) {

                // Initialize the state of worker
                counter.update(0L);
                counter_doubles.update((long) (MathUtils.findValueProbHalved(epsilon,workers)/workers));

                // Initialize the value of last_broadcast_value
                last_broadcast_value.update(MathUtils.findValueProbHalved(epsilon,workers));

                // Initialize the last sent value
                last_sent_value.update(counter_doubles.value());

                // Initialize the last timestamp
                last_ts.update(input.f2);

                // Initialize the number of messages
                num_messages.update(last_sent_value.value());

                // Initialize sync
                sync.update(true);

                // Initialize probability
                prob.update(1.0/highestPowerOf2(epsilon * last_broadcast_value.value() / Math.sqrt(workers)));
            }

            // Update the value of counter
            counter.update(counter.value()+1L);

            // When counter increment by one then send message with probability prob to coordinator
            // INCREMENT message contain INCREMENT,num_worker,last_broadcast_value,prob,current_processing_time(f0),counter(f1)
            if( (counter.value() > (MathUtils.findValueProbHalved(epsilon,workers)/workers)) && prob.value() > new Random().nextDouble() ){

                // Send the message to coordinator(worker,counter_value)
                out.collect(new Tuple2<>("INCREMENT,"+ctx.getCurrentKey().getField(0).toString()+","+last_broadcast_value.value()+","+prob.value()+","+ctx.timerService().currentProcessingTime(),counter.value().intValue()));

                // Update the last sent value
                last_sent_value.update(counter.value());

                // Update the number of messages
                num_messages.update(num_messages.value()+1L);

                // Update last ts(per window computation)
                last_ts.update(input.f2);

                // Print some statistics
                printMessages(ctx,"","INCREMENT");

            }

            // Whenever counter doubles then send an additional message to coordinator
            double avoid_zero = counter_doubles.value() == 0 ? 1d : counter_doubles.value();
            if( (counter_doubles.value() == 0 && counter.value() == 2) || (counter.value()/avoid_zero >= 2) ){

                // Send the message to coordinator(worker,counter_value)
                out.collect(new Tuple2<>("DOUBLES,"+ctx.getCurrentKey().getField(0).toString(),counter.value().intValue()));

                // Update the counter_doubles
                counter_doubles.update(counter.value());

                // Update the last sent value
                last_sent_value.update(counter.value());

                // Update the number of messages
                num_messages.update(num_messages.value()+1L);

                // Update last_ts(per window computation)
                last_ts.update(input.f2);

                // Print some statistics
                printMessages(ctx,"","DOUBLES");

            }

            // Print some statistics
            printStatistics(ctx,input,false);

        }

        @Override
        // Input from feedback source
        // When receive a new message then update the sending condition
        public void processElement2(Tuple3<String, Integer, Long> input, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

            // Message from coordinator

            // Update the last broadcast value , means that the last broadcast value changed by factor between 2 and 4
            if( input.f0.split(",")[0].equals("UPDATE") ){

                // New round begins(not used now)
                sync.update(true);

                // Update last broadcast value
                last_broadcast_value.update(Integer.valueOf(input.f0.split(",")[1]));

                if( last_broadcast_value.value() > (Math.sqrt(workers)/epsilon) ) {

                    // Find the next power of 2 that is smaller than ε*n_hat/sqrt(workers)
                    int power = highestPowerOf2((epsilon * last_broadcast_value.value()) / Math.sqrt(workers));
                    if(power == 0) power = 1;

                    // Calculate the new probability
                    double new_prob = 1.0 / power;

                    // If probability halved then send message to coordinator to adjust the ni(counter)
                    if (prob.value()/new_prob >= 2) {

                        // Adjustment
                        // Update the new probability
                        prob.update(new_prob);

                        // With probability = 1/2 send the counter/ni to coordinator
                        if (new Random().nextDouble() < 0.5) {

                            // Send the message to coordinator(worker,counter_value)
                            out.collect(new Tuple2<>("INCREMENT," + ctx.getCurrentKey().getField(0).toString()+","+last_broadcast_value.value()+","+prob.value()+","+ctx.timerService().currentProcessingTime(),counter.value().intValue()));

                            // Update the last sent value
                            last_sent_value.update(counter.value());

                            // Print some statistics
                            printMessages(ctx," , probability halved , no losing flip ","INCREMENT");

                        }
                        // If we loose the flip
                        else {

                            // Flip a coin with new probability until we succeed and count the number of failures
                            long num_failures = 0L;
                            Random rand = new Random();

                            while (rand.nextDouble() > prob.value()) {
                                // Increment the number of failures
                                num_failures += 1;
                            }

                            // At least one flip
                            if (num_failures == 0) num_failures = 1;

                            // Send the message to coordinator(worker,adjusted_counter) where adjusted_counter = counter_value-num_failures
                            int adjusted = (counter.value() - num_failures) < 0 ? 0 : (int) (counter.value() - num_failures);
                            out.collect(new Tuple2<>("INCREMENT," + ctx.getCurrentKey().getField(0).toString()+","+last_broadcast_value.value()+","+prob.value()+","+ctx.timerService().currentProcessingTime(), adjusted));

                            // Update the last sent value
                            last_sent_value.update((long) adjusted);

                            // Print some statistics
                            printMessages(ctx," , probability halved , losing flip , num_failures : "+ num_failures,"INCREMENT");

                        }

                        // Update the number of messages
                        num_messages.update(num_messages.value() + 1L);
                    }
                    else {
                        // Do nothing , just update the new probability
                        prob.update(new_prob);
                    }

                }

            }

            // Print some statistics
            printStatistics(ctx,input,true);

        }

        // On-timer
        //@Override
        //public void onTimer(long timestamp,OnTimerContext ctx,Collector<Tuple2<String, Integer>> out ){ }

        // Write worker statistics to side output
        public void printStatistics(Context ctx,Tuple3<String, Integer, Long> input,boolean received) throws IOException {

            // Assign a timer service
            TimerService timerService = ctx.timerService();

            // Current processing time
            long currentTimestamp = System.currentTimeMillis();

            StringBuilder str = new StringBuilder();
            str.append("Worker ").append(ctx.getCurrentKey());

            if(received){ str.insert(0,"\nReceived message , ").append(" , type_message : ").append(input); }

            ctx.output(worker_stats, str
                                        + " , last_ts : " + new Timestamp(last_ts.value())
                                        + " , timestamp_input : " + new Timestamp(input.f2)
                                        + " , ctx_timestamp : " + new Timestamp(ctx.timestamp() == null ? 0 : ctx.timestamp())
                                        + " , current_processing_time : " + new Timestamp(timerService.currentProcessingTime())
                                        + " , current watermark : " + new Timestamp(timerService.currentWatermark())
                                        + " , last_broadcast_value : " + last_broadcast_value.value()
                                        + " , last_sent_value : " + last_sent_value.value()
                                        + " , counter : " + counter.value()
                                        + " , counter_doubles : " + counter_doubles.value()
                                        + " , probability : " + prob.value()
                                        + " , num_messages : " + num_messages.value()
                                        + " , sync : " + sync.value()
                                        + " at " + new Timestamp(currentTimestamp)
                                        + (received ? "\n" : ""));

        }

        // Write the messages which sent or received from/to coordinator to side output
        public void printMessages(Context ctx,String probability, String type_message) throws IOException {

            ctx.output(worker_stats, "\nWorker " + ctx.getCurrentKey()
                                        + probability
                                        + " , send a message to coordinator"
                                        + " , type_message : " + type_message
                                        + " , counter : " + counter.value()
                                        + " , counter_doubles : " + counter_doubles.value()
                                        + " , last_sent_value : " + last_sent_value.value()
                                        + " , last_broadcast_value : " + last_broadcast_value.value()
                                        + " , probability : " + prob.value()
                                        + " , num_messages : " + num_messages.value()
                                        + " , sync : " + sync.value()
                                        + " at " + new Timestamp(System.currentTimeMillis())+"\n");

        }

    }

    // Coordinator
    public static final class Coordinator extends CoProcessFunction<Tuple2<String, Integer>,Tuple2<String, Integer>,Tuple3<String, Integer, Long>> {

        // The state of coordinator , just a simple counter which correspond to last send value from worker
        // Each position correspond to each worker
        private transient MapState<String,Integer> last_sent_workers; // ni_dash , for all workers

        // Estimator of counters
        // Key : Each position correspond to each worker
        // Value : Correspond to estimation for counter
        private transient MapState<String,Integer> estimators; // ni_hat , for all workers

        // Last updates
        // Key : Each position correspond to each worker
        // Value : Correspond to last update
        private transient MapState<String,Integer> last_updates; // ni' , for all workers

        // Current processing time round with previous probability
        // Key : Position correspond to the processing time of the beginning of last round(f0)
        // Value : Correspond to probability of previous round(f1)
        private transient ValueState<Tuple2<String,Double>> current_proc_prob; // ???

        // The estimated total count
        private transient ValueState<Integer> total_count;

        // Last broadcast value doubles
        private transient ValueState<Integer> last_broadcast_value_doubles; // n_dash

        // Probability
        private transient ValueState<Double> prob;

        // Number of workers
        private final int num_workers = DefaultParameters.num_workers;

        // Number of rounds
        private transient ValueState<Integer> num_round;

        // Number of messages sent from coordinator
        private transient ValueState<Long> num_messages_sent;

        // Sync
        private transient ValueState<Boolean> sync_cord;

        // Number of messages lost
        private transient ValueState<Long> overhead_messages;


        @Override
        // Initialize state
        // Get parameters from config
        public void open(Configuration config) {

            // Initialize the descriptor of coordinator state

            // Coordinator state
            MapStateDescriptor<String, Integer> mapDesc = new MapStateDescriptor<>("last_sent_workers",String.class,Integer.class);
            last_sent_workers = getRuntimeContext().getMapState(mapDesc);

            // Estimator
            MapStateDescriptor<String, Integer> mapDesc_est = new MapStateDescriptor<>("estimators",String.class,Integer.class);
            estimators = getRuntimeContext().getMapState(mapDesc_est);

            // Last updates
            MapStateDescriptor<String, Integer> mapDesc_updates = new MapStateDescriptor<>("last_updates",String.class,Integer.class);
            last_updates = getRuntimeContext().getMapState(mapDesc_updates);

            // Total count
            ValueStateDescriptor<Integer> valDesc_total = new ValueStateDescriptor<>("total_count", Integer.class);
            total_count = getRuntimeContext().getState(valDesc_total);


            // Last broadcast value doubles
            ValueStateDescriptor<Integer> valDesc_broad_doubles = new ValueStateDescriptor<>("last_broadcast_value_doubles", Integer.class);
            last_broadcast_value_doubles = getRuntimeContext().getState(valDesc_broad_doubles);

            // Probability
            ValueStateDescriptor<Double> valDesc_prob_cord = new ValueStateDescriptor<>("prob", Double.class);
            prob = getRuntimeContext().getState(valDesc_prob_cord);

            // Counter new round
            ValueStateDescriptor<Integer> valDesc_num_round = new ValueStateDescriptor<>("num_round", Integer.class);
            num_round = getRuntimeContext().getState(valDesc_num_round);

            // Number of messages sent from coordinator
            ValueStateDescriptor<Long> valDesc_sent = new ValueStateDescriptor<>("num_messages_sent", Long.class);
            num_messages_sent = getRuntimeContext().getState(valDesc_sent);

            // Initialize sync
            ValueStateDescriptor<Boolean> valDesc_syncC = new ValueStateDescriptor<>("sync_cord", Boolean.class);
            sync_cord = getRuntimeContext().getState(valDesc_syncC);

            // Initialize num_messages_lost
            ValueStateDescriptor<Long> valDesc_overhead = new ValueStateDescriptor<>("overhead_messages", Long.class);
            overhead_messages = getRuntimeContext().getState(valDesc_overhead);

            // Current processing time round with previous probability
            ValueStateDescriptor<Tuple2<String,Double>> valDesc_cur = new ValueStateDescriptor<>("current_proc_prob", Types.TUPLE(Types.STRING,Types.DOUBLE));
            current_proc_prob = getRuntimeContext().getState(valDesc_cur);
        }

        @Override
        // Input for workers
        // Output to feedback-source
        public void processElement1(Tuple2<String, Integer> input, Context ctx, Collector<Tuple3<String, Integer, Long>> out) throws Exception {

            // Initialization
            if( num_round.value() == null ){

                // Initialization
                for(int i=0;i<num_workers;i++){
                    last_sent_workers.put(String.valueOf(i), MathUtils.findValueProbHalved(DefaultParameters.eps,num_workers)/num_workers);
                    estimators.put(String.valueOf(i), MathUtils.findValueProbHalved(DefaultParameters.eps,num_workers)/num_workers);
                    last_updates.put(String.valueOf(i), MathUtils.findValueProbHalved(DefaultParameters.eps,num_workers)/num_workers);
                }

                total_count.update(MathUtils.findValueProbHalved(DefaultParameters.eps,num_workers));
                prob.update(1.0/highestPowerOf2(DefaultParameters.eps * total_count.value() / Math.sqrt(num_workers)));
                last_broadcast_value_doubles.update(total_count.value());
                num_round.update(highestPowerOf2(total_count.value()));
                num_messages_sent.update(((long) num_round.value()*num_workers));
                sync_cord.update(true);
                overhead_messages.update(0L);
                current_proc_prob.update(new Tuple2<>("0",prob.value()));
            }


            if( input.f0.split(",")[0].equals("INCREMENT") )  ctx.output(coordinator_stats,"Coordinator , input : " + input
                                                                                                    +" ,timestamp input : " + new Timestamp(Long.parseLong(input.f0.split(",")[4]))
                                                                                                    +" , current_proc_prob : " + current_proc_prob.value().toString()
                                                                                                    +" , current_proc_prob timestamp : " + new Timestamp(Long.parseLong(current_proc_prob.value().f0)));

            // Update the overhead of messages
            if( input.f0.split(",")[0].equals("INCREMENT") && ( last_broadcast_value_doubles.value() != Integer.parseInt(input.f0.split(",")[2]) ) && prob.value() < 1) overhead_messages.update(overhead_messages.value()+1L);


            // Check the type of message
            // INCREMENT
            if( input.f0.split(",")[0].equals("INCREMENT") ){

                // Get the worker
                String worker = input.f0.split(",")[1];

                // Update the last sent value from worker
                last_sent_workers.put(worker,input.f1);

                // Update the estimator
                estimators.put(worker,calculateEstimator(worker));

            }
            // DOUBLES
            else if ( input.f0.split(",")[0].equals("DOUBLES") ){

                // Update the ni'
                last_updates.put(input.f0.split(",")[1], input.f1);

                // Calculate the n' => sum{ni'} at given time
                int total_updates = calculateSumOfUpdates();

                // Update the last sent value
                last_sent_workers.put(input.f0.split(",")[1],input.f1);

                // When the n' is changed by a factor between two and four then new round begins
                double avoid_zero = last_broadcast_value_doubles.value() == 0 ? 1d : last_broadcast_value_doubles.value();
                if( (last_broadcast_value_doubles.value() == 0 && total_updates > 2) || (total_updates/avoid_zero) > 2 ){

                    // Update the last_broadcast_value_doubles => n_dash
                    last_broadcast_value_doubles.update(total_updates);

                    // Send message to workers
                    for (int i=0;i<num_workers;i++){ out.collect(new Tuple3<>("UPDATE,"+last_broadcast_value_doubles.value(),i,ctx.timestamp())); }

                    // Update the number of messages sent from coordinator
                    num_messages_sent.update(num_messages_sent.value()+num_workers);

                    // Update current processing time round with previous probability
                    current_proc_prob.update(new Tuple2<>(String.valueOf(ctx.timerService().currentProcessingTime()),prob.value()));

                    // Update the probability
                    // last_broadcast_value_doubles => n_hat
                    if( last_broadcast_value_doubles.value() > (Math.sqrt(num_workers)/DefaultParameters.eps) ){

                        // Find the next power of 2 that is smaller than epsilon*n_hat/sqrt(workers)
                        int power = highestPowerOf2((DefaultParameters.eps * last_broadcast_value_doubles.value()) / Math.sqrt(num_workers));
                        if(power == 0) power = 1;

                        // Update the new probability
                        prob.update(1.0/ power);
                    }

                    // Update the estimators
                    updateEstimators();

                    // Update the number of round
                    num_round.update(num_round.value()+1);

                    // Start a new round,update sync
                    sync_cord.update(true);


                    // Print some statistics
                    System.out.println("\nReceived and send messages from/to workers "
                                        + " , new round begins "
                                        + " , type_message_rec : " + input
                                        + " , type_message_sent : " + "UPDATE"
                                        + " , last_broadcast_value_doubles : " + last_broadcast_value_doubles.value()
                                        + " , probability : " + prob.value()
                                        + " , num_round : " + num_round.value()
                                        + " , sync : " + sync_cord.value()
                                        + " at " + new Timestamp(System.currentTimeMillis())+"\n");


                    // Write to side output
                    printMessagesCord(ctx,input);
                }

            }


            // Update total count(or update periodically with callback)
            calculateTotalCount();

            // Assign a timer service
            TimerService timerService = ctx.timerService();

            // Current timestamp
            long currentTimestamp = System.currentTimeMillis();

            // Context timestamp correspond to last sent timestamp of message
            System.out.println("Coordinator "
                                + " , ctx_timestamp : " + new Timestamp(ctx.timestamp() == null ? 0 : ctx.timestamp())
                                + " , current_processing_time : " + new Timestamp(timerService.currentProcessingTime())
                                + " , current watermark : " + new Timestamp(timerService.currentWatermark())
                                + " , last_broadcast_value_doubles : " + last_broadcast_value_doubles.value()
                                + " , num_round : " + num_round.value()
                                + " , num_messages_sent : " + num_messages_sent.value()
                                + " , probability : " + prob.value()
                                + " , last_sent_workers : " + printMapState(last_sent_workers)
                                + " , last_updates : " + printMapState(last_updates)
                                + " , estimators : " + printMapState(estimators)
                                + " , total count : " + total_count.value()
                                + " , type_message : " + input
                                + " , sync : " + sync_cord.value()
                                + " , overhead_messages : " + overhead_messages.value()
                                + " at " + new Timestamp(currentTimestamp));

            // Write to side output some statistics
            printStatisticsCord(ctx,input);

        }

        @Override
        // Input for initialization or query
        public void processElement2(Tuple2<String, Integer> input, Context ctx, Collector<Tuple3<String, Integer, Long>> out) throws Exception {

            // Initialize some variables

            // Register event time timer
            ctx.timerService().registerEventTimeTimer(1627851824999L); // First window
            // ctx.timerService().registerEventTimeTimer(0L);

            // Register event time timer
            // ctx.timerService().registerProcessingTimeTimer(currentTimestamp+500);

            // Print some statistics
            printStatisticsCord(ctx,input);

        }

        @Override
        public void onTimer(long timestamp,OnTimerContext ctx,Collector<Tuple3<String, Integer, Long>> out ) throws Exception {

            // Get a timer service
            TimerService timerService = ctx.timerService();

            // Current processing time
            long currentTimestamp = System.currentTimeMillis();

            // Print some statistics about coordinator
            ctx.output(coordinator_stats,"On timer-Coordinator "
                                            + " , timestamp : " + new Timestamp(timestamp)
                                            + " , ctx_timestamp : " + new Timestamp(ctx.timestamp() == null ? 0 : ctx.timestamp())
                                            + " , current_processing_time : " + new Timestamp(timerService.currentProcessingTime())
                                            + " , current watermark : " + new Timestamp(timerService.currentWatermark())
                                            + " , last_sent_workers : " + printMapState(last_sent_workers)
                                            + " , estimators : " + printMapState(estimators)
                                            + " , total count : " + total_count.value()
                                            + " , num_messages_sent : " + num_messages_sent.value()
                                            + " , overhead_messages : " + overhead_messages.value()
                                            + " , num_round : " + num_round.value()
                                            + " at " + new Timestamp(currentTimestamp));

            // Send message to workers
            // for (int i=0;i<num_workers;i++){ out.collect(new Tuple3<>("COUNTER",i,ctx.timestamp())); }

            // Update
            // num_messages_sent.update((long) num_workers);

        }


        // Calculate the new estimation for worker
        public int calculateEstimator(String worker) throws Exception {

            // Get the last sent value from worker
            int value = last_sent_workers.get(worker);

            // If ni_hat does not exist
            if( value == 0 ){ return 0; }
            else{
                // Calculate the new estimation => ni_hat - 1 + 1/prob
                return (int) (value - 1 + (1.0/prob.value()));
            }

        }

        // Update all estimators
        public void updateEstimators() throws Exception {

            for(int i=0;i<num_workers;i++){
                String worker = String.valueOf(i);
                estimators.put(worker,calculateEstimator(worker));
            }
        }

        // Calculate total count
        public void calculateTotalCount() throws Exception {

            // Reset total count
            total_count.update(0);

            // Update the estimators
            updateEstimators();

            // Get the values and added
            estimators.values().forEach( (value) -> {
                try { total_count.update(total_count.value()+value); }
                catch (IOException e) { e.printStackTrace(); }
            });
        }

        // Calculate the sum of last updates(ni')
        public int calculateSumOfUpdates() throws Exception {

            AtomicInteger total_updates = new AtomicInteger();

            last_updates.values().forEach(total_updates::addAndGet);

            return total_updates.get();
        }

        // Print map state
        public String printMapState(MapState<String,Integer> mapState) throws Exception {

            StringBuilder str = new StringBuilder();
            str.append("[");

            for( Map.Entry<String,Integer> entry : mapState.entries() ){
                str.append("(").append(entry.getKey()).append(",").append(entry.getValue().toString()).append(")").append(",");
            }

            if( str.toString().length() > 1) str.deleteCharAt(str.lastIndexOf(","));
            str.append("]");
            return str.toString();

        }

        // Write some statistics for coordinator to side output
        public void printStatisticsCord(Context ctx,Tuple2<String, Integer> input) throws Exception {

            // Assign a timer service
            TimerService timerService = ctx.timerService();

            // Current timestamp
            long currentTimestamp = System.currentTimeMillis();

            ctx.output(coordinator_stats,"Coordinator "
                                            + " , ctx_timestamp : " + new Timestamp(ctx.timestamp() == null ? 0 : ctx.timestamp())
                                            + " , current_processing_time : " + new Timestamp(timerService.currentProcessingTime())
                                            + " , current watermark : " + new Timestamp(timerService.currentWatermark())
                                            + " , last_broadcast_value_doubles : " + last_broadcast_value_doubles.value()
                                            + " , num_round : " + num_round.value()
                                            + " , num_messages_sent : " + num_messages_sent.value()
                                            + " , probability : " + prob.value()
                                            + " , last_sent_workers : " + printMapState(last_sent_workers)
                                            + " , last_updates : " + printMapState(last_updates)
                                            + " , estimators : " + printMapState(estimators)
                                            + " , total count : " + total_count.value()
                                            + " , type_message : " + input
                                            + " , sync : " + sync_cord.value()
                                            + " , overhead_messages : " + overhead_messages.value()
                                            + " at " + new Timestamp(currentTimestamp) );

        }

        // Write the messages which sent or received from/to workers to side output
        public void printMessagesCord(Context ctx , Tuple2<String, Integer> input) throws IOException {

            ctx.output(coordinator_stats,"\nReceived and send messages from/to workers "
                                            + " , new round begins "
                                            + " , type_message_rec : " + input
                                            + " , type_message_sent : " + "UPDATE"
                                            + " , last_broadcast_value_doubles : " + last_broadcast_value_doubles.value()
                                            + " , probability : " + prob.value()
                                            + " , num_round : " + num_round.value()
                                            + " , sync : " + sync_cord.value()
                                            + " at " + new Timestamp(System.currentTimeMillis())+"\n");

        }
    }


}
