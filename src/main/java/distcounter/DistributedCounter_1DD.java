package distcounter;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.util.Map;
import java.util.Random;

import static utils.KafkaUtils.simpleConsumer;

public class DistributedCounter_1DD {

    private static final OutputTag<String> worker_stats = new OutputTag<String>("side-output"){};
    private static final OutputTag<String> coordinator_stats = new OutputTag<String>("coordinator_stats"){};

    public static void main(String[] args) throws Exception {

        // A simple communication-efficient deterministic distributed counter with one-way communication

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

        // Mapper produce a Tuple2<String,Worker>
        DataStream<Tuple2<String, Integer>> inputStream = env
                                                          .addSource(simpleConsumer(DefaultParameters.input_topic,DefaultParameters.server,DefaultParameters.input_group_id)
                                                          .setStartFromEarliest())
                                                          .map(new Mapper())
                                                          .name("Input Stream");

        // Worker
        SingleOutputStreamOperator<Tuple2<String, Integer>> workerStream =  inputStream
                                                                           .keyBy(1) // KeyBy number of worker
                                                                           .process(new Worker())
                                                                           .name("Workers");

        // Coordinator
        SingleOutputStreamOperator<Integer> coordinatorStream =  workerStream
                                                                .keyBy(input -> "0")
                                                                .process(new Coordinator()).setParallelism(1)
                                                                .name("Coordinator");


        // Write worker statistics
        workerStream.getSideOutput(worker_stats).writeAsText("workers_statistics.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("Workers statistics");

        // Write coordinator statistics
        coordinatorStream.getSideOutput(coordinator_stats).writeAsText("coordinator_statistics.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("Coordinator statistics");


        // Print the execution plan
        // System.out.println(env.getExecutionPlan());

        // Trigger the execution of program
        env.execute("DC_1DD");

    }


    // *************************************************************************
    //                              USER FUNCTIONS
    // *************************************************************************

    // Mapper
    public static final class Mapper extends RichMapFunction<String, Tuple2<String, Integer>> {

        private transient int parallelism;

        @Override
        public void open(Configuration config) { parallelism = getRuntimeContext().getExecutionConfig().getParallelism(); }

        @Override
        public Tuple2<String, Integer> map(String value) { return new Tuple2<>(value, new Random().nextInt(parallelism)); }

    }

    // Worker
    public static final class Worker extends KeyedProcessFunction<Tuple,Tuple2<String, Integer>,Tuple2<String, Integer>> {


        // The state of each worker , just a simple counter
        private transient ValueState<Integer> local_counter; // ni

        // Last sent value to coordinator
        private transient ValueState<Integer> last_send; // ni_dash

        // Number of messages per worker
        private transient ValueState<Long> num_messages;

        @Override
        // Initialize state
        public void open(Configuration config) {

            // Initialize the descriptor of state

            // Local counter
            ValueStateDescriptor<Integer> valDesc = new ValueStateDescriptor<>("local_counter",Integer.class);
            local_counter = getRuntimeContext().getState(valDesc);

            // Last sent value
            ValueStateDescriptor<Integer> valDesc_1 = new ValueStateDescriptor<>("last_send",Integer.class);
            last_send = getRuntimeContext().getState(valDesc_1);

            // Number of messages
            ValueStateDescriptor<Long> valDesc_2 = new ValueStateDescriptor<>("num_messages",Long.class);
            num_messages = getRuntimeContext().getState(valDesc_2);

        }

        @Override
        public void processElement(Tuple2<String, Integer> input, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

            // Initialize counters
            if(local_counter.value() == null){
                local_counter.update(0);
                last_send.update(0);
                num_messages.update(0L);
            }

            // Get the value of state
            Integer counter = local_counter.value();

            // Check the condition ni > (1+epsilon)ni_dash
            if( counter != 0 ) {
                if ( (counter + 1) >= (1 + DefaultParameters.eps)*last_send.value() ) {

                    // Send a message to coordinator with updated counter => (UPDATE,num_worker,ni)
                    out.collect(new Tuple2<>("UPDATE,"+ctx.getCurrentKey().getField(0).toString(), counter + 1));

                    // Update the last send value
                    last_send.update(counter+1);

                    // Update the number of messages
                    num_messages.update(num_messages.value()+1L);
                }
            }

            // Update the counter
            local_counter.update(counter+1);

            // Collect the worker statistics
            ctx.output(worker_stats, "Worker : " + ctx.getCurrentKey()
                                        +" , local_count : " + local_counter.value()
                                        +" , number_of_messages : " + num_messages.value()
                                        +" at " + new Timestamp(System.currentTimeMillis()));
        }


    }

    // Coordinator
    public static final class Coordinator extends ProcessFunction<Tuple2<String, Integer>,Integer> {


        // The state of each worker , just a simple counter
        // Key : number of worker
        // Value : The last sent local counter from worker
        private transient MapState<String,Integer> appr_counter; // ni_dash

        // Total count
        private transient ValueState<Long> total_count; // n_dash => sum{ni_dash}

        @Override
        // Initialize state
        // Get parameters from config
        public void open(Configuration config) {

            // Initialize the descriptor of state
            MapStateDescriptor<String, Integer> mapDesc = new MapStateDescriptor<>("appr_counter",String.class,Integer.class);
            appr_counter = getRuntimeContext().getMapState(mapDesc);

            ValueStateDescriptor<Long> valueDesc = new ValueStateDescriptor<>("total_count",Long.class);
            total_count = getRuntimeContext().getState(valueDesc);

        }

        @Override
        public void processElement(Tuple2<String, Integer> input, Context ctx, Collector<Integer> out) throws Exception {

            // Update the counter of worker
            appr_counter.put(input.f0.split(",")[1],input.f1);

            // Calculate and update the total count
            int sum = 0;
            for ( Integer i : appr_counter.values()) { sum += i; }
            total_count.update((long) sum);

            // Collect the coordinator statistics
            ctx.output(coordinator_stats, "Coordinator : "
                                            +" , local counters : " + printMapState(appr_counter)
                                            +" , total count : " + total_count.value()
                                            +" at " + new Timestamp(System.currentTimeMillis()));

        }

        // @Override
        // public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception { }

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

    }
}
