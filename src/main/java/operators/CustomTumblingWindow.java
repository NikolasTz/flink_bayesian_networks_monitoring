package operators;

import datatypes.input.Input;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static job.JobKafka.late_events;
import static job.JobKafka.window_stats;


public class CustomTumblingWindow {

    // Event time tumbling window
    public static class TumblingEventTimeWindow extends KeyedProcessFunction<String,Input,Input>{

        // Window size to milliseconds
        private final long windowSize;

        // Keyed, managed state, with an entry for each window, keyed by the window's end time
        // MapState => key correspond to window's end time and value correspond to elements which belongs to this event time window
        private transient MapState<Long, List<Input>> window_state;

        // Constructor
        public TumblingEventTimeWindow(Time windowSize){ this.windowSize = windowSize.toMilliseconds(); }

        @Override
        public void open(Configuration config){

            // Initialize the descriptor for window state

            // Map state
            MapStateDescriptor<Long, List<Input>> mapDesc =  new MapStateDescriptor<>("window_state", TypeInformation.of(Long.class), Types.LIST(Types.POJO(Input.class)) );

            window_state = getRuntimeContext().getMapState(mapDesc);
        }

        @Override
        public void processElement(Input input, Context ctx, Collector<Input> out) throws Exception {

            // Get a time service
            TimerService timerService = ctx.timerService();

            // Find the end of window for this input
            long endOfWindow = (input.getTimestamp() - (input.getTimestamp() % windowSize) + windowSize - 1);

            // Late elements => collect to side output
            if( input.getTimestamp() < timerService.currentWatermark() ){

                // Emit late element to side output
                ctx.output(late_events,"Late element , "+input);
            }
            else{

                // Register a timer/callback for when the window has been completed
                ctx.timerService().registerEventTimeTimer(endOfWindow);

                List<Input> list;

                // Get the window state of input
                if( window_state.get(endOfWindow) == null ){

                    // Initialize the list for this key
                    list = new ArrayList<>();

                }
                else{ list = window_state.get(endOfWindow); }

                // Add element to list and change the timestamp to end_window
                input.setTimestamp(endOfWindow);
                list.add(input);

                // Update the state
                window_state.put(endOfWindow,list);

            }

            // Current processing time
            long currentTimestamp = System.currentTimeMillis();

            // Emit to side output
            ctx.output(window_stats,"Window-Worker " + ctx.getCurrentKey()
                                        + " , timestamp_input : " + new Timestamp(input.getTimestamp())
                                        + " , endOfWindow : " + new Timestamp(endOfWindow)
                                        + " , ctx_timestamp : " + new Timestamp(ctx.timestamp() == null ? 0 : ctx.timestamp())
                                        + " , current_processing_time : "+ new Timestamp(timerService.currentProcessingTime())
                                        + " , current watermark : "+ new Timestamp(timerService.currentWatermark())
                                        + " at " + new Timestamp(currentTimestamp));


        }

        @Override
        public void onTimer(long timestamp,OnTimerContext ctx,Collector<Input> out) throws Exception {

            // Get the state correspond to this timestamp
            List<Input> window_res = window_state.get(timestamp);

            // Emit the results
            for (Input input : window_res) { out.collect(input); }

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
    public static final class TumblingProcessingTimeWindow extends KeyedProcessFunction<String,Input,Input> {

        // Window size to milliseconds
        private final long windowSize;

        // Keyed, managed state, with an entry for each window, keyed by the window's end time
        // MapState => key correspond to window's end time and value correspond to elements which belongs to this event time window
        private transient MapState<Long, List<Input>> window_state;

        // Constructor
        public TumblingProcessingTimeWindow(Time windowSize){ this.windowSize = windowSize.toMilliseconds(); }

        @Override
        public void open(Configuration config){

            // Initialize the descriptor for window state

            // Map state
            MapStateDescriptor<Long, List<Input>> mapDesc =  new MapStateDescriptor<>("window_state", TypeInformation.of(Long.class), Types.LIST(Types.POJO(Input.class)) );

            window_state = getRuntimeContext().getMapState(mapDesc);
        }

        @Override
        public void processElement(Input input, Context ctx, Collector<Input> out) throws Exception {

            // Get a time service
            TimerService timerService = ctx.timerService();

            // Find the end of window for this input
            long endOfWindow = (ctx.timerService().currentProcessingTime() - (ctx.timerService().currentProcessingTime() % windowSize) + windowSize - 1);

            // Register a timer/callback for when the window has been completed
            ctx.timerService().registerProcessingTimeTimer(endOfWindow);

            List<Input> list;

            // Get the window state of input
            if( window_state.get(endOfWindow) == null ){

                // Initialize the list for this key
                list = new ArrayList<>();

            }
            else{ list = window_state.get(endOfWindow); }

            // Add element to list and change the timestamp to end_window
            input.setTimestamp(endOfWindow);
            list.add(input);

            // Update the state
            window_state.put(endOfWindow,list);

            // Current processing time
            long currentTimestamp = System.currentTimeMillis();

            // Emit to side output
            ctx.output(window_stats,"Window-Worker " + ctx.getCurrentKey()
                                        + " , timestamp_input : " + new Timestamp(input.getTimestamp())
                                        + " , endOfWindow : " + new Timestamp(endOfWindow)
                                        + " , ctx_timestamp : " + new Timestamp(ctx.timestamp() == null ? 0 : ctx.timestamp())
                                        + " , current_processing_time : "+ new Timestamp(timerService.currentProcessingTime())
                                        + " , current watermark : "+ new Timestamp(timerService.currentWatermark())
                                        + " at " + new Timestamp(currentTimestamp));

        }

        @Override
        public void onTimer(long timestamp,OnTimerContext ctx,Collector<Input> out) throws Exception {

            // Get the state correspond to this timestamp
            List<Input> window_res = window_state.get(timestamp);

            // Emit the results
            for (Input input : window_res) { out.collect(input); }

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


    // Count tumbling window
    // Use the following format : process(new CountTumblingWindow(Time.milliseconds(Long.MAX_VALUE),10));
    public static final class CountTumblingWindow extends KeyedProcessFunction<String,Input,Input>{

        // Window size
        private final long windowSize;

        // Timeout
        private final long timeout;

        // Keep the elements of window before emitted to the next operator
        private transient ValueState<List<Input>> window_state;

        // Constructor
        public CountTumblingWindow(Time timeout,long windowSize){
            this.timeout = timeout.toMilliseconds();
            this.windowSize = windowSize;
        }

        @Override
        public void open(Configuration config){

            // Initialize the descriptor for window state

            // Value state
            ValueStateDescriptor<List<Input>> valueDesc =  new ValueStateDescriptor<>("window_state", Types.LIST(Types.POJO(Input.class)));

            window_state = getRuntimeContext().getState(valueDesc);
        }

        @Override
        public void processElement(Input input, Context ctx, Collector<Input> out) throws Exception {

            // Get a time service
            TimerService timerService = ctx.timerService();

            // Register a timer for the timeout(This callback is valid only one time per worker)
            ctx.timerService().registerEventTimeTimer(timeout);

            List<Input> list; // window's values

            // Get the window state
            if( window_state.value() == null ){
                // Initialize the list for this key
                list = new ArrayList<>();
            }
            else{ list = new ArrayList<>(window_state.value()); }

            // Add element to list
            list.add(input);

            if(list.size() == windowSize){

                // Emit the results
                for (Input in : list) { out.collect(in); }

                // Clear the window state
                window_state.clear();

                // Collect some statistics

                // Current processing time
                long currentTimestamp = System.currentTimeMillis();

                // Emit to side output
                ctx.output(window_stats,"Window-Worker " + ctx.getCurrentKey()
                                            + " , timestamp_input : " + new Timestamp(input.getTimestamp())
                                            + " , count of window : " + list.size()
                                            + " , ctx_timestamp : " + new Timestamp(ctx.timestamp() == null ? 0 : ctx.timestamp())
                                            + " , current_processing_time : "+ new Timestamp(timerService.currentProcessingTime())
                                            + " , current watermark : "+ new Timestamp(timerService.currentWatermark())
                                            + " at " + new Timestamp(currentTimestamp));
            }
            else{

                // Update the state
                window_state.update(list);
            }
        }

        @Override
        public void onTimer(long timestamp,OnTimerContext ctx,Collector<Input> out) throws Exception {

            // Empty window
            if(window_state.value() == null) return;

            // Get the window state
            List<Input> window_res = new ArrayList<>(window_state.value());

            // Emit the results
            for (Input input : window_res) { out.collect(input); }

            // Remove window
            window_state.clear();

            // Get a timer service
            TimerService timerService = ctx.timerService();

            // Current processing time
            long currentTimestamp = System.currentTimeMillis();

            ctx.output(window_stats, "\nOn timer-Worker "   + ctx.getCurrentKey()
                                        + " , timestamp : " + new Timestamp(timestamp)
                                        + " , ctx_timestamp : " + new Timestamp(ctx.timestamp() == null ? 0 : ctx.timestamp())
                                        + " , current_processing_time : " + new Timestamp(timerService.currentProcessingTime())
                                        + " , current watermark : " + new Timestamp(timerService.currentWatermark())
                                        + " , window's size : " + window_res.size()
                                        + " at " + new Timestamp(currentTimestamp)+"\n");

        }
    }

    // Count Process window function , simply emit the results of window
    // Combined with countWindow(windowSize)
    // Use the following format : countWindow(10).process(new CountProcessWindowFunction())
    // or window(GlobalWindows.create()).trigger(new CountTrigger(10,Long.MAX_VALUE)).process(new CountProcessWindowFunction()) with custom CountTrigger
    public static final class CountProcessWindowFunction extends ProcessWindowFunction<Input,Input,String,GlobalWindow>{

        @Override
        public void process(String key, Context ctx, Iterable<Input> iterable, Collector<Input> out) throws Exception {

            int window_size = 0;

            for(Input input : iterable) {
                window_size++;
                out.collect(input);
            }

            // Collect some statistics

            // Current processing time
            long currentTimestamp = System.currentTimeMillis();

            ctx.output(window_stats, "\nOn process-Worker "   + key
                                        + " , current_processing_time : " + new Timestamp(ctx.currentProcessingTime())
                                        + " , current watermark : " + new Timestamp(ctx.currentWatermark())
                                        + " , window's size : " + window_size
                                        + " at " + new Timestamp(currentTimestamp)+"\n");
        }
    }

    // Count trigger with timeout
    // Use the following format : window(GlobalWindows.create()).trigger(new CountTrigger(10,Long.MAX_VALUE)).process(new CountProcessWindowFunction())
    public static final class CountTrigger extends Trigger<Input,GlobalWindow>{

        private final long count; // The count which is triggered the window
        private final long timeout; // The timeout which is triggered the window

        // State object used to store the current amount of data in the window
        private final ValueStateDescriptor<Long> countStateDescriptor = new ValueStateDescriptor<>("counter",Long.class);

        public CountTrigger(long count,long timeout){
            this.count = count;
            this.timeout = timeout;
        }

        // Getters
        public long getCount() { return count; }
        public long getTimeout() { return timeout; }

        @Override
        public TriggerResult onElement(Input input, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {

            // Get the state
            ValueState<Long> countState = ctx.getPartitionedState(countStateDescriptor);

            // Increment appropriately the count state
            if( countState.value() == null ) countState.update(1L);
            else{ countState.update(countState.value()+1L); }

            // Register an event time timer for timeout(Long.MAX_VALUE)
            ctx.registerEventTimeTimer(timeout);

            // Check the count
            if( countState.value() >= count ){

                // Reset the count state
                clear(window,ctx);

                // Fire and purge the window
                return TriggerResult.FIRE_AND_PURGE;
            }
            else{ return TriggerResult.CONTINUE; }

        }

        @Override
        public TriggerResult onProcessingTime(long timestamp, GlobalWindow window, TriggerContext ctx) throws IOException {

            // Get the state
            ValueState<Long> countState = ctx.getPartitionedState(countStateDescriptor);
            if(countState.value() == null) return TriggerResult.CONTINUE;
            else{
                // Reset the count state
                clear(window,ctx);

                // Fire and purge the window
                return TriggerResult.FIRE_AND_PURGE;
            }
        }

        @Override
        public TriggerResult onEventTime(long timestamp, GlobalWindow window, TriggerContext ctx) throws IOException {

            // Get the state
            ValueState<Long> countState = ctx.getPartitionedState(countStateDescriptor);

            if(countState.value() == null) return TriggerResult.CONTINUE;
            else{

                // Reset the count state
                clear(window,ctx);

                // Fire and purge the window
                return TriggerResult.FIRE_AND_PURGE;
            }

        }

        @Override
        public void clear(GlobalWindow window, TriggerContext ctx){ ctx.getPartitionedState(countStateDescriptor).clear(); }
    }

}
