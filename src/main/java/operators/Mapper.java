package operators;

import datatypes.input.Input;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Random;

public class Mapper {

    /**
     * Mapper which generate a triplet with first field correspond to value,second field correspond to worker and third field correspond to event timestamp
     */
    public static class MapperInput extends RichMapFunction<String, Input>{

        private final int workers; // Number of workers
        private transient long lastTimestamp; // The timestamp of input

        // Constructor
        public MapperInput(int workers){ this.workers = workers; }

        @Override
        public void open(Configuration config){
            // Initialize the timestamp
            lastTimestamp = System.currentTimeMillis();
        }

        @Override
        public Input map(String input) {

            // Update the last timestamp
            lastTimestamp += 10;
            return new Input(input, String.valueOf(new Random().nextInt(workers)),lastTimestamp);
        }
    }

    public static class FlatMapperInput extends RichFlatMapFunction<String,Input>{

        private final int workers; // Number of workers
        private transient long lastTimestamp; // The timestamp of input

        // Constructor
        public FlatMapperInput(int workers){ this.workers = workers; }

        @Override
        public void open(Configuration config){

            // Initialize the timestamp
            lastTimestamp = System.currentTimeMillis();
        }

        @Override
        public void flatMap(String input, Collector<Input> out){

            // Update the last timestamp
            lastTimestamp += 10;

            // End of file
            if( input.equals("EOF") ){
                // Broadcast EOF to all workers
                for (int i=0;i<workers;i++){ out.collect(new Input(input,String.valueOf(i),System.currentTimeMillis())); }
            }
            else out.collect(new Input(input, String.valueOf(new Random().nextInt(workers)),System.currentTimeMillis()));
        }
    }

    // Return a triplet of the following formation : <value,worker,timestamp>
    public static class FlatMapperStrToInput extends RichFlatMapFunction<String,Input>{

        private final int workers; // Number of workers

        // Constructor
        public FlatMapperStrToInput(int workers){
            this.workers = workers;
        }

        @Override
        public void open(Configuration config){}

        @Override
        public void flatMap(String input, Collector<Input> out){

            // End of file
            if( input.equals("EOF") ){
                // Broadcast EOF to all workers
                for (int i=0;i<workers;i++){ out.collect(new Input(input,String.valueOf(i),System.currentTimeMillis())); }
            }
            else out.collect(new Input(input, String.valueOf(new Random().nextInt(workers)),System.currentTimeMillis()));
        }
    }
}
