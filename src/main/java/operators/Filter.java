package operators;

import datatypes.input.Input;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import static utils.Util.encode;

public class Filter {

    private final int numOfWorkers; // Number of workers-sites

    // Constructors
    public Filter(int numOfWorkers){ this.numOfWorkers = numOfWorkers; }

    // Methods
    public void filterAndWrite(SingleOutputStreamOperator<Input> inputStream){

        for(int i=0;i<numOfWorkers;i++){

            int finalI = i;

            inputStream
            .filter((FilterFunction<Input>) input -> input.getKey().equals(String.valueOf(finalI)))
            .writeAsText("input_" + i + ".txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("Window statistics");


        }

    }

    public void filterAndWriteQuerySource(SingleOutputStreamOperator<Input> inputStream){
        inputStream.writeAsText("querySourceStatistics", FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("Query Source statistics");
    }

    public void filterAndWriteWorkers(DataStream<String> inputStream){

        for(int i=0;i<numOfWorkers;i++){

            int finalI = i;

            inputStream
            .filter((FilterFunction<String>) input -> input.contains("Worker "+finalI))
            .writeAsText("worker_stats_"+i+".txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("Workers statistics");


        }

    }

    public void filterAndWriteWorkers(DataStream<String> inputStream,String workerPath) {

        // String encodedWorkerPath = encode(workerPath);

        for(int i=0;i<numOfWorkers;i++){

            int finalI = i;
            String finalSpace = " ";

            inputStream
                    .filter((FilterFunction<String>) input -> input.contains("Worker "+finalI+finalSpace))
                    .writeAsText(workerPath+"worker_stats_"+i+".txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("Workers statistics");


        }

    }

    public void filterAndWriteCoordinator(DataStream<String> inputStream){
        inputStream.writeAsText("coordinator_stats.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("Coordinator statistics");
    }

    public void filterAndWriteCoordinator(DataStream<String> inputStream,String coordinatorPath) {

        // String encodedCoordinatorPath = encode(coordinatorPath);
        inputStream.writeAsText(coordinatorPath+"coordinator_stats.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("Coordinator statistics");
    }

}
