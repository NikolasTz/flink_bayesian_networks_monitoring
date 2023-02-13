package fgm.sketch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import config.InternalConfig;
import datatypes.NodeBN_schema;
import datatypes.counter.Counter;
import datatypes.input.Input;
import datatypes.message.Message;
import fgm.datatypes.VectorType;
import net.openhft.hashing.LongHashFunction;

import java.io.*;
import java.lang.instrument.Instrumentation;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static config.InternalConfig.TypeMessage.INCREMENT;
import static config.InternalConfig.coordinatorKey;

import static datatypes.counter.Counter.findCardinality;
import static fgm.coordinator.CoordinatorFunction.checkParameter;
import static fgm.sketch.CountMin.calculateDepth;
import static fgm.sketch.CountMin.calculateWidth;
import static fgm.state.CoordinatorState.*;
import static fgm.worker.WorkerFunction.getKeyNBC;
import static operators.FeatureHashing.*;
import static state.State.*;
import static state.State.TypeNode.COORDINATOR;
import static state.State.TypeNode.WORKER;
import static state.State.hashKey;
import static utils.MathUtils.*;
import static utils.MathUtils.log;
import static utils.Util.*;
import static utils.Util.longToBytesBE;

public class TestCountMin {

    public static long totalCount = 0;
    public static Instrumentation instrumentation;

    public static void main(String[] args) throws Exception {

        /*  System.out.println("Char : "+ Character.BYTES);
            System.out.println("Int : "+ Integer.BYTES);
            System.out.println("Long : "+ Long.BYTES);
            System.out.println("Float : "+ Float.BYTES);
            System.out.println("Double : "+ Double.BYTES);
        */

        // Test initialization(hepar2)
        int depth = calculateDepth(0.25,151);
        int width = calculateWidth(0.1,131,500000,depth,2816);
        // int width = calculateWidth(0.1,131,500000,1);
        // CountMin sketch = new CountMin(depth,width);
        CountMin sketch = new CountMin(3,751);
        // CountMin sketch = new CountMin(0.1,0.25);

        // Print sketch
        System.out.println(sketch);

        // Test initialization with epsilon and delta
        CountMin sketch1 = new CountMin(0.1,0.01);

        // Print sketch
        System.out.println(sketch1);

        // Test log of MathUtils
        double logV;
        logV = log(Math.exp(1)); logV = log(Math.exp(2)); logV = log(32.4d);
        logV = log10(10d); logV = log10(100d); logV = log10(1.1d);
        logV = log2(2d); logV = log2(8d); logV = log2(32d); logV = log2(1.5d);

        logV = convertLn(2);logV = convertLn(1);logV = convertLn(32.4);

        // Test Vector type

        // Vector

        /*VectorType v = new fgm.datatypes.Vector();
        int size = v.getSize();
        double[][] vec = v.get();

        // CountMin sketch
        VectorType vCM = new CountMin(4,10);
        int sizeVCM = vCM.getSize();
        double[][] vecVCM = vCM.get();

        // AGMS sketch
        AGMS agmsSketchVector = new AGMS(4,200);
        int sizeAGMS = agmsSketchVector.getSize();
        double[][] vectorAGMS = agmsSketchVector.get();

        // Update using Vector type
        Vector<Counter> countersVector = testUpdateVector(v);
        testVectorType(v);
        vec = v.get();

        // Estimation using Vector type
        testEstimationVector(v,countersVector,totalCount);
        totalCount = 0; // Reset the total count

        // Test AGMS_sketch
        AGMS agmsSketch = new AGMS(4,200);
        Vector<Counter> countersAgms = testUpdateAGMS(agmsSketch);
        testEstimationAGMS(agmsSketch,countersAgms,totalCount);
        totalCount = 0; // Reset the total count*/

        // Test AGMS_sketch
        // AGMS agmsSketch = new AGMS(4,351);
        // System.out.println(agmsSketch);
        // Vector<Counter> countersAgms = testUpdateAGMS(agmsSketch);
        // testEstimationAGMS(agmsSketch,countersAgms,totalCount);


        // Test Update
        // Vector<Counter> counters = testUpdate(sketch); // or testUpdate(sketch)
        // System.out.println("Total count : " + totalCount);

        // Test Estimation
        // testEstimation(sketch,counters,totalCount); // or  testEstimation(sketch,counters,totalCount)
        // LinkedHashMap<Long,Counter> counters = testProcessingTime();
        // testErrorGT();

        // collectStatBN("hepar2",0.1d,0.25d,true);
        LinkedHashMap<Long,Counter> countersSketch = testUpdateAGMSMod();
        // LinkedHashMap<Long,Counter> counters = testUpdateRCModF();
        // Vector<Counter> counters = testUpdateRC();
        // processingQueriesBN(counters,"querySource");


        // Test ARRAY and VECTOR UTILS
        /* boolean equal;

        double[][] scaledArray = scaleArray(sketch.getSketch(),2);

        // Row vector
        double[] rowVector = rowMajor(sketch.getSketch());

        // Scale vector
        double[] scaledVector = scaleVector(rowVector,2);

        // Column vector
        double[] columnVector = columnMajor(sketch.getSketch());

        // Add vector
        double[] added = addVectors(rowVector,columnVector);

        equal = compareVector(added,columnVector);
        double[] newVector = new double[3];
        equal = compareVector(added,newVector);

        // Norm
        // L1 norm
        double l1 = norm(rowVector,1);

        // L2 norm
        double l2 = norm(rowVector,2);

        // Conversion
        double[][] convertedRow = convert2D(rowVector,true,sketch.depth(),sketch.width());

        double[][] convertedColumn = convert2D(columnVector,false,sketch.depth(),sketch.width());

        equal = compareArrays(convertedRow,convertedColumn);

        // Add arrays
        // Add vector
        double[][] addedArrays = addArrays(convertedRow,convertedColumn);

        // Reset
        sketch.reset();

        // Empty instance
        double[][] sketch2 = sketch.emptyInstance();

        double[][] sketch3 = sketch.emptyInstance();

        equal= compareArrays(sketch3,sketch2);
        equal = compareArrays(sketch3,sketch3);
        equal = compareArrays(addedArrays,sketch2);
        */

    }


    // Update process

    // Update Bayesian Network(BN)
    public static Vector<Counter> testUpdate(CountMin cm) throws IOException {

        // Bayesian Network
        // String jsonBN = "[{\"name\":\"Alarm\",\"trueParameters\":[\"0.95,0.94,0.29,0.001\",\"0.05,0.06,0.71,0.999\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[{\"name\":\"Burglary\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]},{\"name\":\"Earthquake\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]}] },{\"name\":\"Burglary\",\"trueParameters\":[\"0.01\",\"0.99\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[ ] },{\"name\":\"Earthquake\",\"trueParameters\":[\"0.02\",\"0.98\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[ ] },{\"name\":\"JohnCalls\",\"trueParameters\":[\"0.9,0.05\",\"0.1,0.95\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[{\"name\":\"Alarm\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]}] },{\"name\":\"MaryCalls\",\"trueParameters\":[\"0.7,0.01\",\"0.3,0.99\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[{\"name\":\"Alarm\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]}] }]";
        ObjectMapper objectMapper = new ObjectMapper();


        // Unmarshall as array
        NodeBN_schema[] nodes = objectMapper.readValue(new File("src\\main\\java\\bayesianNetworks\\bn_JSON_hepar2.json"), NodeBN_schema[].class);
        // objectMapper.readValue(jsonBN, NodeBN_schema[].class); // Read value from string

        // Initialize all the counters of BN
        Vector<Counter> counters = new Vector<>();
        for (NodeBN_schema node : nodes) { counters.addAll(initializeCounters(node)); }

        // The schema of dataset
        String datasetSchema = new BufferedReader(new FileReader("src\\main\\java\\bayesianNetworks\\dataset_schema_hepar2")).readLine();
        // String datasetSchema = "Burglary,Earthquake,Alarm,JohnCalls,MaryCalls";
        List<String> schema = Arrays.asList(datasetSchema.split(","));

        // Read the file line by line
        BufferedReader br = new BufferedReader(new FileReader("datasets\\data_hepar20_5000000"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("testing", true));

        // Find cardinality of each parameter
        int card = 0;
        for(Counter counter : counters){ card = findCardinality(counters,counter); }

        int c = findNumUpdates(counters);

        String line = br.readLine();

        while( line != null && !line.equals("EOF") ) {

            List<String> inputValues = Arrays.asList(line.split(","));

            for (Counter counter : counters) {
                if (checkIncrement(inputValues, counter, schema)) {

                    // Get the key(nodeNames,nodeValues,nodeParentsNames,nodeParentsValues)
                    String key = getKey(counter);

                    // Update using HashCode
                    // cm.updateHashCode(key);

                    // Update using Array HashCode
                    // cm.updateArrayHashCode(key);

                    // Update using hash function
                    // cm.updateHash(key);

                    // Update using murmur3
                    cm.updateMurmur(key);

                    // Update using polyMurmur3
                    // cm.updatePolyMurmur(key);

                    // Update using CW
                    // cm.updateCW(key);

                    // Update using CWS
                    // cm.updateCWS(key);

                    // Update using polynomial rolling hash function
                    // cm.updatePolyRollingHash(key);

                    // Update the counter
                    counter.setCounter(counter.getCounter()+1L);

                    totalCount++;
                }
            }

            // Write the line to file
            /*List<String> testSchema = new ArrayList<>(); List<String> output = new ArrayList<>(); int index = 0;
            testSchema.add("B"); testSchema.add("E"); testSchema.add("A"); testSchema.add("J"); testSchema.add("M");
            for(String value : inputValues){ output.add(testSchema.get(index)+value); index++; }
            writer.write(output+"\n");*/

            // Read the next line
            line = br.readLine();

        }

        // Close BufferedReader and BufferedWriter
        br.close();
        writer.close();

        System.out.println("Number of collisions : " + cm.getNumOfCollisions());
        return counters;
    }

    // Update for Naive Bayes Classifiers(NBC)
    public static Vector<Counter> testUpdateNBC(CountMin cm) throws IOException {

        // Bayesian Network
        String jsonBN = "[{\"name\":\"outlook\",\"trueParameters\":[],\"cardinality\":3,\"values\":[\"Sunny\",\"Overcast\",\"Rain\"],\"parents\":[]},{\"name\":\"temp\",\"trueParameters\":[],\"cardinality\":3,\"values\":[\"Hot\",\"Mild\",\"Cool\"],\"parents\":[]},{\"name\":\"humidity\",\"trueParameters\":[],\"cardinality\":2,\"values\":[\"High\",\"Normal\"],\"parents\":[ ] },{\"name\":\"wind\",\"trueParameters\":[],\"cardinality\":2,\"values\":[\"Weak\",\"Strong\"],\"parents\":[]},{\"name\":\"play\",\"trueParameters\":[],\"cardinality\":2,\"values\":[\"Yes\",\"No\"],\"parents\":[]}]";
        ObjectMapper objectMapper = new ObjectMapper();

        // Unmarshall as array
        NodeBN_schema[] nodes = objectMapper.readValue(jsonBN, NodeBN_schema[].class); // Read value from string
        // Convention : The last node correspond to the class node ???
        // Class counter/parameter => nodeParents and nodeParentsValues = null ???

        // Initialize all the counters of BN
        Vector<Counter> counters = new Vector<>();
        for (NodeBN_schema node : nodes) {
            if(node.equals(nodes[nodes.length-1])){
                counters.addAll(initializeCountersNBC(null,node)); // Class node
                continue;
            }
            counters.addAll(initializeCountersNBC(node,nodes[nodes.length-1]));
        }

        // The schema of dataset
        // The last field of dataset correspond to class ???
        String datasetSchema = "outlook,temp,humidity,wind,play";
        List<String> schema = Arrays.asList(datasetSchema.split(","));

        // Read the file line by line
        BufferedReader br = new BufferedReader(new FileReader("play_tennis.csv"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("testing_tennis", true));

        String line = br.readLine();

        // Find the number of updates per input
        int classValues = findClassValue(counters);

        // Find cardinality of each parameter
        int card = 0;
        for(Counter counter : counters){
            card = findCardinality(counters,counter);
        }

        while( line != null ) {

            List<String> inputValues = Arrays.asList(line.split(","));

            for (Counter counter : counters) {

                boolean check = checkParameter(inputValues,counter,schema);

                if (checkIncrement(inputValues, counter, schema)) {

                    // Get the key
                    String key = getKeyNBC(findIndex(schema,counter.getNodeName().get(0)),counter); // or getKey(counter);

                    // Update using HashCode
                    // cm.updateHashCode(key);

                    // Update using Array HashCode
                    // cm.updateArrayHashCode(key);

                    // Update using hash function
                    // cm.updateHash(key);

                    // Update using murmur3
                    cm.updateMurmur(key);

                    // Update using polyMurmur3
                    // cm.updatePolyMurmur(key);

                    // Update using CW
                    // cm.updateCW(key);

                    // Update using CWS
                    // cm.updateCWS(key);

                    // Update using polynomial rolling hash function
                    // cm.updatePolyRollingHash(key);

                    // Update the counter
                    counter.setCounter(counter.getCounter()+1L);

                    totalCount++;
                }
            }

            // Write the line to file
            // List<String> testSchema = new ArrayList<>(); List<String> output = new ArrayList<>(); int index = 0;
            // testSchema.add("B"); testSchema.add("E"); testSchema.add("A"); testSchema.add("J"); testSchema.add("M");
            // for(String value : inputValues){ output.add(testSchema.get(index)+value); index++; }
            // writer.write(output+"\n");

            // Read the next line
            line = br.readLine();

        }

        // Close BufferedReader and BufferedWriter
        br.close();
        writer.close();

        System.out.println("Number of collisions : " + cm.getNumOfCollisions());
        return counters;
    }

    // Update Bayesian Network using feature hashing before(BN)
    public static Vector<Counter> testUpdateFH(CountMin cm) throws IOException {

        // Bayesian Network
        String jsonBN = "[{\"name\":\"Alarm\",\"trueParameters\":[\"0.95,0.94,0.29,0.001\",\"0.05,0.06,0.71,0.999\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[{\"name\":\"Burglary\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]},{\"name\":\"Earthquake\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]}] },{\"name\":\"Burglary\",\"trueParameters\":[\"0.01\",\"0.99\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[ ] },{\"name\":\"Earthquake\",\"trueParameters\":[\"0.02\",\"0.98\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[ ] },{\"name\":\"JohnCalls\",\"trueParameters\":[\"0.9,0.05\",\"0.1,0.95\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[{\"name\":\"Alarm\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]}] },{\"name\":\"MaryCalls\",\"trueParameters\":[\"0.7,0.01\",\"0.3,0.99\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[{\"name\":\"Alarm\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]}] }]";
        ObjectMapper objectMapper = new ObjectMapper();

        // Unmarshall as array
        NodeBN_schema[] nodes = objectMapper.readValue(jsonBN, NodeBN_schema[].class); // Read value from string

        // Initialize all the counters of BN
        Vector<Counter> counters = new Vector<>();
        for (NodeBN_schema node : nodes) { counters.addAll(initializeCounters(node)); }

        // The schema of dataset
        String datasetSchema = "Burglary,Earthquake,Alarm,JohnCalls,MaryCalls";
        List<String> schema = Arrays.asList(datasetSchema.split(","));

        // Read the file line by line
        BufferedReader br = new BufferedReader(new FileReader("data_earthquake_1000"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("testing", true));

        String line = br.readLine();

        // Find the number of updates per input
        int numUpdates = findNumUpdates(counters);
        int retCounters = findNumRetCounts(counters);

        while( line != null ) {

            List<String> inputValues = Arrays.asList(line.split(","));
            long[] hashedVector = new long[4];

            for (Counter counter : counters) {
                if (checkIncrement(inputValues, counter, schema)) {

                    // Get the key
                    String key = getKey(counter);

                    // Update the hashedVector
                    hashedVector[(int) (hashing(key) % hashedVector.length)] += 1;
                    // hashingVectorized(key,4,hashedVector);
                    // hashingVector(new Input(key,"0",0),4);
                    // hashingVectorized(new Input(key,"0",0),4);

                    // Update using murmur3
                    // long[] keys = hashingVector(new Input(key,"0",0),4);
                    // long sum = Arrays.stream(keys).sum();
                    // cm.updateMurmur(longToBytesBE(sum));

                    // for (long hashedKey : keys) {
                    //    byte[] bytes1 = longToBytesBE(hashedKey);
                        // byte[] bytes2 = longToByteArrayLE(hashedKey);
                    //    cm.updateMurmur(bytes1);
                    //}


                    // Update the counter
                    counter.setCounter(counter.getCounter()+1L);
                    totalCount++;
                }
            }


            // Feature hashing
            long[] vector = hashingVector(new Input(line,"0",0),4);


            // Update the sketch
            for (long key : hashedVector) {
                byte[] bytes = longToBytesBE(key);
                cm.updateMurmur(bytes);
            }

            // Write the line to file
            // List<String> testSchema = new ArrayList<>(); List<String> output = new ArrayList<>(); int index = 0;
            // testSchema.add("B"); testSchema.add("E"); testSchema.add("A"); testSchema.add("J"); testSchema.add("M");
            // for(String value : inputValues){ output.add(testSchema.get(index)+value); index++; }
            // writer.write(output+"\n");

            // Read the next line
            line = br.readLine();

        }

        // Close BufferedReader and BufferedWriter
        br.close();
        writer.close();

        System.out.println("Number of collisions : " + cm.getNumOfCollisions());
        return counters;
    }

    // Update for Naive Bayes Classifiers with Feature Hashing(NBC)
    public static Vector<Counter> testUpdateNBCFH(CountMin cm) throws IOException {

        // Bayesian Network
        String jsonBN = "[{\"name\":\"outlook\",\"trueParameters\":[],\"cardinality\":3,\"values\":[\"Sunny\",\"Overcast\",\"Rain\"],\"parents\":[]},{\"name\":\"temp\",\"trueParameters\":[],\"cardinality\":3,\"values\":[\"Hot\",\"Mild\",\"Cool\"],\"parents\":[]},{\"name\":\"humidity\",\"trueParameters\":[],\"cardinality\":2,\"values\":[\"High\",\"Normal\"],\"parents\":[ ] },{\"name\":\"wind\",\"trueParameters\":[],\"cardinality\":2,\"values\":[\"Weak\",\"Strong\"],\"parents\":[]},{\"name\":\"play\",\"trueParameters\":[],\"cardinality\":2,\"values\":[\"Yes\",\"No\"],\"parents\":[]}]";
        ObjectMapper objectMapper = new ObjectMapper();

        // Unmarshall as array
        NodeBN_schema[] nodes = objectMapper.readValue(jsonBN, NodeBN_schema[].class); // Read value from string
        // Convention : The last node correspond to the class node ???
        // Class counter/parameter => nodeParents and nodeParentsValues = null ???

        // Initialize all the counters of BN
        Vector<Counter> counters = new Vector<>();
        for (NodeBN_schema node : nodes) {
            if(node.equals(nodes[nodes.length-1])){
                counters.addAll(initializeCountersNBC(null,node)); // Class node
                continue;
            }
            counters.addAll(initializeCountersNBC(node,nodes[nodes.length-1]));
        }

        // The schema of dataset
        // The last field of dataset correspond to class ???
        String datasetSchema = "outlook,temp,humidity,wind,play";
        List<String> schema = Arrays.asList(datasetSchema.split(","));

        // Read the file line by line
        BufferedReader br = new BufferedReader(new FileReader("play_tennis.csv"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("testing_tennis", true));

        String line = br.readLine();

        // Find the value of class node
        int classValues = findClassValue(counters);

        while( line != null ) {

            List<String> inputValues = Arrays.asList(line.split(","));

            for (Counter counter : counters) {

                boolean check = checkParameter(inputValues,counter,schema);

                if (checkIncrement(inputValues, counter, schema)) {

                    // Get the key
                    String key = getKeyNBC(findIndex(schema,counter.getNodeName().get(0)),counter); // or getKey(counter);

                    // Update the counter
                    counter.setCounter(counter.getCounter()+1L);

                    totalCount++;
                }
            }

            // Feature hashing on input
            long[] hashedVector = hashingVector(new Input(line,"0",0),3); // or hashingVectorized()

            // Update the sketch
            for(int i=0;i<hashedVector.length-1;i++){

                // Generate the key = <index,value,class>
                String key = String.valueOf(i).concat(",").concat(String.valueOf(hashedVector[i])).concat(",").concat(inputValues.get(inputValues.size()-1));

                // Update the sketch
                cm.updateMurmur(key);
            }

            // Write the line to file
            // List<String> testSchema = new ArrayList<>(); List<String> output = new ArrayList<>(); int index = 0;
            // testSchema.add("B"); testSchema.add("E"); testSchema.add("A"); testSchema.add("J"); testSchema.add("M");
            // for(String value : inputValues){ output.add(testSchema.get(index)+value); index++; }
            // writer.write(output+"\n");

            // Read the next line
            line = br.readLine();

        }

        // Close BufferedReader and BufferedWriter
        br.close();
        writer.close();

        System.out.println("Number of collisions : " + cm.getNumOfCollisions());
        return counters;
    }

    // Update Bayesian Network(BN) using AGMS-sketch
    public static Vector<Counter> testUpdateAGMS(AGMS agmsSketch) throws IOException {

        // Bayesian Network
        // String jsonBN = "[{\"name\":\"Alarm\",\"trueParameters\":[\"0.95,0.94,0.29,0.001\",\"0.05,0.06,0.71,0.999\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[{\"name\":\"Burglary\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]},{\"name\":\"Earthquake\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]}] },{\"name\":\"Burglary\",\"trueParameters\":[\"0.01\",\"0.99\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[ ] },{\"name\":\"Earthquake\",\"trueParameters\":[\"0.02\",\"0.98\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[ ] },{\"name\":\"JohnCalls\",\"trueParameters\":[\"0.9,0.05\",\"0.1,0.95\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[{\"name\":\"Alarm\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]}] },{\"name\":\"MaryCalls\",\"trueParameters\":[\"0.7,0.01\",\"0.3,0.99\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[{\"name\":\"Alarm\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]}] }]";
        ObjectMapper objectMapper = new ObjectMapper();

        // Unmarshall as array
        // NodeBN_schema[] nodes = objectMapper.readValue(jsonBN, NodeBN_schema[].class); // Read value from string
        NodeBN_schema[] nodes = objectMapper.readValue(new File("src\\main\\java\\bayesianNetworks\\bn_JSON_hepar2.json"), NodeBN_schema[].class);

        // Initialize all the counters of BN
        Vector<Counter> counters = new Vector<>();
        for (NodeBN_schema node : nodes) { counters.addAll(initializeCounters(node)); }

        // The schema of dataset
        String datasetSchema = new BufferedReader(new FileReader("src\\main\\java\\bayesianNetworks\\dataset_schema_hepar2")).readLine();
        List<String> schema = Arrays.asList(datasetSchema.split(","));

        // Read the file line by line
        BufferedReader br = new BufferedReader(new FileReader("datasets\\data_hepar20_500000"));
        // BufferedWriter writer = new BufferedWriter(new FileWriter("testing", true));

        String line = br.readLine();

        while( line != null && !line.equals("EOF") ) {

            List<String> inputValues = Arrays.asList(line.split(","));

            for (Counter counter : counters) {
                if (checkIncrement(inputValues, counter, schema)) {

                    // Get the key(nodeNames,nodeValues,nodeParentsNames,nodeParentsValues)
                    String key = getKey(counter);

                    // Update the sketch
                    agmsSketch.update(Murmur3.hash64(key.getBytes()),1.0d);

                    // Update the counter
                    counter.setCounter(counter.getCounter()+1L);

                    totalCount++;
                }
            }

            // Read the next line
            line = br.readLine();

        }

        // Close BufferedReader and BufferedWriter
        br.close();
        // writer.close();

        return counters;
    }

    // Update Bayesian Network(BN) using VectorType
    public static Vector<Counter> testUpdateVector(VectorType vector) throws IOException {

        // Bayesian Network
        String jsonBN = "[{\"name\":\"Alarm\",\"trueParameters\":[\"0.95,0.94,0.29,0.001\",\"0.05,0.06,0.71,0.999\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[{\"name\":\"Burglary\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]},{\"name\":\"Earthquake\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]}] },{\"name\":\"Burglary\",\"trueParameters\":[\"0.01\",\"0.99\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[ ] },{\"name\":\"Earthquake\",\"trueParameters\":[\"0.02\",\"0.98\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[ ] },{\"name\":\"JohnCalls\",\"trueParameters\":[\"0.9,0.05\",\"0.1,0.95\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[{\"name\":\"Alarm\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]}] },{\"name\":\"MaryCalls\",\"trueParameters\":[\"0.7,0.01\",\"0.3,0.99\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[{\"name\":\"Alarm\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]}] }]";
        ObjectMapper objectMapper = new ObjectMapper();

        // Unmarshall as array
        NodeBN_schema[] nodes = objectMapper.readValue(jsonBN, NodeBN_schema[].class); // Read value from string

        // Initialize all the counters of BN
        Vector<Counter> counters = new Vector<>();
        for (NodeBN_schema node : nodes) { counters.addAll(initializeCounters(node)); }

        // The schema of dataset
        String datasetSchema = "Burglary,Earthquake,Alarm,JohnCalls,MaryCalls";
        List<String> schema = Arrays.asList(datasetSchema.split(","));
        long sizeDataset = calculateSizeOfDataset(nodes,0.1,0.25,0.01);

        // Read the file line by line
        BufferedReader br = new BufferedReader(new FileReader("datasets\\data_earthquake_5000"));

        // Find cardinality of each parameter
        int card = 0;
        for(Counter counter : counters){ card = findCardinality(counters,counter); }

        // Initialize keys for vector
        for(Counter counter : counters){

            // Get the key
            String key = getKey(counter);

            // Add to vector
            vector.addEntry(key);
        }

        String line = br.readLine();

        while( line != null && !line.equals("EOF") ) {

            List<String> inputValues = Arrays.asList(line.split(","));

            for (Counter counter : counters) {
                if (checkIncrement(inputValues, counter, schema)) {

                    // Get the key(nodeNames,nodeValues,nodeParentsNames,nodeParentsValues)
                    String key = getKey(counter);

                    // Update using murmur3
                    vector.update(key);

                    // Update the counter
                    counter.setCounter(counter.getCounter()+1L);

                    totalCount++;
                }
            }

            // Read the next line
            line = br.readLine();

        }

        // Close BufferedReader
        br.close();
        return counters;
    }

    // Test update process on RC
    public static Vector<Counter> testUpdateRC() throws IOException {

        // Bayesian Network
        ObjectMapper objectMapper = new ObjectMapper();

        // Unmarshall as array
        NodeBN_schema[] nodes = objectMapper.readValue(new File("src\\main\\java\\bayesianNetworks\\bn_JSON_link.json"), NodeBN_schema[].class);

        // Initialize all the counters of BN
        Vector<Counter> counters = new Vector<>();
        for (NodeBN_schema node : nodes) { counters.addAll(initializeCounters(node)); }
        initializeExactCounter(counters,WORKER);

        // The schema of dataset
        String datasetSchema = new BufferedReader(new FileReader("src\\main\\java\\bayesianNetworks\\dataset_schema_link")).readLine();
        List<String> schema = Arrays.asList(datasetSchema.split(","));

        // Read the file line by line
        BufferedReader br = new BufferedReader(new FileReader("datasets\\data_link0_5000"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("coordinator_stats"));

        String line = br.readLine();
        int counterLines = 0;
        int times=1;

        System.out.println("Processing start at : "+System.currentTimeMillis());
        while( line != null && !line.equals("EOF") ) {

            List<String> inputValues = Arrays.asList(line.split(","));

            for (Counter counter : counters) {
                if (checkIncrement(inputValues, counter, schema)) {
                    incrementExactCounter(counter,System.currentTimeMillis());
                    totalCount++;
                }
            }

            counterLines++;

            if(counterLines % 500 == 0){
                System.out.println("Worker receive events at "+System.currentTimeMillis()+" , count : "+counterLines);
            }

            if(counterLines == times*100000){
                System.out.println("Processing 100000 lines");
                times++;
            }
            // System.out.println("Count : "+counterLines);

            // Write the line to file
            /*List<String> testSchema = new ArrayList<>(); List<String> output = new ArrayList<>(); int index = 0;
            testSchema.add("B"); testSchema.add("E"); testSchema.add("A"); testSchema.add("J"); testSchema.add("M");
            for(String value : inputValues){ output.add(testSchema.get(index)+value); index++; }
            writer.write(output+"\n");*/

            // Read the next line
            line = br.readLine();

        }
        System.out.println("Processing finished at : "+System.currentTimeMillis());


        // Collect statistics

        System.out.println("End of processing");
        writer.write("End of processing\n");
        AtomicInteger zero_counters = new AtomicInteger();
        counters.forEach( counter -> {
            try {
                writer.write(counter.toString()+"\n\n");
                if(counter.getCounter() == 0) zero_counters.getAndIncrement();
            }
            catch (IOException e) {e.printStackTrace();}
        });
        System.out.println("Total count : "+ totalCount);
        System.out.println("Zero counters : "+zero_counters.get());

        // Close BufferedReader and BufferedWriter
        br.close();
        writer.close();

        return counters;
    }
    public static Vector<Counter> testUpdateRCMod() throws IOException {

        // Bayesian Network
        ObjectMapper objectMapper = new ObjectMapper();

        // Unmarshall as array
        NodeBN_schema[] nodes = objectMapper.readValue(new File("src\\main\\java\\bayesianNetworks\\bn_JSON_earthquake.json"), NodeBN_schema[].class);

        // Initialize all the counters of BN
        Vector<Counter> counters = new Vector<>();
        for (NodeBN_schema node : nodes) { counters.addAll(initializeCounters(node)); }
        initializeExactCounter(counters,WORKER);

        // Alternative => HashMap
        HashMap<String,Counter> newCounters = new HashMap<>();
        for (Counter counter: counters){
            newCounters.put(getKey(counter),counter);
        }

        // The schema of dataset
        String datasetSchema = new BufferedReader(new FileReader("src\\main\\java\\bayesianNetworks\\dataset_schema_earthquake")).readLine();
        List<String> schema = Arrays.asList(datasetSchema.split(","));

        // Read the file line by line
        BufferedReader br = new BufferedReader(new FileReader("datasets\\data_earthquake_5000"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("coordinator_stats"));

        String line = br.readLine();
        int counterLines = 0;
        int times=1;

        while( line != null && !line.equals("EOF") ) {

            List<String> inputValues = Arrays.asList(line.split(","));

            for(int j=0;j<inputValues.size();j++){

                // Get the key from input
                String keyInput = schema.get(j)+inputValues.get(j);

                // Update the counter
                incrementExactCounter(newCounters.get(keyInput),System.currentTimeMillis());

                // Update the parents counters
                String parentKey = getParentsKey(newCounters.get(keyInput),schema,inputValues);
                if( parentKey != null){ incrementExactCounter(newCounters.get(parentKey),System.currentTimeMillis()); }

                totalCount++;
            }

            for (Counter counter : counters) {
                if (checkIncrement(inputValues, counter, schema)) {
                    incrementExactCounter(counter,System.currentTimeMillis());
                    totalCount++;
                }
            }

            counterLines++;
            if(counterLines == times*100000){
                System.out.println("Processing 100000 lines");
                times++;
            }
            // System.out.println("Count : "+counterLines);

            // Write the line to file
            /*List<String> testSchema = new ArrayList<>(); List<String> output = new ArrayList<>(); int index = 0;
            testSchema.add("B"); testSchema.add("E"); testSchema.add("A"); testSchema.add("J"); testSchema.add("M");
            for(String value : inputValues){ output.add(testSchema.get(index)+value); index++; }
            writer.write(output+"\n");*/

            // Read the next line
            line = br.readLine();

        }


        // Collect statistics
        System.out.println("End of processing");
        writer.write("End of processing\n");
        counters.forEach( counter -> {
            try { writer.write(counter.toString()+"\n\n"); }
            catch (IOException e) {e.printStackTrace();}
        });
        System.out.println("Total count : "+ totalCount);

        // Close BufferedReader and BufferedWriter
        br.close();
        writer.close();

        return counters;
    }
    public static LinkedHashMap<Long,Counter> testUpdateRCModF() throws IOException {

        // Bayesian Network
        ObjectMapper objectMapper = new ObjectMapper();

        // Unmarshall as array
        NodeBN_schema[] nodes = objectMapper.readValue(new File("src\\main\\java\\bayesianNetworks\\bn_JSON_hepar2.json"), NodeBN_schema[].class);

        // Initialize all the counters of BN
        Vector<Counter> counters = new Vector<>();
        for (NodeBN_schema node : nodes) { counters.addAll(initializeCounters(node)); }
        initializeExactCounter(counters,WORKER);
        // for(Counter counter: counters){ if(counter.isActualNode()) System.out.println(counter.toString()+"\n"); }


        // Alternative => HashMap
        LinkedHashMap<Long,Counter> newCounters = new LinkedHashMap<>();
        for (Counter counter: counters){

            // Get the key
            String key = getKey(counter);

            // Hash the key
            long hashedKey = hashKey(key.getBytes());

            // Update the worker state
            newCounters.put(hashedKey,counter);

            // Update the counter
            counter.setCounterKey(hashedKey);
        }

        // The schema of dataset
        String datasetSchema = new BufferedReader(new FileReader("src\\main\\java\\bayesianNetworks\\dataset_schema_hepar2")).readLine();
        LinkedHashMap<String,Integer> schema = new LinkedHashMap<>();
        int pos = 0;
        for(String node : datasetSchema.split(",")){

            // Update the schema
            schema.put(node,pos);

            // Update the position
            pos++;
        }

        // Read the file line by line
        BufferedReader br = new BufferedReader(new FileReader("datasets\\data_hepar20_5000"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("coordinator_stats"));

        String line = br.readLine();
        int counterLines = 0;
        int times=1;

        while( line != null && !line.equals("EOF") ) {

            // Skip the schema of dataset
            if( counterLines == 0 ){
                counterLines++;
                line = br.readLine();
                continue;
            }

            // Strips off all non-ASCII characters
            line = line.replaceAll("[^\\x00-\\x7F]", "");
            // Erases all the ASCII control characters
            line = line.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "");
            // Skip empty lines
            if(line.equals("")) {
                line = br.readLine();
                continue;
            }
            // Skip lines with dataset schema
            if(line.equals(datasetSchema)){
                line = br.readLine();
                continue;
            }

            List<String> inputValues = Arrays.asList(line.split(","));
            incrementCounters(inputValues,nodes,schema,newCounters);

            counterLines++;
            if(counterLines == times*100000){
                System.out.println("Processing 100000 lines");
                times++;
            }

            // Read the next line
            line = br.readLine();

        }

        // Collect statistics
        AtomicInteger zero_counters = new AtomicInteger();
        AtomicInteger zero_trueParameter = new AtomicInteger();
        System.out.println("End of processing");
        writer.write("End of processing\n");
        newCounters.values().forEach( counter -> {
            try {
                writer.write(counter.toString()+"\n\n");
                if(counter.getCounter() == 0) zero_counters.getAndIncrement();
                if(counter.getTrueParameter() == 0d && counter.isActualNode()) zero_trueParameter.getAndIncrement();
            }
            catch (IOException e) {e.printStackTrace();}
        });
        System.out.println("Total count : "+ totalCount);
        System.out.println("Zero counters : " + zero_counters.get());
        System.out.println("Zero true parameters : " + zero_trueParameter.get());

        // Close BufferedReader and BufferedWriter
        br.close();
        writer.close();

        return newCounters;
    }
    public static Vector<Counter> testSizeMessage() throws IOException, ClassNotFoundException {

        // Bayesian Network
        ObjectMapper objectMapper = new ObjectMapper();

        // Unmarshall as array
        NodeBN_schema[] nodes = objectMapper.readValue(new File("src\\main\\java\\bayesianNetworks\\bn_JSON_hepar2.json"), NodeBN_schema[].class);

        // Initialize all the counters of BN
        Vector<Counter> counters = new Vector<>();
        for (NodeBN_schema node : nodes) { counters.addAll(initializeCounters(node)); }
        initializeExactCounter(counters,WORKER);

        // The schema of dataset
        String datasetSchema = new BufferedReader(new FileReader("src\\main\\java\\bayesianNetworks\\dataset_schema_hepar2")).readLine();
        List<String> schema = Arrays.asList(datasetSchema.split(","));

        // Produce key for all counters
        // Key format: <nodeNames,nodeValues,nodeParentsNames,nodeParentsValues>
        ArrayList<String> keys = new ArrayList<>();
        ArrayList<Long> hashedKeys = new ArrayList<>();
        for(int i=0;i<counters.size();i++){

            // Get the key
            String key = getKey(counters.get(i));
            keys.add(key);

            // Hashing using Murmur3
            long hashedKey = Murmur3.hash64(key.getBytes());
            hashedKeys.add(hashedKey);

            // System.out.println("Key : "+key+" , hashedKey : "+hashedKey);

        }

        // Get the counter from key
        for(int i=0;i<counters.size();i++){

            // Get the hashedKey
            String key = getKey(counters.get(i));
            long hashedKey = Murmur3.hash64(key.getBytes());

            // Get the hashedKey from list
            long hashedKeyList = hashedKeys.get(i);

            // Compare
            if(hashedKey != hashedKeyList){
                System.out.println("Different keys");
                System.out.println("Counter : "+counters.get(i));
            }
        }

        // Check for duplicates keys
        if( hashedKeys.stream().distinct().count() != hashedKeys.size()){ System.out.println("Duplicates keys!!!"); }

        // Runtime for messages using optimized message
        OptimizedMessage optMessage = createOptMessage("0",counters.get(2815),INCREMENT,System.currentTimeMillis());
        System.out.println("Runtime before opt-search:"+System.currentTimeMillis());
        // long counterKey;
        for(Counter counter : counters){

            // Get key
            // System.out.println("Runtime for getKey before"+System.currentTimeMillis());
            String key = getKey(counter);
            // System.out.println("Runtime for getKey after"+System.currentTimeMillis());

            // Hashing
            // System.out.println("Runtime for hashing before"+System.currentTimeMillis());
            long counterKey = Murmur3.hash64(key.getBytes());
            // System.out.println("Runtime for hashing before"+System.currentTimeMillis());

            if(counterKey == optMessage.getKeyMessage()) {
                System.out.println("Runtime after opt-search:"+System.currentTimeMillis());
                break;
            }
        }

        // Runtime for messages using message
        Message message = createMessage("0",counters.get(2815),INCREMENT,System.currentTimeMillis());
        System.out.println("Runtime before search:"+System.currentTimeMillis());
        for(Counter counter : counters){
            // Get the counter
            if (findCounter(counter, message)) {
                System.out.println("Runtime after search:"+System.currentTimeMillis());
                break;
            }
        }

        // Read the file line by line
        BufferedReader br = new BufferedReader(new FileReader("datasets\\data_hepar20_5000"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("coordinator_stats"));

        String line = br.readLine();
        int counterLines = 0;
        int times=1;
        long totalSizeBytes = 0L;
        long totalSizeOptimizedBytes = 0L;

        while( line != null && !line.equals("EOF") ) {

            List<String> inputValues = Arrays.asList(line.split(","));

            for (Counter counter : counters) {
                if (checkIncrement(inputValues, counter, schema)) {
                    totalSizeBytes += incrementExactCounterSize(counter,System.currentTimeMillis());
                    totalSizeOptimizedBytes += incrementExactCounterSizeNew(counter,System.currentTimeMillis());
                    totalCount++;
                }
            }

            counterLines++;
            if(counterLines % 500 == 0){ System.out.println("Worker receive events at "+System.currentTimeMillis()+" , count : "+counterLines); }

            if(counterLines == times*100000){
                System.out.println("Processing 100000 lines");
                times++;
            }

            // Read the next line
            line = br.readLine();

        }


        // Collect statistics
        System.out.println("End of processing");
        writer.write("End of processing\n");
        counters.forEach( counter -> {
            try { writer.write(counter.toString()+"\n\n"); }
            catch (IOException e) {e.printStackTrace();}
        });

        System.out.println("Total count of message : "+ totalCount);
        System.out.println("Total size of message : "+ totalSizeBytes);
        System.out.println("Total size of opt message : "+ totalSizeOptimizedBytes);

        // Close BufferedReader and BufferedWriter
        br.close();
        writer.close();
        return counters;
    }
    public static LinkedHashMap<Long,Counter> testProcessingTime() throws IOException, ClassNotFoundException {

        // Bayesian Network
        ObjectMapper objectMapper = new ObjectMapper();

        // Unmarshall as array
        NodeBN_schema[] nodes = objectMapper.readValue(new File("src\\main\\java\\bayesianNetworks\\bn_JSON_link.json"), NodeBN_schema[].class);

        // Initialize all the counters of BN
        Vector<Counter> counters = new Vector<>();
        for (NodeBN_schema node : nodes) { counters.addAll(initializeCounters(node)); }
        initializeExactCounter(counters,WORKER);

        // The schema of dataset
        String datasetSchema = new BufferedReader(new FileReader("src\\main\\java\\bayesianNetworks\\dataset_schema_link")).readLine();
        LinkedHashMap<String,Integer> mod_schema = new LinkedHashMap<>();
        int value = 0;
        for(String node : datasetSchema.split(",")){
            mod_schema.put(node,value);
            value++;
        }

        LinkedHashMap<Long,Counter> new_counters = new LinkedHashMap<>();
        // Key format: <nodeNames,nodeValues,nodeParentsNames,nodeParentsValues>

        ArrayList<Long> hashedKeys = new ArrayList<>();
        for (Counter item : counters) {

            // Get the key
            String key = getKey(item);

            // Hashing using Murmur3
            // Murmur3.hash64(key.getBytes());
            // LongHashFunction.xx3().hashBytes(key.getBytes());
            long hashedKey = LongHashFunction.xx3().hashBytes(key.getBytes());
            hashedKeys.add(hashedKey);

            item.setCounterKey(hashedKey);
            new_counters.put(hashedKey, item);

        }

        // Check for duplicates keys
        if( hashedKeys.stream().distinct().count() != hashedKeys.size()){ System.out.println("Duplicates keys!!!"); }

        // Read the file line by line
        BufferedReader br = new BufferedReader(new FileReader("datasets\\data_link0_5000000"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("coordinator_stats"));

        String line = br.readLine();
        int counterLines = 0;
        int times=1;
        int totalUpdates = 0;

        System.out.println("Processing start at : "+System.currentTimeMillis());
        while( line != null && !line.equals("EOF") ) {

            List<String> inputValues = Arrays.asList(line.replaceAll(" ","").trim().split(","));

            // for Nodes_BN
            for(NodeBN_schema node : nodes ){

                // String buffer
                StringBuffer sb = new StringBuffer();

                // Get the name of node and appended
                String name = node.getName();
                sb.append(name);

                // Check if exists parents
                if(node.getParents().length != 0){

                    // Find the value of dataset_schema and appended to buffer
                    sb.append(",").append(inputValues.get(mod_schema.get(name)));

                    // Get the name and values of parents
                    StringBuilder nameParents = new StringBuilder();
                    StringBuilder valuesParents = new StringBuilder();
                    for(NodeBN_schema nodeParents : node.getParents()){
                        nameParents.append(nodeParents.getName()).append(",");
                        valuesParents.append(inputValues.get(mod_schema.get(nodeParents.getName()))).append(",");
                    }

                    // Replace the last comma
                    valuesParents.deleteCharAt(valuesParents.length()-1);

                    // Hashing the key
                    String key = sb.append(",").append(nameParents).append(valuesParents).toString().replace(" ","");

                    // Update the counter
                    // LongHashFunction.xx3().hashBytes(key.getBytes())
                    // Murmur3.hash64(key.getBytes())
                    Counter counter = new_counters.get(LongHashFunction.xx3().hashBytes(key.getBytes()));

                    // Increment
                    if( counter != null ){
                        totalUpdates++;

                        // Increment
                        incrementExactCounter(counter,System.currentTimeMillis());
                    }

                    // Update the parent counter
                    // Murmur3.hash64(parentKey.getBytes())
                    String parentKey = (nameParents.toString()+valuesParents+","+name).replace(" ","");
                    Counter parentCounter = new_counters.get(LongHashFunction.xx3().hashBytes(parentKey.getBytes()));

                    // Increment
                    if( parentCounter != null ){
                        totalUpdates++;

                        // Increment
                        incrementExactCounter(parentCounter,System.currentTimeMillis());
                    }

                }
                else{

                    // Find the values of dataset_schema
                    sb.append(",").append(inputValues.get(mod_schema.get(name)));

                    // Hashing the key
                    String key = sb.toString().replace(" ","");

                    // Update the counter
                    Counter counter = new_counters.get(LongHashFunction.xx3().hashBytes(key.getBytes()));
                    if( counter != null ){
                        totalUpdates++;

                        // Increment
                        incrementExactCounter(counter,System.currentTimeMillis());
                    }

                }

            }

            counterLines++;
            if(counterLines % 50000 == 0){ System.out.println("Worker receive events at "+System.currentTimeMillis()+" , count : "+counterLines); }

            if(counterLines == times*100000){
                System.out.println("Processing 100000 lines");
                times++;
            }

            // Read the next line
            line = br.readLine();

        }

        // Collect statistics
        System.out.println("Processing finished at : "+System.currentTimeMillis());
        System.out.println("End of processing");
        writer.write("End of processing\n");
        counters.forEach( counter -> {
            try { writer.write(counter.toString()+"\n\n"); }
            catch (IOException e) {e.printStackTrace();}
        });

        System.out.println("Total count : "+ totalUpdates);
        System.out.println("Total count : "+ totalCount);

        // Close BufferedReader and BufferedWriter
        br.close();
        writer.close();
        return new_counters;

    }
    public static class ArrayList1<E> extends java.util.ArrayList<E>{ }
    public static String getParentsKey(Counter counter,List<String> schema,List<String> inputValues){

        // Get the parents names
        ArrayList<String> parentsNames = counter.getNodeParents();
        ArrayList<String> parentsValues = new ArrayList<>();

        // Orphan nodes
        if(parentsNames == null) return null;

        // For each parent node get the value from input
        for(int j=0;j<schema.size();j++){
            for (String parentsName : parentsNames) {
                if (parentsName.equals(schema.get(j))) {
                    parentsValues.add(inputValues.get(j));
                    break;
                }
            }
        }

        // Return the key
        parentsNames.addAll(parentsValues);
        return convertToString(parentsNames).replace(" ","");

    }
    public static void initializeExactCounter(List<Counter> counters,TypeNode typeNode){

        // Initialize the counters
        for(Counter counter : counters){

            // Initialize epsilon,delta and rescaling constant
            counter.setEpsilon(0);
            counter.setDelta(0);
            counter.setResConstant(0);

            // Initialize the rest of the fields of counter
            initializeZeroCounter(counter,typeNode);
        }

    }
    public static void initializeZeroCounter(Counter counter, TypeNode typeNode){

        if( typeNode == COORDINATOR ){

            counter.setLastSentValues(new HashMap<>());
            counter.setLastSentUpdates(new HashMap<>());

            // Initialize the fields of CoordinatorCounter class
            for(int i=0;i<8;i++){
                counter.setLastSentValues(String.valueOf(i),0L);
                counter.setLastSentUpdates(String.valueOf(i),0L);
            }

        }

        counter.setCounter(0L); // Initialize the value of counter
        counter.setCounterDoubles(0L); // Initialize the value of counterDoubles
        counter.setLastBroadcastValue(0L);  // Initialize the value of lastBroadcastValue
        counter.setLastSentValue(0L); // Initialize the lastSentValue
        counter.setNumMessages(0L); // Initialize the number of messages
        counter.setProb(1.0d); // Initialize probability
        counter.setLastTs(0); // Initialize the last timestamp
        counter.setSync(true); // Initialize sync

    }
    public static void incrementExactCounter(Counter counter, long ts){

        // Increment the counter
        counter.setCounter(counter.getCounter()+1L);

        // Update the number of messages
        counter.setNumMessages(counter.getNumMessages()+1L);

        // Update the last timestamp
        counter.setLastTs(ts);

        totalCount++;
    }
    public static int incrementExactCounterSize(Counter counter, long ts) throws ClassNotFoundException, JsonProcessingException {

        // Increment the counter
        counter.setCounter(counter.getCounter()+1L);

        // Update the number of messages
        counter.setNumMessages(counter.getNumMessages()+1L);
        
        // Message
        Message message = createMessage("0",counter,INCREMENT,ts);
        
        // Update the last timestamp
        counter.setLastTs(ts);
        
        // Get the size
        ObjectMapper mapper = new ObjectMapper();
        byte[] value = mapper.writeValueAsBytes(message);
        return value.length*Byte.BYTES;

        // return (int) ObjectSizeFetcher.getObjectSize(message);
        // return message.size();
    }
    public static int incrementExactCounterSizeNew(Counter counter, long ts) throws ClassNotFoundException, JsonProcessingException {

        // Increment the counter
        counter.setCounter(counter.getCounter()+1L);

        // Update the number of messages
        counter.setNumMessages(counter.getNumMessages()+1L);

        // Message
        OptimizedMessage message = createOptMessage("0",counter,INCREMENT,ts);

        // Update the last timestamp
        counter.setLastTs(ts);

        // Get the size
        ObjectMapper mapper = new ObjectMapper();
        byte[] value = mapper.writeValueAsBytes(message);
        return value.length*Byte.BYTES;

        // return (int) instrumentation.getObjectSize(message);
        // return message.size();
    }
    public static OptimizedMessage createOptMessage(String key, Counter counter, InternalConfig.TypeMessage typeMessage, long timestamp){
        OptimizedMessage message = new OptimizedMessage(key,counter,typeMessage,counter.getCounter(),counter.getLastBroadcastValue());
        message.setTimestamp(timestamp); // Update the timestamp
        return message;
    }
    public static Message createMessage(String key, Counter counter, InternalConfig.TypeMessage typeMessage, long timestamp){
        Message message = new Message(key,counter,typeMessage,counter.getCounter(),counter.getLastBroadcastValue());
        message.setTimestamp(timestamp); // Update the timestamp
        return message;
    }
    public static class OptimizedMessage implements Serializable {

        private String worker; // Worker who sent the message
        private InternalConfig.TypeMessage typeMessage; // Type of message
        private long keyMessage; // Key of Message

        // One message for all type of counters
        private long value;
        private long lastBroadcastValue;
        private long timestamp;

        public OptimizedMessage(){}
        public OptimizedMessage(String worker, Counter counter, InternalConfig.TypeMessage typeMessage, long value, long lastBroadcastValue){

            this.worker = worker;
            this.typeMessage = typeMessage;
            this.keyMessage = Murmur3.hash64(getKey(counter).getBytes());
            this.value = value;
            this.lastBroadcastValue = lastBroadcastValue;
        }

        public String getWorker() { return worker; }
        public void setWorker(String worker) { this.worker = worker; }
        public InternalConfig.TypeMessage getTypeMessage() { return typeMessage; }
        public void setTypeMessage(InternalConfig.TypeMessage typeMessage) { this.typeMessage = typeMessage; }
        public long getKeyMessage() { return keyMessage; }
        public void setKeyMessage(long keyMessage) { this.keyMessage = keyMessage; }
        public long getValue() { return value; }
        public void setValue(long value) { this.value = value; }
        public long getLastBroadcastValue() { return lastBroadcastValue; }
        public void setLastBroadcastValue(long lastBroadcastValue) { this.lastBroadcastValue = lastBroadcastValue; }
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

        @Override
        public String toString(){
            return "Message => worker : " + this.worker
                    + " , typeMessage : " + this.typeMessage
                    + " , key : " + this.keyMessage
                    + " , value : " + this.value
                    + " , lastBroadcastValue : " + this.lastBroadcastValue
                    + " , message timestamp : " + new Timestamp(this.timestamp);
        }

        // Get the size of message in bytes
        public int size() throws ClassNotFoundException {

            int size;
            size =  this.getWorker().length()*Character.BYTES
                    + this.getTypeMessage().toString().length()*Character.BYTES
                    + Long.BYTES*4;

            return size;
        }
    }
    public static class ObjectSizeFetcher {
        private static Instrumentation instrumentation;
        public static void premain(String args, Instrumentation inst) { instrumentation = inst; }
        public static long getObjectSize(Object o) { return instrumentation.getObjectSize(o); }
    }
    public static boolean incrementCounters(List<String> inputValues,NodeBN_schema[] nodes,LinkedHashMap<String,Integer> schema,LinkedHashMap<Long,Counter> counters){

        // Increment the numbers of messages
        boolean incrementMessage = false;

        // Number of messages of counter
        long numMessages;

        // For each of node of BN or NBC
        for(NodeBN_schema node : nodes){

            // String buffer
            StringBuffer sb = new StringBuffer();

            // Get the name of node and appended
            String name = node.getName();
            sb.append(name);

            // Check if exists parents
            if(node.getParents().length != 0){

                // Counter
                // Find and append the value from input using the schema of dataset
                sb.append(",").append(inputValues.get(schema.get(name)));

                // Get and append the name and values of parents nodes
                StringBuilder nameParents = new StringBuilder();
                StringBuilder valuesParents = new StringBuilder();
                for(NodeBN_schema nodeParents : node.getParents()){
                    nameParents.append(nodeParents.getName()).append(",");
                    valuesParents.append(inputValues.get(schema.get(nodeParents.getName()))).append(",");
                }

                // Delete the last comma
                valuesParents.deleteCharAt(valuesParents.length()-1);

                // Get the key
                String key = sb.append(",").append(nameParents).append(valuesParents).toString().replace(" ","");

                // Hashing the key and get the counter
                Counter counter = counters.get(hashKey(key.getBytes()));

                // Get the number of messages before the increment
                numMessages = counter.getNumMessages();

                // Increment the counter
                incrementExactCounter(counter,System.currentTimeMillis());

                // If the number of messages was changed(only one increment is enough)
                if(numMessages != counter.getNumMessages() && !incrementMessage) incrementMessage = true;


                // Parent counter
                // Get the key of parent counter
                String parentKey = (nameParents.toString()+valuesParents+","+name).replace(" ","");

                // Hashing the key and get the parent-counter
                Counter parentCounter = counters.get(hashKey(parentKey.getBytes()));

                // Get the number of messages before the increment
                numMessages = counter.getNumMessages();

                // Increment the parent counter
                incrementExactCounter(parentCounter,System.currentTimeMillis());

                // If the number of messages was changed(only one increment is enough)
                if(numMessages != counter.getNumMessages() && !incrementMessage) incrementMessage = true;

            }
            else{

                // Find and append the value from input using the schema of dataset
                sb.append(",").append(inputValues.get(schema.get(name)));

                // Get the key
                String key = sb.toString().replace(" ","");

                // Hashing the key and get the counter
                Counter counter = counters.get(hashKey(key.getBytes()));

                // Get the number of messages before the increment
                numMessages = counter.getNumMessages();

                // Increment the counter
                incrementExactCounter(counter,System.currentTimeMillis());

                // If the number of messages was changed(only one increment is enough)
                if(numMessages != counter.getNumMessages() && !incrementMessage) incrementMessage = true;

            }

        }

        return incrementMessage;
    }



    // Estimate queries RC
    // Bayesian Network(BN)
    public static void processingQueriesBN(Vector<Counter> counters,String file) throws Exception {

        // Lower and upper bound violations
        int lbv = 0,ubv = 0;
        double avgError = 0;

        // Get the schema of dataset
        String schema = new BufferedReader(new FileReader("src\\main\\java\\bayesianNetworks\\dataset_schema_link")).readLine();
        List<String> datasetSchema = Arrays.asList(schema.split(","));

        List<Input> queries = getQueries(file);
        System.out.println("\nProcessing BN queries : "+queries.size()+"\n");
        BufferedWriter writer = new BufferedWriter(new FileWriter("queries2"));

        // Processing the queries
        for(Input query : queries){

            double truthProb = estimateJointProbTruth(counters,query);
            double estProb = estimateJointProbEst(counters,query);
            double error = estProb-truthProb;

            // Collect some statistics
            writer.write("Input : " + query
                            + " , estimated probability : " + estProb
                            + " , truth probability : " + truthProb
                            + " , error : " + error
                            + " , absolute error : " + Math.abs(error)
                            + "," + -0.1*datasetSchema.size() + "  <= ln(estimated) - ln(truth) = " + (estProb-truthProb) + " <= " + 0.1*datasetSchema.size()
                            + " with probability " + (1-0.25)+"\n");

            if( (estProb-truthProb) >= (0.1*datasetSchema.size()) ){ ubv++; }
            else if( (estProb-truthProb) <= (-0.1*datasetSchema.size()) ){ lbv++; }

            // Update the average error
            avgError += error;
        }

        System.out.println("\nProcessed queries : "+queries.size()+"\n");
        writer.write("Average error : " + avgError/queries.size()
                        + " , lower violations : " + lbv
                        + " , upper violations : " + ubv);

        // Close the writer buffer
        writer.close();

    }

    public static void processingQueriesBN(LinkedHashMap<Long,Counter> counters,String file) throws Exception {

        // Lower and upper bound violations
        int lbv = 0,ubv = 0;
        double avgError = 0;
        double absAvgError = 0;
        double avgGT = 0;
        int gt_threshold = 0;
        int queriesSize = 0;

        // Get the schema of dataset
        String schema = new BufferedReader(new FileReader("src\\main\\java\\bayesianNetworks\\dataset_schema_alarm")).readLine();
        List<String> datasetSchema = Arrays.asList(schema.split(","));

        List<Input> queries = getQueries(file);
        // List<Input> queries = getQueriesFormatted(file);
        System.out.println("\nProcessing BN queries : "+queries.size()+"\n");
        BufferedWriter writer = new BufferedWriter(new FileWriter("queries2"));

        // Processing the queries
        for(Input query : queries){

            double truthProb = estimateJointProbTruth(counters,query);
            double estProb = estimateJointProbEst(counters,query);
            double error = estProb-truthProb;

            // Skip not defined estimated probabilities
            if(estProb == 1) continue;

            // Collect some statistics
            writer.write("\nInput : " + query
                                        + " , estimated probability : " + estProb
                                        + " , truth probability : " + truthProb
                                        + " , error : " + error
                                        + " , absolute error : " + Math.abs(error)
                                        + "," + -0.1*datasetSchema.size() + "  <= ln(estimated) - ln(truth) = " + (estProb-truthProb) + " <= " + 0.1*datasetSchema.size()
                                        + " with probability " + (1-0.25)+"\n");

            if( (estProb-truthProb) >= (0.1*datasetSchema.size()) ){ ubv++; }
            else if( (estProb-truthProb) <= (-0.1*datasetSchema.size()) ){ lbv++; }

            // Update the average error
            avgError += error;
            absAvgError += Math.abs(error);

            // Update the average of GT probability
            avgGT += truthProb;
            if(truthProb > Math.log(0.01)) gt_threshold++;

            // Update the total count of queries
            queriesSize++;
        }

        System.out.println("\nProcessed queries : "+queriesSize+"\n");
        writer.write("\nQueries size : "+queriesSize);
        writer.write("\nAverage error : "+ avgError/queriesSize
                                             + " , lower violations : " + lbv
                                             + " , upper violations : " + ubv);

        writer.write("\nAverage absolute error : "+ absAvgError/queriesSize);
        writer.write("\nAverage Ground truth Log likelihood : "+ avgGT/queriesSize);
        writer.write("\nCount of log(prob) > log(0.01) : "+ gt_threshold);

        // Close the writer buffer
        writer.close();

    }

    // Estimate the logarithm of joint probability based on truth parameters of Bayesian Network(BN) - Method B
    public static double estimateJointProbTruth(Vector<Counter> counters, Input input) throws Exception {

        // Get the values of input
        List<String> values = Arrays.asList(input.getValue().replaceAll(" ","").split(","));

        // Get the schema of dataset
        String schema = new BufferedReader(new FileReader("src\\main\\java\\bayesianNetworks\\dataset_schema_link")).readLine();
        List<String> datasetSchema = Arrays.asList(schema.split(","));

        // The log of joint probability of input
        // Log to avoid underflow problems
        // double prob = 1.0d;
        double logProb = 0d;

        // Parsing all the counters
        for (Counter counter : counters) {

            if(counter.isActualNode() && checkCounter(counter,values,datasetSchema)){
                // Update the probability
                // prob = prob * counter.getTrueParameter();
                logProb += log(counter.getTrueParameter());
            }
        }

        return logProb;
    }

    // Estimate the logarithm of joint probability based on truth parameters of Bayesian Network(BN) - Method B
    public static double estimateJointProbTruth(LinkedHashMap<Long,Counter> counters, Input input) throws Exception {

        // Bayesian Network
        ObjectMapper objectMapper = new ObjectMapper();

        // Unmarshall as array
        NodeBN_schema[] nodes = objectMapper.readValue(new File("src\\main\\java\\bayesianNetworks\\bn_JSON_alarm.json"), NodeBN_schema[].class);

        // Get the values of input
        List<String> values = Arrays.asList(input.getValue().replaceAll(" ","").split(","));

        // Get the schema of dataset
        String schema = new BufferedReader(new FileReader("src\\main\\java\\bayesianNetworks\\dataset_schema_alarm")).readLine();
        LinkedHashMap<String,Integer> dataset_schema = new LinkedHashMap<>();
        int value = 0;
        for(String node : schema.split(",")){
            dataset_schema.put(node,value);
            value++;
        }

        // The log of joint probability of input
        // Log to avoid underflow problems
        // double prob = 1.0d;
        double logProb = 0d;
        double trueParameter;

        // Parsing all nodes
        for( NodeBN_schema node : nodes ) {

            // String buffer
            StringBuffer sb = new StringBuffer();

            // Get the name of node and appended
            String name = node.getName();
            sb.append(name);

            // Check if exists parents
            if (node.getParents().length != 0) {

                // Find the value of dataset_schema and appended to buffer
                sb.append(",").append(values.get(dataset_schema.get(name)));

                // Get the name and values of parents
                StringBuilder nameParents = new StringBuilder();
                StringBuilder valuesParents = new StringBuilder();
                for (NodeBN_schema nodeParents : node.getParents()) {
                    nameParents.append(nodeParents.getName()).append(",");
                    valuesParents.append(values.get(dataset_schema.get(nodeParents.getName()))).append(",");
                }

                // Replace the last comma
                valuesParents.deleteCharAt(valuesParents.length() - 1);

                // Hashing the key and get the counter
                // Get the true parameter
                String key = sb.append(",").append(nameParents).append(valuesParents).toString().replace(" ", "");
                trueParameter = counters.get(LongHashFunction.xx3().hashBytes(key.getBytes())).getTrueParameter();

            }
            else {

                // Find the values of dataset_schema
                sb.append(",").append(values.get(dataset_schema.get(name)));

                // Hashing the key
                String key = sb.toString().replace(" ", "");

                // Get the true parameter
                trueParameter = counters.get(LongHashFunction.xx3().hashBytes(key.getBytes())).getTrueParameter();
            }

            // Update the probability
            // prob = prob * counter.getTrueParameter();
            logProb += log(trueParameter);

            // if( trueParameter == 0){ logProb += log(1.0d/node.getCardinality()); }
            // else{ logProb += log(trueParameter); }


        }

        return logProb;
    }

    // Estimate the joint probability based on estimated parameters of Bayesian Network(BN)
    public static double estimateJointProbEst(Vector<Counter> counters, Input input) throws Exception {

        // Get the schema of dataset
        String schema = new BufferedReader(new FileReader("src\\main\\java\\bayesianNetworks\\dataset_schema_link")).readLine();
        List<String> datasetSchema = Arrays.asList(schema.split(","));

        // Get the values of input
        List<String> values = Arrays.asList(input.getValue().replaceAll(" ","").split(","));

        // The joint probability of input
        // Log to avoid underflow problems
        // double prob = 1.0d;
        double logProb = 0d;

        // Parsing all the counters
        for (Counter counter : counters) {

            // If counter is needed , then estimate the parameter
            if (counter.isActualNode() && checkCounter(counter,values,datasetSchema)){
                // Update the joint probability
                // prob = prob * estimateParameter(coordinatorState,counter);
                logProb += log(estimateParameter(counters,counter));
            }
        }
        return logProb;
    }

    // Estimate the joint probability based on estimated parameters of Bayesian Network(BN)
    public static double estimateJointProbEst(LinkedHashMap<Long,Counter> counters, Input input) throws Exception {

        // Bayesian Network
        ObjectMapper objectMapper = new ObjectMapper();

        // Unmarshall as array
        NodeBN_schema[] nodes = objectMapper.readValue(new File("src\\main\\java\\bayesianNetworks\\bn_JSON_alarm.json"), NodeBN_schema[].class);

        // Get the schema of dataset
        String schema = new BufferedReader(new FileReader("src\\main\\java\\bayesianNetworks\\dataset_schema_alarm")).readLine();
        LinkedHashMap<String,Integer> dataset_schema = new LinkedHashMap<>();
        int value = 0;
        for(String node : schema.split(",")){
            dataset_schema.put(node,value);
            value++;
        }

        // Get the values of input
        List<String> values = Arrays.asList(input.getValue().replaceAll(" ","").split(","));

        // The joint probability of input
        // Log to avoid underflow problems
        // double prob = 1.0d;
        double logProb = 0d;
        long estimator_node;
        long estimator_parent = 0L;

        // Parsing all nodes
        for(NodeBN_schema node : nodes){

            // String buffer
            StringBuffer sb = new StringBuffer();

            // Get the name of node and appended
            String name = node.getName();
            sb.append(name);

            // Check if exists parents
            if(node.getParents().length != 0){

                // Parameter = estimator_node / estimator_parent

                // Find the value of dataset_schema and appended to buffer
                sb.append(",").append(values.get(dataset_schema.get(name)));

                // Get the name and values of parents
                StringBuilder nameParents = new StringBuilder();
                StringBuilder valuesParents = new StringBuilder();
                for(NodeBN_schema nodeParents : node.getParents()){
                    nameParents.append(nodeParents.getName()).append(",");
                    valuesParents.append(values.get(dataset_schema.get(nodeParents.getName()))).append(",");
                }

                // Replace the last comma
                valuesParents.deleteCharAt(valuesParents.length()-1);

                // Hashing the key and get the counter
                // Estimator node
                String key = sb.append(",").append(nameParents).append(valuesParents).toString().replace(" ","");
                estimator_node = counters.get(LongHashFunction.xx3().hashBytes(key.getBytes())).getCounter();

                // Estimator parent
                String parentKey = (nameParents.toString()+valuesParents+","+name).replace(" ","");
                estimator_parent = counters.get(LongHashFunction.xx3().hashBytes(parentKey.getBytes())).getCounter();

            }
            else{

                // Parameter = estimator_node / estimator_parent

                // Find the values of dataset_schema
                sb.append(",").append(values.get(dataset_schema.get(name)));

                // Hashing the key
                String key = sb.toString().replace(" ","");

                // Estimator node
                estimator_node = counters.get(LongHashFunction.xx3().hashBytes(key.getBytes())).getCounter();

                // Parent node
                // For each value of node
                String parent_key;
                for(String parent_value : node.getValues()){

                    // Hashing the key
                    parent_key = (name+","+parent_value).replace(" ","");

                    // Get the counter
                    estimator_parent += counters.get(LongHashFunction.xx3().hashBytes(parent_key.getBytes())).getCounter();
                }
            }

            // Update the joint probability
            // prob = prob * estimateParameter(coordinatorState,counter);

            if( estimator_parent == 0 || estimator_node == 0){
                // logProb += laplaceSmoothing(estimator_node,estimator_parent,node.getCardinality(),1.0d);
                logProb += log(1.0d);
                // return 1;
            }
            else logProb += log(((double)estimator_node/estimator_parent));

            // estProb = laplaceSmoothing(estimator_node,estimator_parent,node.getCardinality(),1.0d);
            // if(estimator_node == 0 || estimator_parent == 0 ){ logProb += log(1.0d/node.getCardinality()); }
            // else{ logProb += log(laplaceSmoothing(estimator_node,estimator_parent,node.getCardinality(),1.0d)); }
            // logProb += log(laplaceSmoothing(estimator_node,estimator_parent,node.getCardinality(),1.0d));

            estimator_parent = 0;
        }

        return logProb;
    }

    // Estimate the parameter of Bayesian Network(BN)
    public static double estimateParameter(Vector<Counter> counters,Counter counter){

        long estimator_node;
        long estimator_parent = 0L;
        int parent_index = -1;

        ArrayList<Integer> indexes = null;
        Counter parent_counter;

        // Check for parents node
        if(counter.getNodeParents() != null){
            // Find the parent node
            parent_index = findParentNode(counters,counter);
        }
        else{
            // Find all indexes of node for the counter
            indexes = findIndexesCounter(counters,counter);
        }


        // Parameter = estimator_node / estimator_parent

        // Calculate the estimator of node
        estimator_node = counter.getCounter();

        // Calculate the estimator of parent
        if( parent_index > 0 ){

            parent_counter = counters.get(parent_index);

            // Check the type of counter
            estimator_parent = parent_counter.getCounter();
        }
        else{
            Counter nodeCounter;
            for(Integer indexNode : Objects.requireNonNull(indexes) ){
                nodeCounter = counters.get(indexNode);

                // Check the type of counter
                estimator_parent += nodeCounter.getCounter();
            }
        }


        // Laplace smoothing for probability of parameter(only for zero parameters)
        if( estimator_parent == 0 || estimator_node == 0){ return 1.0d; }
        else{ return (double)estimator_node / estimator_parent; }

    }

    // Find the parent's node of search counter(BN)
    public static int findParentNode(Vector<Counter> counters,Counter searchCounter){

        Counter counter;

        for(int i=0;i<counters.size();i++){

            counter = counters.get(i);

            if( equalsList(counter.getNodeName(),searchCounter.getNodeParents())
                    && equalsList(counter.getNodeValue(),searchCounter.getNodeParentValues())
                    && equalsList(counter.getNodeParents(),searchCounter.getNodeName()) ){

                return i;

            }

        }

        return -1;
    }

    // Find all the indexes of search counter , that all the counters with the same node name(BN)
    public static ArrayList<Integer> findIndexesCounter(Vector<Counter> counters, Counter searchCounter){

        ArrayList<Integer> indexes = new ArrayList<>();
        Counter counter;

        for(int i=0;i<counters.size();i++){

            counter = counters.get(i);

            if( equalsList(counter.getNodeName(),searchCounter.getNodeName())
                    && equalsList(counter.getNodeParents(),searchCounter.getNodeParents())
                    && equalsList(counter.getNodeParentValues(),searchCounter.getNodeParentValues())
                    && counter.isActualNode() == searchCounter.isActualNode() ){

                indexes.add(i);
            }
        }

        return indexes;
    }

    public static List<Input> getQueries(String file) throws IOException, ParseException {

        // Read the queries line by line
        BufferedReader br = new BufferedReader(new FileReader(file));

        List<Input> queries = new ArrayList<>();

        String line = br.readLine();
        int count = 0;

        while( line != null ){

            // Process the line

            // Split the lines based on colon
            String[] splitsLine1 = line.replaceAll("\0","").split(":");

            // Get the value
            String value = splitsLine1[1].substring(0,splitsLine1[1].lastIndexOf(",")).trim();

            // Get the timestamp
            String mileSec = splitsLine1[splitsLine1.length-1];
            if(mileSec.split("\\.")[1].length() == 2){ mileSec = mileSec.trim().concat("0"); }
            else if(mileSec.split("\\.")[1].length() == 1){ mileSec = mileSec.trim().concat("0").concat("0"); }

            String timestamp = splitsLine1[splitsLine1.length-3]+":"+splitsLine1[splitsLine1.length-2]+":"+mileSec;
            timestamp = timestamp.trim();

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            Date date = simpleDateFormat.parse(timestamp);
            long ts = date.getTime();

            // Create the output-event(output follow the schema of datasetSchema)
            Input input = new Input(value, coordinatorKey, ts);
            queries.add(input);

            // Update the count
            count++;

            // Read the next line
            line = br.readLine();
        }


        System.out.println("Total count : "+ count);

        // Close the buffer
        br.close();

        return  queries;

    }

    public static List<Input> getQueriesFormatted(String file) throws IOException {

        // Read the queries line by line
        BufferedReader br = new BufferedReader(new FileReader(file));

        List<Input> queries = new ArrayList<>();

        String line = br.readLine();
        int count = 0;

        while (line != null) {

            // Process the line

            // Get the value
            String value = line.trim();

            // Create the output-event(output follow the schema of datasetSchema)
            Input input = new Input(value, coordinatorKey, System.currentTimeMillis());
            queries.add(input);

            // Update the count
            count++;

            // Read the next line
            line = br.readLine();
        }

        System.out.println("Total count : " + count);

        // Close the buffer
        br.close();

        return queries;
    }

    // Get the key from NodeBN_schema.The key is used for hashing
    public static void getKeyNode(NodeBN_schema node,List<String> inputValues,LinkedHashMap<String,Integer> schema,LinkedHashMap<Long,Counter> counters){

        // Get the input values

        // String buffer
        StringBuffer sb = new StringBuffer();

        // Get the name of node
        String name = node.getName();

        // Check if exists parents
        if(node.getParents().length != 0){

            // Append the name of node
            sb.append(name);

            // Find the value of node using the input and dataset_schema
            String valueName = inputValues.get(schema.get(name));
            sb.append(",").append(valueName);

            // Get the name and values of parents
            StringBuilder nameParents = new StringBuilder();
            StringBuilder valuesParents = new StringBuilder();
            for(NodeBN_schema nodeParents : node.getParents()){
               nameParents.append(nodeParents.getName()).append(",");
               valuesParents.append(inputValues.get(schema.get(nodeParents.getName()))).append(",");
            }

            // Replace the last comma
            // nameParents.substring(0,valuesParents.length()-1);
            valuesParents.deleteCharAt(valuesParents.length()-1);

            // Append and hashing the key
            sb.append(",").append(nameParents).append(valuesParents);
            String key = sb.toString().replace(" ","");

            // Update the counter
            Counter counter = counters.get(Murmur3.hash64(key.getBytes()));

            // Increment
            if( counter != null ){

                totalCount++;

                // Increment
                incrementExactCounter(counter,System.currentTimeMillis());
            }

            // Update the parent counter
            String parentKey = (nameParents + "," + valuesParents + "," + name).replace(" ","");
            Counter parentCounter = counters.get(Murmur3.hash64(parentKey.getBytes()));

            // Increment
            if( parentCounter != null ){
                totalCount++;

                // Increment
                incrementExactCounter(parentCounter,System.currentTimeMillis());
            }

        }
        else{


            // Find the values of dataset_schema
            String valueName = inputValues.get(schema.get(name));
            sb.append(",").append(valueName);

            // Hashing the key
            String key = sb.toString().replace(" ","");

            // Update the counter
            Counter counter = counters.get(Murmur3.hash64(key.getBytes()));
            if( counter != null ){
                totalCount++;

                // Increment
                incrementExactCounter(counter,System.currentTimeMillis());
            }

        }

    }

    // Check if counter is needed for the estimation of joint probability(BN)
    public static boolean checkCounter(Counter counter,List<String> inputValues,List<String> datasetSchema){

        // Find the index of node on datasetSchema
        int indexName = findIndex(datasetSchema,convertToString(counter.getNodeName()));
        if( indexName != -1){
            // If exist , check the value
            if(inputValues.get(indexName).equals(convertToString(counter.getNodeValue()))){

                // Check if exists parents
                if( counter.getNodeParents() != null ){

                    // Get the parents names and values
                    List<String> parentsNames = counter.getNodeParents();
                    List<String> parentsValues = counter.getNodeParentValues();
                    int indexParent;

                    // For each parent check the value on input
                    for(int i=0;i<parentsNames.size();i++){
                        indexParent = findIndex(datasetSchema,parentsNames.get(i));
                        if(indexParent != -1){
                            if(!inputValues.get(indexParent).equals(parentsValues.get(i))) return false;
                        }
                    }
                }
                return true;
            }
        }
        return false;
    }

    // Find the counter based on message
    public static boolean findCounter(Counter counter,Message message){

        if( equalsList(counter.getNodeName(), message.getNodeName()) // Node name
                && equalsList(counter.getNodeValue(),message.getNodeValue()) // Node values
                && counter.isActualNode() == message.isActualNode() ){

            // Check if exists parents
            if( counter.getNodeParents() != null && message.getNodeParents() != null ){

                // Check the name of parents
                if( equalsList(counter.getNodeParents(),message.getNodeParents()) ){

                    // Check the parents values
                    if( counter.getNodeParentValues() != null && message.getNodeParentValues() != null ){
                        return equalsList(counter.getNodeParentValues(),message.getNodeParentValues());
                    }
                    else return counter.getNodeParentValues() == null && message.getNodeParentValues() == null;

                }
            }
            else return counter.getNodeParents() == null && message.getNodeParents() == null;

        }

        return false;
    }

    // Check for increment
    public static boolean checkIncrement(List<String> inputValues, Counter counter, List<String> datasetSchema){

        int index;

        // Check the node values
        ArrayList<String> counterNames = counter.getNodeName(); // Names of node
        ArrayList<String> counterValues = counter.getNodeValue(); // Values of node
        for (int i=0;i<counterNames.size();i++){

            index = findIndex(datasetSchema,counterNames.get(i));
            if(!counterValues.get(i).equals(inputValues.get(index))) return false; // Not increment

        }

        // Check the parents node values(if exists)
        if( counter.isActualNode() && counter.getNodeParents() != null ){

            ArrayList<String> counterParentsNames = counter.getNodeParents(); // Parents names of node
            ArrayList<String> counterParentsValues = counter.getNodeParentValues(); // Parents values of node

            for (int i=0;i<counterParentsNames.size();i++){
                index = findIndex(datasetSchema,counterParentsNames.get(i));
                if(!counterParentsValues.get(i).equals(inputValues.get(index))) return false; // Not Increment
            }
        }

        return true;
    }

    // Test error to ground truth
    public static void testErrorGT() throws IOException {

        // Bayesian Network
        ObjectMapper objectMapper = new ObjectMapper();

        // Unmarshall as array
        NodeBN_schema[] nodes = objectMapper.readValue(new File("src\\main\\java\\bayesianNetworks\\bn_JSON_hepar2.json"), NodeBN_schema[].class);

        long size = calculateSizeOfDataset(nodes,0.1,0.25,0.01);

    }

    // Estimation process

    // Estimation Bayesian Network(BN)
    public static void testEstimation(CountMin cm,Vector<Counter> counters,long totalCount){


        long error = 0;  // Average error
        int vlb = 0; // Number of violation lower bound
        int vub = 0; // Number of violation lower bound


        // Print some statistics
        double epsilon = cm.epsilon();
        double delta = cm.delta();
        System.out.println( "Error : " + epsilon*totalCount
                            + " , confidence level : " + (1-delta));

        // For each count , compute the estimated count
        for (Counter counter : counters) {

            // Get key
            String key = getKey(counter);

            // Get the estimation using HashCode
            // double estimatedCount = cm.estimateHashCode(key);

            // Get the estimation using Arrays HashCode
            // double estimatedCount = cm.estimateArrayHashCode(key);

            // Get the estimation using hash function
            // double estimatedCount = cm.estimateHash(key);

            // Get the estimation using murmur3
            double estimatedCount = cm.estimateMurmur(key);

            // Get the estimation using polyMurmur3
            // double estimatedCount = cm.estimatePolyMurmur(key);

            // Get the estimation using CW
            // double estimatedCount = cm.estimateCW(key);

            // Get the estimation using CWS
            // double estimatedCount = cm.estimateCWS(key);

            // Get the estimation using polynomial rolling hash function
            // double estimatedCount = cm.estimatePolyRollingHash(key);

            // Print some statistics
            System.out.println( "Counter => "
                                + " name : " + counter.getNodeName()
                                + " node values : " + counter.getNodeValue()
                                + " , nodeParents : " + counter.getNodeParents()
                                + " , nodeParentsValues : " + counter.getNodeParentValues()
                                + " , estimated count : " + estimatedCount
                                + " , actual count : " + counter.getCounter()
                                + " , " + counter.getCounter() + " <= est = "+ estimatedCount + " <= " +(counter.getCounter() + (epsilon*totalCount)) );


            // Update the lower and upper bounds violations
            if( estimatedCount < counter.getCounter() ) vlb++;
            if( estimatedCount > (counter.getCounter() + (epsilon*totalCount)) ) vub++;

            // Update the error
            error += Math.abs(estimatedCount-counter.getCounter());

        }

        // Print some statistics
        System.out.println( "Error : " + error
                            + " , average error : " + (double)error/counters.size()
                            + " , lower bound violations : " + vlb
                            + " , upper bound violations : " + vub);

    }

    // Estimation the parameters of Naive Bayes Classifier(NBC)
    public static void testEstimationNBC(CountMin cm,Vector<Counter> counters,long totalCount){

        long error = 0;  // Average error
        int vlb = 0; // Number of violation lower bound
        int vub = 0; // Number of violation lower bound

        // The schema of dataset
        String datasetSchema = "outlook,temp,humidity,wind,play";
        List<String> schema = Arrays.asList(datasetSchema.split(","));

        // Print some statistics
        double epsilon = cm.epsilon();
        double delta = cm.delta();
        System.out.println( "Error : " + epsilon*totalCount
                            + " , confidence level : " + (1-delta));

        // Estimated total count
        double estimatedTotalCount = 0;

        // For each count , compute the estimated count
        for (Counter counter : counters) {

            // Get key
            String key = getKeyNBC(findIndex(schema,counter.getNodeName().get(0)),counter); // or getKey(counter);

            // Get the estimation using HashCode
            // double estimatedCount = cm.estimateHashCode(key);

            // Get the estimation using Arrays HashCode
            // double estimatedCount = cm.estimateArrayHashCode(key);

            // Get the estimation using hash function
            // double estimatedCount = cm.estimateHash(key);

            // Get the estimation using murmur3
            double estimatedCount = cm.estimateMurmur(key);
            if( counter.getNodeParents() == null ) estimatedTotalCount += estimatedCount; // Class node

            // Get the estimation using polyMurmur3
            // double estimatedCount = cm.estimatePolyMurmur(key);

            // Get the estimation using CW
            // double estimatedCount = cm.estimateCW(key);

            // Get the estimation using CWS
            // double estimatedCount = cm.estimateCWS(key);

            // Get the estimation using polynomial rolling hash function
            // double estimatedCount = cm.estimatePolyRollingHash(key);

            // Print some statistics
            System.out.println( "Counter => "
                                + " name : " + counter.getNodeName()
                                + " node values : " + counter.getNodeValue()
                                + " , nodeParents : " + counter.getNodeParents()
                                + " , nodeParentsValues : " + counter.getNodeParentValues()
                                + " , estimated count : " + estimatedCount
                                + " , actual count : " + counter.getCounter()
                                + " , " + counter.getCounter() + " <= est = "+ estimatedCount + " <= " +(counter.getCounter() + (epsilon*totalCount)) );


            // Update the lower and upper bounds violations
            if( estimatedCount < counter.getCounter() ) vlb++;
            if( estimatedCount > (counter.getCounter() + (epsilon*totalCount)) ) vub++;

            // Update the error
            error += Math.abs(estimatedCount-counter.getCounter());

        }

        // Print the estimated total count
        System.out.println("Estimated total count : " + estimatedTotalCount);

        // Print some statistics
        System.out.println( "Error : " + error
                            + " , average error : " + (double)error/counters.size()
                            + " , lower bound violations : " + vlb
                            + " , upper bound violations : " + vub);

    }

    // Estimation the parameters of Naive Bayes Classifier using Feature Hashing(NBC)
    public static void testEstimationNBCFH(CountMin cm,Vector<Counter> counters,long totalCount){

        long error = 0;  // Average error
        int vlb = 0; // Number of violation lower bound
        int vub = 0; // Number of violation lower bound

        // The schema of dataset
        String datasetSchema = "outlook,temp,humidity,wind,play";
        List<String> schema = Arrays.asList(datasetSchema.split(","));

        // Print some statistics
        double epsilon = cm.epsilon();
        double delta = cm.delta();
        System.out.println( "Error : " + epsilon*totalCount
                            + " , confidence level : " + (1-delta));

        // Estimated total count
        double estimatedTotalCount = 0;

        // Input
        Input input = new Input("Overcast,Hot,High,Weak,Yes","0",0);
        List<String> inputValues = Arrays.asList(input.getValue().split(","));

        // Hashing the input
        long[] hashedVector = hashingVector(input,3); // or hashingVectorized

        // Get the estimated count
        for(int i=0;i<hashedVector.length-1;i++){

            // Generate the key = <index,value,class>
            String key = String.valueOf(i).concat(",").concat(String.valueOf(hashedVector[i])).concat(",").concat(inputValues.get(inputValues.size()-1));

            // Update the sketch
            double est = cm.estimateMurmur(key);
        }


        // For each count of input, compute the estimated count
        for (Counter counter : counters) {

            if (checkIncrement(inputValues, counter, schema)) {

                // Get the index
                int index = (int) (hashing(counter.getNodeValue().get(0)) % 3);

                // Get key
                String key = String.valueOf(index).concat(",").concat(String.valueOf(hashedVector[index])).concat(",").concat(inputValues.get(inputValues.size()-1));

                // Get the estimation using murmur3
                double estimatedCount = cm.estimateMurmur(key);
                if (counter.getNodeParents() == null) estimatedTotalCount += estimatedCount; // Class node

                // Print some statistics
                System.out.println( "Counter => "
                                    + " name : " + counter.getNodeName()
                                    + " node values : " + counter.getNodeValue()
                                    + " , nodeParents : " + counter.getNodeParents()
                                    + " , nodeParentsValues : " + counter.getNodeParentValues()
                                    + " , estimated count : " + estimatedCount
                                    + " , actual count : " + counter.getCounter()
                                    + " , " + counter.getCounter() + " <= est = " + estimatedCount + " <= " + (counter.getCounter() + (epsilon * totalCount)));


                // Update the lower and upper bounds violations
                if (estimatedCount < counter.getCounter()) vlb++;
                if (estimatedCount > (counter.getCounter() + (epsilon * totalCount))) vub++;

                // Update the error
                error += Math.abs(estimatedCount - counter.getCounter());
            }



        }

        // Print the estimated total count
        System.out.println("Estimated total count : " + estimatedTotalCount);

        // Print some statistics
        System.out.println( "Error : "  + error
                                        + " , average error : " + (double)error/counters.size()
                                        + " , lower bound violations : " + vlb
                                        + " , upper bound violations : " + vub);

    }

    // Estimation Bayesian Network after feature hashing(BN)
    public static void testEstimationFH(CountMin cm,Vector<Counter> counters,long totalCount){


        long error = 0;  // Average error
        int vlb = 0; // Number of violation lower bound
        int vub = 0; // Number of violation lower bound


        // Print some statistics
        double epsilon = cm.epsilon();
        double delta = cm.delta();
        System.out.println( "Error : " + epsilon*totalCount
                            + " , confidence level : " + (1-delta));

        // For each count , compute the estimated count
        for (Counter counter : counters) {

            // Get key
            String key = getKey(counter);

            // Update using murmur3
            // long[] keys = hashingVector(new Input(key,"0",0),4);

            // Get the estimation using murmur3
            double estimatedCount = 0;
            //for (long hashedKey : keys) {
            estimatedCount = cm.estimateMurmur(key);
            // estimatedCount = cm.estimateMurmur(longToBytesBE(hashing(key,4)));
            //}


            // Print some statistics
            System.out.println( "Counter => "
                                + " name : " + counter.getNodeName()
                                + " node values : " + counter.getNodeValue()
                                + " , nodeParents : " + counter.getNodeParents()
                                + " , nodeParentsValues : " + counter.getNodeParentValues()
                                + " , estimated count : " + estimatedCount
                                + " , actual count : " + counter.getCounter()
                                + " , " + counter.getCounter() + " <= est = "+ estimatedCount + " <= " +(counter.getCounter() + (epsilon*totalCount)) );


            // Update the lower and upper bounds violations
            if( estimatedCount < counter.getCounter() ) vlb++;
            if( estimatedCount > (counter.getCounter() + (epsilon*totalCount)) ) vub++;

            // Update the error
            error += Math.abs(estimatedCount-counter.getCounter());

        }

        // Print some statistics
        System.out.println( "Error : " + error
                            + " , average error : " + (double)error/counters.size()
                            + " , lower bound violations : " + vlb
                            + " , upper bound violations : " + vub);

    }

    // Estimation Bayesian Network(BN) using AGMS-sketch
    public static void testEstimationAGMS(AGMS agmsSketch, Vector<Counter> counters, long totalCount){

        long error = 0;  // Average error
        int vlb = 0; // Number of violation lower bound
        int vub = 0; // Number of violation lower bound

        // Print some statistics
        double epsilon = Math.sqrt(16.0/agmsSketch.width());
        double delta = Math.pow(2,-agmsSketch.depth()/2.0);

        System.out.println( "Epsilon : " + epsilon
                            + " , confidence level : " + (1-delta)
                            + ", error : " + epsilon*totalCount);


        // For each count , compute the estimated count
        for (Counter counter : counters) {

            // Get key
            String key = getKey(counter);

            // Get the estimation using murmur3
            double estimatedCount = agmsSketch.estimate(Murmur3.hash64(key.getBytes()));
            if( estimatedCount < 0 ){ estimatedCount = - estimatedCount; }

            // Print some statistics
            System.out.println( "Counter => "
                                + " name : " + counter.getNodeName()
                                + " node values : " + counter.getNodeValue()
                                + " , nodeParents : " + counter.getNodeParents()
                                + " , nodeParentsValues : " + counter.getNodeParentValues()
                                + " , estimated count : " + estimatedCount
                                + " , actual count : " + counter.getCounter()
                                + " , error : " + (estimatedCount-counter.getCounter())
                                + " , " + (counter.getCounter() - (epsilon*counter.getCounter())) + " <= est = "+ estimatedCount + " <= " +(counter.getCounter() + (epsilon*counter.getCounter())) );


            // Update the lower and upper bounds violations
            if( estimatedCount < (counter.getCounter() - (epsilon*counter.getCounter())) ) vlb++;
            if( estimatedCount > (counter.getCounter() + (epsilon*counter.getCounter())) ) vub++;

            // Update the error
            error += Math.abs(estimatedCount-counter.getCounter());

        }

        // Print some statistics
        System.out.println( "Error : " + error
                            + " , average error : " + (double)error/counters.size()
                            + " , lower bound violations : " + vlb
                            + " , upper bound violations : " + vub);

    }

    // Estimation Bayesian Network(BN) using Vector type
    public static void testEstimationVector(VectorType vector,Vector<Counter> counters,long totalCount){


        long error = 0;  // Average error
        int vlb = 0; // Number of violation lower bound
        int vub = 0; // Number of violation lower bound


        // Print some statistics
        System.out.println( "Size : " + vector.getSize());

        // For each count , compute the estimated count
        for (Counter counter : counters) {

            // Get key
            String key = getKey(counter);

            // Get the estimation using murmur3
            double estimatedCount = vector.estimate(key);

            // Print some statistics
            System.out.println( "Counter => "
                                + " name : " + counter.getNodeName()
                                + " node values : " + counter.getNodeValue()
                                + " , nodeParents : " + counter.getNodeParents()
                                + " , nodeParentsValues : " + counter.getNodeParentValues()
                                + " , estimated count : " + estimatedCount
                                + " , actual count : " + counter.getCounter());


            // Update the lower and upper bounds violations
            if( estimatedCount < counter.getCounter() ) vlb++;
            if( estimatedCount > counter.getCounter() ) vub++;

            // Update the error
            error += Math.abs(estimatedCount-counter.getCounter());

        }

        // Print some statistics
        System.out.println( "Error : " + error
                            + " , average error : " + (double)error/counters.size()
                            + " , lower bound violations : " + vlb
                            + " , upper bound violations : " + vub);

    }

    // Test all methods from Vector type
    public static void testVectorType(VectorType vectorType){

        // Get size
        int size = vectorType.getSize();

        // Get the vector
        double[][] copy = vectorType.get();
        double[][] vector = new double[vectorType.get().length][vectorType.get()[0].length];
        for(int i=0;i<vector.length;i++){ System.arraycopy(copy[i], 0, vector[i], 0, vector[0].length); }

        // Get empty instance
        double[][] empty = vectorType.getEmptyInstance();

        // Reset the vector
        vectorType.resetVector();

        // Set vector
        vectorType.set(vector);
    }



    // Estimation Bayesian Network(BN) using AGMS-sketch,modified
    public static void testEstimationAGMSMod(AGMS agmsSketch, LinkedHashMap<Long,Counter> counters, long totalCount){

        long error = 0;  // Average error
        int vlb = 0; // Number of violation lower bound
        int vub = 0; // Number of violation upper bound

        int vlbc = 0; // Number of violation lower bound on actual counters
        int vubc = 0; // Number of violation upper bound on actual counters

        // Print some statistics
        double epsilon = Math.sqrt(16.0/agmsSketch.width());
        double delta = Math.pow(2,-agmsSketch.depth()/2.0);

        System.out.println( "Epsilon : " + epsilon
                            + " , confidence level : " + (1-delta)
                            + ", error : " + epsilon*totalCount);

        epsilon = 0.1;
        delta = 0.1;

        // For each count , compute the estimated count
        for (Counter counter : counters.values()) {

            // Get the estimation using murmur3
            // double estimatedCount = agmsSketch.estimate(counter.getCounterKey());
            double estimatedCount = agmsSketch.estimate(worker.WorkerFunction.getKey(counter));
            if( estimatedCount < 0 ){ estimatedCount = - estimatedCount; }

            // Print some statistics
            System.out.println( "Counter => "
                                + " name : " + counter.getNodeName()
                                + " node values : " + counter.getNodeValue()
                                + " , nodeParents : " + counter.getNodeParents()
                                + " , nodeParentsValues : " + counter.getNodeParentValues()
                                + " , estimated count : " + estimatedCount
                                + " , actual count : " + counter.getCounter()
                                + " , error : " + (estimatedCount-counter.getCounter())
                                + " , " + (counter.getCounter() - (epsilon*totalCount)) + " <= est = "+ estimatedCount + " <= " +(counter.getCounter() + (epsilon*totalCount)) );


            // Update the lower and upper bounds violations
            if( estimatedCount < (counter.getCounter() - (epsilon*totalCount)) ) vlb++;
            if( estimatedCount > (counter.getCounter() + (epsilon*totalCount)) ) vub++;

            // Update the lower and upper bounds violations on actual counts
            if( estimatedCount < (counter.getCounter() - (epsilon*counter.getCounter())) ) vlbc++;
            if( estimatedCount > (counter.getCounter() + (epsilon*counter.getCounter())) ) vubc++;

            // Update the error
            error += Math.abs(estimatedCount-counter.getCounter());

        }

        // Print some statistics
        System.out.println( "Error : " + error
                            + " , average error : " + (double)error/counters.size()
                            + " , lower bound violations : " + vlb
                            + " , upper bound violations : " + vub
                            + " , lower bound violations on actual counters : " + vlbc
                            + " , upper bound violations on actual counters : " + vubc);

    }

    // Estimation Bayesian Network(BN) using CountMin sketch,modified
    public static void testEstimationCM(CountMin cm,LinkedHashMap<Long,Counter> counters,long totalCount){


        long error = 0;  // Average error
        int vlb = 0; // Number of violation lower bound
        int vub = 0; // Number of violation lower bound

        int vubc = 0; // Number of violation upper bound on actual counters

        // Print some statistics
        double epsilon = cm.epsilon();
        double delta = cm.delta();
        System.out.println( "Error : " + epsilon*totalCount
                            + " , epsilon : "+epsilon
                            + " , confidence level : " + (1-delta));

        epsilon = 0.001;
        delta = 0.1;

        // For each count , compute the estimated count
        for (Counter counter : counters.values()) {

            // Get the estimation using murmur3
            // double estimatedCount = cm.estimateMurmur(counter.getCounterKey());
            double estimatedCount = cm.estimateMurmur(worker.WorkerFunction.getKey(counter));

            // Print some statistics
            System.out.println( "Counter => "
                                + " name : " + counter.getNodeName()
                                + " node values : " + counter.getNodeValue()
                                + " , nodeParents : " + counter.getNodeParents()
                                + " , nodeParentsValues : " + counter.getNodeParentValues()
                                + " , estimated count : " + estimatedCount
                                + " , actual count : " + counter.getCounter()
                                + " , " + counter.getCounter() + " <= est = "+ estimatedCount + " <= " +(counter.getCounter() + (epsilon*totalCount)) );


            // Update the lower and upper bounds violations
            if( estimatedCount < counter.getCounter() ) vlb++;
            if( estimatedCount > (counter.getCounter() + (epsilon*totalCount)) ) vub++;

            // Update the lower and upper bounds violations on actual counts
            if( estimatedCount > (counter.getCounter() + (0.1*counter.getCounter())) ){
                // System.out.println("Upper violations !!!");
                vubc++;
            }

            // Update the error
            error += Math.abs(estimatedCount-counter.getCounter());

        }

        // Print some statistics
        System.out.println( "Error : " + error
                            + " , average error : " + (double)error/counters.size()
                            + " , lower bound violations : " + vlb
                            + " , upper bound violations : " + vub
                            + " , upper bound violations on actual counters : " + vubc);

    }

    // Update Bayesian Network(BN) using AGMS-sketch,modified
    public static LinkedHashMap<Long,Counter> testUpdateAGMSMod() throws IOException {

        // Bayesian Network
        ObjectMapper objectMapper = new ObjectMapper();

        // Unmarshall as array
        NodeBN_schema[] nodes = objectMapper.readValue(new File("src\\main\\java\\bayesianNetworks\\bn_JSON_hepar2.json"), NodeBN_schema[].class);

        // Initialize all the counters of BN
        Vector<Counter> counters = new Vector<>();
        for (NodeBN_schema node : nodes) { counters.addAll(initializeCounters(node)); }
        initializeExactCounter(counters,WORKER);

        // Create the LinkedHashMap
        LinkedHashMap<Long,Counter> newCounters = new LinkedHashMap<>();
        for (Counter counter: counters){

            // Get the key
            String key = getKey(counter);

            // Hash the key
            long hashedKey = hashKey(key.getBytes());

            // Update the worker state
            newCounters.put(hashedKey,counter);

            // Update the counter
            counter.setCounterKey(hashedKey);
        }

        // The schema of dataset
        String datasetSchema = new BufferedReader(new FileReader("src\\main\\java\\bayesianNetworks\\dataset_schema_hepar2")).readLine();
        LinkedHashMap<String,Integer> schema = new LinkedHashMap<>();
        int pos = 0;
        for(String node : datasetSchema.split(",")){

            // Update the schema
            schema.put(node,pos);

            // Update the position
            pos++;
        }

        // Crete the Vector type
        // CountMin sketch = new CountMin(0.001,0.1);
        CountMin sketch = new CountMin(3,751);
        // CountMin sketch = new CountMin(3,3);
        // AGMS sketch = new AGMS(0.1,0.1);
        System.out.println(sketch);

        // Read the file line by line
        BufferedReader br = new BufferedReader(new FileReader("datasets\\data_hepar20_5000"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("coordinator_stats"));

        String line = br.readLine();
        int counterLines = 0;
        int times=1;

        while( line != null && !line.equals("EOF") ) {

            // Skip the schema of dataset
            if( counterLines == 0 ){
                counterLines++;
                line = br.readLine();
                continue;
            }

            // Strips off all non-ASCII characters
            line = line.replaceAll("[^\\x00-\\x7F]", "");
            // Erases all the ASCII control characters
            line = line.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "");
            // Skip empty lines
            if(line.equals("")) {
                line = br.readLine();
                continue;
            }
            // Skip lines with dataset schema
            if(line.equals(datasetSchema)){
                line = br.readLine();
                continue;
            }

            List<String> inputValues = Arrays.asList(line.split(","));
            incrementSketch(inputValues,nodes,schema,newCounters,sketch);

            counterLines++;
            if(counterLines == times*100000){
                System.out.println("Processing 100000 lines");
                times++;
            }

            // Read the next line
            line = br.readLine();

        }

        // Collect statistics
        AtomicInteger zero_counters = new AtomicInteger();
        AtomicInteger zero_trueParameter = new AtomicInteger();
        System.out.println("End of processing");
        writer.write("End of processing\n");
        newCounters.values().forEach( counter -> {
            try {
                writer.write(counter.toString()+"\n\n");
                if(counter.getCounter() == 0) zero_counters.getAndIncrement();
                if(counter.getTrueParameter() == 0d && counter.isActualNode()) zero_trueParameter.getAndIncrement();
            }
            catch (IOException e) {e.printStackTrace();}
        });
        System.out.println("Total count : "+ totalCount);
        System.out.println("Zero counters : " + zero_counters.get());
        System.out.println("Zero true parameters : " + zero_trueParameter.get());

        // Close BufferedReader and BufferedWriter
        br.close();
        writer.close();

        // testEstimationAGMSMod(sketch,newCounters,5000);
        long l1Norm = 0L;
        for(Counter counter : newCounters.values()) l1Norm += counter.getCounter();
        testEstimationCM(sketch,newCounters,l1Norm);

        long l2Norm = 0L;
        for(Counter counter : newCounters.values()){
            l2Norm += Math.pow(counter.getCounter(),2);
        }
        // testEstimationAGMSMod(sketch,newCounters,l2Norm);

        return newCounters;
    }
    public static void incrementSketch(List<String> inputValues,NodeBN_schema[] nodes,LinkedHashMap<String,Integer> schema,LinkedHashMap<Long,Counter> counters,VectorType vector){

        // For each of node of BN or NBC
        for(NodeBN_schema node : nodes){

            // String buffer
            StringBuffer sb = new StringBuffer();

            // Get the name of node and appended
            String name = node.getName();
            sb.append(name);

            // Check if exists parents
            if(node.getParents().length != 0){

                // Counter
                // Find and append the value from input using the schema of dataset
                sb.append(",").append(inputValues.get(schema.get(name)));

                // Get and append the name and values of parents nodes
                StringBuilder nameParents = new StringBuilder();
                StringBuilder valuesParents = new StringBuilder();
                for(NodeBN_schema nodeParents : node.getParents()){
                    nameParents.append(nodeParents.getName()).append(",");
                    valuesParents.append(inputValues.get(schema.get(nodeParents.getName()))).append(",");
                }

                // Delete the last comma
                valuesParents.deleteCharAt(valuesParents.length()-1);

                // Get the key
                String key = sb.append(",").append(nameParents).append(valuesParents).toString().replace(" ","");

                // Hashing the key and get the counter
                Counter counter = counters.get(hashKey(key.getBytes()));

                // Update the sketch
                vector.update(key);

                // Update the counter,testing purposes
                counter.setCounter(counter.getCounter()+1L);

                // Parent counter
                // Get the key of parent counter
                String parentKey = (nameParents.toString()+valuesParents+","+name).replace(" ","");

                // Hashing the key and get the parent-counter
                Counter parentCounter = counters.get(hashKey(parentKey.getBytes()));

                // Update the sketch
                vector.update(parentKey);

                // Update the parent counter,testing purposes
                parentCounter.setCounter(parentCounter.getCounter()+1L);

            }
            else{

                // Find and append the value from input using the schema of dataset
                sb.append(",").append(inputValues.get(schema.get(name)));

                // Get the key
                String key = sb.toString().replace(" ","");

                // Hashing the key and get the counter
                Counter counter = counters.get(hashKey(key.getBytes()));

                // Update the sketch
                vector.update(key);

                // Update the counter,testing purposes
                counter.setCounter(counter.getCounter()+1L);

            }

        }
    }


    // Get the key from Counter.The key is used for hashing
    public static String getKey(Counter counter){

        String key;

        // Get the node name
        ArrayList<String> names = new ArrayList<>(counter.getNodeName());

        // Append the node values
        names.addAll(counter.getNodeValue());

        // Append the node parents names if exists
        if( counter.getNodeParents() != null ){ names.addAll(counter.getNodeParents()); }

        // Append the node parents values if exists
        if( counter.getNodeParentValues() != null ){ names.addAll(counter.getNodeParentValues()); }

        key = convertToString(names).replace(" ","");
        return key;
    }


    // Statistics

    // Sketch => Depth : 4 , Width : 10 , Epsilon : 0.2718281828459045 , Delta : 0.01831563888873418 , Size : 40 , Size in bytes : 320
    // Error : 2174.625462767236 , confidence level : 0.9816843611112658
    // Using Hash function method => number of collisions : 31963 , error : 9167 , average error : 327.39285714285717 , lower bound violations : 0 , upper bound violations : 0 , Working
    // Using Arrays HashCode method => number of collisions : 31961, error : 8333 , average error : 297.60714285714283 , lower bound violations : 0 , upper bound violations : 0 , Working
    // Using Murmur3 function method => number of collisions : 31964, error : 2338 , average error : 83.5 , lower bound violations : 0 , upper bound violations : 0 , Working
    // Using PolyMurmur3 function method => number of collisions : 31961, error : 2212 , average error : 79.0 , lower bound violations : 0 , upper bound violations : 0 , Working
    // Using CW method => number of collisions = 31963, error : 8067 , average error : 288.10714285714283 , lower bound violations : 0 , upper bound violations : 0 , Working
    // Using CWS method => number of collisions = 31962, error : 2252 , average error : 80.42857142857143 , lower bound violations : 0 , upper bound violations : 0 , Working

    // Using HashCode method => number of collisions : 31962, error : 7813 , average error : 279.0357142857143 , lower bound violations : 10 , upper bound violations : 0 , Not working

    // Using Polynomial rolling hash function method => number of collisions = 31961 , error : 8074 , average error : 288.35714285714283 , lower bound violations : 12 , upper bound violations : 0 , Not Working


    // Sketch => Depth : 8 , Width : 15 , Epsilon : 0.18121878856393633 , Delta : 3.3546262790251185E-4 , Size : 120 , Size in bytes : 960
    // Error : 1449.7503085114906 , confidence level : 0.9996645373720975
    // Using Hash function method => number of collisions : 63907,  error : 2059 , average error : 73.53571428571429 , lower bound violations : 0 , upper bound violations : 0 , Working
    // Using Arrays HashCode method => number of collisions : 63911, error : 6192 , average error : 221.14285714285714 , lower bound violations : 0 , upper bound violations : 0 , Working
    // Using Murmur3 function method => number of collisions = 63893, error : 24 , average error : 0.8571428571428571 , lower bound violations : 0 , upper bound violations : 0 , Working
    // Using PolyMurmur3 function method => number of collisions : 63898, error : 73 , average error : 2.607142857142857 , lower bound violations : 0 , upper bound violations : 0 , Working
    // Using CW method => number of collisions = 63903, error : 1032 , average error : 36.857142857142854 , lower bound violations : 0 , upper bound violations : 0 , Working
    // Using CWS method => number of collisions = 63901, error : 1017 , average error : 36.32142857142857 , lower bound violations : 0 , upper bound violations : 0 , Working

    // Using HashCode method => number of collisions : 63915, error : 8000 , average error : 285.7142857142857 , lower bound violations : 25 , upper bound violations : 0 , Not working
    // Using Polynomial rolling hash function method => number of collisions = 63900 , error : 7967 , average error : 284.5357142857143 , lower bound violations : 24 , upper bound violations : 0 , Not Working


    // Observations
    // Hash function works better with large primes(e.g 10000) for low and high array
    // CW with ArrayHashCode works better with large primes and low dimensional array(e.g 4x10)
    // CWS with hashCode works better with large primes
    // CWS with ArrayHashCode works better with large primes but works worse than CWS with hashCode
    // PolyMurmur3 not working better with large primes on low dimensional arrays , for high dimensional arrays the same
    // Best Murmur3,PolyMurmur3 and HashFunction

}
