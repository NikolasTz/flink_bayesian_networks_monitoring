package config;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class InternalConfig {

    // Internal configuration about counters
    public static ErrorSetup errorSetup = ErrorSetup.BASELINE; // Error setup - Individual counters
    public static SetupCounter setupCounter = SetupCounter.ZERO; // Setup of counter - Individual counters
    public static TypeCounter typeCounter = TypeCounter.RANDOMIZED; // Type of counter - Individual counters
    public static TypeNetwork typeNetwork = TypeNetwork.BAYESIAN; // Type of network - Both
    public static TypeState typeState = TypeState.VECTOR; // Type of state - FGM protocol
    public static long condition = 0L; // Starting condition about DETERMINISTIC and RANDOMIZED counters(comes with SetupCounter.CONDITION) - Individual counters

    // The size of queries(testing dataset size)
    public static long querySize = 1000L;

    // DatasetSchema and BNSchema
    public static String datasetSchema = "HEPAR2"; // From method
    // Get the schema of dataset from file or url
    /* static {
        try { datasetSchema = new BufferedReader(new FileReader("src\\main\\java\\bayesianNetworks\\dataset_schema_hepar2")).readLine(); }
        catch (IOException e) { e.printStackTrace(); }
    }*/

    public static String BNSchema = "HEPAR2"; // From method
    // Get the BN from file or url : "src\\main\\java\\bayesianNetworks\\bn_JSON_hepar2.json";

    // NBC : "outlook,temp,humidity,wind,play";
    // NBC : "[{\"name\":\"outlook\",\"trueParameters\":[],\"cardinality\":3,\"values\":[\"Sunny\",\"Overcast\",\"Rain\"],\"parents\":[]},{\"name\":\"temp\",\"trueParameters\":[],\"cardinality\":3,\"values\":[\"Hot\",\"Mild\",\"Cool\"],\"parents\":[]},{\"name\":\"humidity\",\"trueParameters\":[],\"cardinality\":2,\"values\":[\"High\",\"Normal\"],\"parents\":[ ] },{\"name\":\"wind\",\"trueParameters\":[],\"cardinality\":2,\"values\":[\"Weak\",\"Strong\"],\"parents\":[]},{\"name\":\"play\",\"trueParameters\":[],\"cardinality\":2,\"values\":[\"Yes\",\"No\"],\"parents\":[]}]";

    // Coordinator key
    public static final String coordinatorKey = "0";

    // Error setup
    public enum ErrorSetup {
        BASELINE,
        UNIFORM,
        NON_UNIFORM
    }

    // Counter setup act as warmup mechanism
    public enum SetupCounter {
        ZERO,
        HALVED,
        SUB_TRIPLED,
        SUB_QUADRUPLED,
        SUB_QUINTUPLED,
        CONDITION // Using from Deterministic and Randomized counters with the variable condition
        // All the setups used exclusively from Randomized and Deterministic counters
    }

    // Counter type
    public enum TypeCounter{
        RANDOMIZED,
        DETERMINISTIC,
        CONTINUOUS,
        EXACT
    }

    // Type of Bayesian Network
    public enum TypeNetwork{
        BAYESIAN, // Correspond to Bayesian Network(BN)
        NAIVE // Correspond to Naive Bayes Classifier(NBC)
    }

    // Type of state(used exclusively from FGM protocol)
    public enum TypeState{
        VECTOR,
        COUNT_MIN,
        AGMS
    }

    // Type of message
    public enum TypeMessage{
        INCREMENT, // To FGM correspond to increment on local counter ci
        DOUBLES,
        UPDATE,
        COUNTER, // Correspond to the local counter of worker
        DRIFT, // To FGM correspond to drift vector Xi
        QUANTUM, // To FGM correspond to quantum a.k.a  theta
        ESTIMATE, // To FGM correspond to global estimate E
        ZETA, // To FGM correspond to zeta
        LAMBDA, // To FGM correspond to lambda , for rebalancing
        END_OF_STREAM, // Correspond to the end of INPUT stream
        END_OF_WORKER // Correspond to the end of worker stream
    }

}
