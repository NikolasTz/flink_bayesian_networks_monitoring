package distcounter;

import config.InternalConfig.ErrorSetup;
import config.InternalConfig.SetupCounter;
import config.InternalConfig.TypeCounter;
import config.InternalConfig;

public class DefaultParameters {

    public static int num_workers = 4;
    public static String server = "localhost:9092";

    public static String init_input_topic = "init_topic";
    public static String init_group_id = "init_group";

    public static String feedback_topic = "f";
    public static String feedback_group_id = "f-group";

    public static String input_file = "wc.txt";
    public static String input_topic = "test";
    public static String input_group_id = "test-group";

    public static long window_size = 3000;
    public static boolean enable_window = false;

    public static double eps = 0.1;
    public static double delta = 0.25;
    public static double resConstant = 1.0;

    public static String datasetSchema = "Burglary,Earthquake,Alarm,JohnCalls,MaryCalls";
    public static String BNSchema = "[{\"name\":\"Alarm\",\"trueParameters\":[\"0.95,0.94,0.29,0.001\",\"0.05,0.06,0.71,0.999\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[{\"name\":\"Burglary\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]},{\"name\":\"Earthquake\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]}] },{\"name\":\"Burglary\",\"trueParameters\":[\"0.01\",\"0.99\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[ ] },{\"name\":\"Earthquake\",\"trueParameters\":[\"0.02\",\"0.98\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[ ] },{\"name\":\"JohnCalls\",\"trueParameters\":[\"0.9,0.05\",\"0.1,0.95\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[{\"name\":\"Alarm\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]}] },{\"name\":\"MaryCalls\",\"trueParameters\":[\"0.7,0.01\",\"0.3,0.99\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[{\"name\":\"Alarm\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]}] }]";
    public static ErrorSetup errorSetup = InternalConfig.ErrorSetup.NON_UNIFORM;
    public static SetupCounter setupCounter = InternalConfig.SetupCounter.HALVED;
    public static TypeCounter typeCounter = InternalConfig.TypeCounter.RANDOMIZED;
    public static long queriesSize = 1000L;

    public static int condition = 4000;

    // TODO FGM ???
    // TODO state on FGM just a 2d array or sketch ???
    // TODO Start with 2d array as state on FGM ???

    // TODO Worker and Coordinator => Add List of Counter for handle input,messages or array of indexes of input to get the value as key ???
    // TODO SafeZone => Fp moments on paper ???

    // TODO Concept drift for BN - AdaptiveBN(AdaSketchNB) => A Sketch-Based Naive Bayes Algorithms for Evolving Data Streams ???
    // TODO Feature hashing(memory consumption) => Feature Hashing for Large Scale Multitask Learning ???
    // TODO One COUNT-MIN sketch for each counter and parent counter(GMHash,GMSketch,GMFactorSketch) => Graphical Model Sketch ???
    // TODO One COUNT-MIN sketch for all Bayesian Network with appropriate epsilon,delta => A Sketch-Based Naive Bayes Algorithms for Evolving Data Streams ???


    // Testing
    // Messages on worker => one messages( consists of merging the resulting updates-increments for all counters into a single message ) per input and on initialization
    // Messages on coordinator => for broadcast , one message consists of merging the resulting updates for all workers into a single message and on initialization ???
    // NON_UNIFORM => Epsilon , on orphan nodes that has not parents(Ki=0) set Ki = 1(only the numerator of vi) , not to alpha and beta(Ki=0,Naive bayes-solution 1)

    // True parameters => Field on NodeBN_schema,probability calculate on the fly(based on NodeBN_schema)
    // Queries(testing data) and QuerySource
    // Internal config,CBNConfig and DefaultParameters
    // Error/accuracy => True and estimated parameters
    // Only for parameters=0 => Laplace smoothing-pseudoCounts or MAP estimation (avoid overfitting) ???

    // Iteration(Testing ???)

    // Deterministic counters(Testing ???)
    // Exact counters
    // Continuous counters
    // Rescaling constant
    // Total count on worker state


}
