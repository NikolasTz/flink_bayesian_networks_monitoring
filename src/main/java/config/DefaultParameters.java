package config;

public class DefaultParameters {

    // Kafka brokers server
    public static String server = "localhost:9092";
    // public static String server = "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667";
    // public static String server = "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667";


    // Input topic
    public static String inputTopic = "test";
    public static String inputGroupId = "test-group";
    public static int sourcePartition = 1;

    // Feedback topic
    public static String feedbackTopic = "hepar2QWEFG";
    public static String feedbackGroupId = "hepar2QWEFG-group";

    // Query topic
    public static String queryTopic = "querySource";
    public static String queryGroupId = "querySource-group";

    // Number of workers
    public static int workers = 8;

    // Parallelism
    public static int parallelism = 4;

    // Epsilon and delta
    public static double eps = 0.1d;
    public static double delta = 0.25d;

    // Dummy Father - INDIVIDUAL COUNTERS
    public static boolean dummyFather = false; // Enable/disable the mechanism of Dummy Father

    // FGM protocol
    public static double epsilonPsi = 0.01d;
    public static boolean enableRebalancing = true;
    public static double beta = 0d; // b:scaling constant,used on CountMin sketches
    public static long streamSize = 500000L; // N:total count of stream
    public static double epsilonSketch = 0.1d;
    public static double deltaSketch = 0.25d;
    public static int width = 4;
    public static int depth = 4;
    // width and depth not used ???

    // Window
    public static long windowSize = 3000L;
    public static boolean enableWindow = false;

    // Input
    public static boolean enableInputString = false;

    // Coordinator and Worker statistics
    public static boolean enableWorkerStats = false;

    public static String coordinatorPath = "coordinator_stats.txt";
    public static String workerPath = "";

    public static String workerStatsTopic = "workerStatsALARM";
    public static String workerStatsGroupId = "workerStatsALARM-group";

    public static String coordinatorStatsTopic = "coordinatorStatsALARM";
    public static String coordinatorStatsGroupId = "coordinatorStatsALARM-group";


}
