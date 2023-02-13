package fgm.state;

import config.FGMConfig;
import net.openhft.hashing.LongHashFunction;

import java.io.Serializable;
import java.util.LinkedHashMap;

import static config.InternalConfig.TypeNetwork;
import static config.InternalConfig.TypeState;
import static config.InternalConfig.ErrorSetup;

public class FGMState implements Serializable {

    // Contain common basic variables for setup of the project

    private final LinkedHashMap<String,Integer> datasetSchema; // The schema of dataset
    private final TypeNetwork typeNetwork; // Type of network
    private final TypeState typeState; // Type of state

    private final int numOfWorkers; // The number of workers-sites
    private final double eps; // Epsilon(input parameter)
    private final double delta; // Delta(input parameter)
    private final ErrorSetup errorSetup; // Error setup of FGM protocol(input parameter)

    private long numOfNodes; // Correspond to the number of nodes of Bayesian Network


    // Constructors
    public FGMState(FGMConfig config){

        // The schema of dataset
        datasetSchema = initializeDatasetSchema(config.datasetSchema());

        // Internal configuration
        typeNetwork = config.typeNetwork();
        typeState = config.typeState();

        // User configuration
        numOfWorkers = config.workers();
        eps = config.epsilon();
        delta = config.delta();
        errorSetup = config.errorSetup();
    }

    // Getters
    public LinkedHashMap<String,Integer> getDatasetSchema() { return datasetSchema; }
    public TypeNetwork getTypeNetwork() { return typeNetwork; }
    public TypeState getTypeState() { return typeState; }
    public int getNumOfWorkers() { return numOfWorkers; }
    public double getEps() { return eps; }
    public double getDelta() { return delta; }
    public ErrorSetup getErrorSetup() { return errorSetup; }
    public long getNumOfNodes() { return numOfNodes; }

    // Setters
    public void setNumOfNodes(long numOfNodes) { this.numOfNodes = numOfNodes; }

    // Methods

    // Initialize the schema of dataset as Linked HashMap<String,Integer>
    // The key correspond to each node/attribute of dataset and value correspond to the position of node/attribute in schema
    public static LinkedHashMap<String,Integer> initializeDatasetSchema(String datasetSchema){

        LinkedHashMap<String,Integer> schema = new LinkedHashMap<>();
        int pos = 0;
        for(String node : datasetSchema.split(",")){

            // Update the schema
            schema.put(node,pos);

            // Update the position
            pos++;
        }
        return schema;
    }

    // Initialize the error of counters based on setup of error
    public double baseline(){ return eps/(3*numOfNodes); } // BASELINE
    public double uniform(){ return eps/(16*Math.sqrt(numOfNodes)); } // UNIFORM

    // Hashing the key using xxh3 hash function
    // The type of key is a byte array
    public static long hashKey(byte[] key){ return LongHashFunction.xx3().hashBytes(key); }

    // Hashing the key using xxh3 hash function
    // The type of key is String
    public static long hashKey(String key){ return LongHashFunction.xx3().hashBytes(key.getBytes()); }

    @Override
    public String toString(){

        return  " , datasetSchema : " + this.getDatasetSchema()
                + " , typeNetwork : " + this.getTypeNetwork()
                + " , typeState : " + this.getTypeState()
                + " , numOfWorkers : " + this.getNumOfWorkers()
                + " , eps : " + this.getEps()
                + " , delta : " + this.getDelta()
                + " , errorSetup : " + this.getErrorSetup();

    }

}
