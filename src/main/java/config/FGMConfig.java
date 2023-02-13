package config;

import org.apache.flink.api.java.utils.ParameterTool;

import static bayesianNetworks.bayesianNetwork.getBayesianNetwork;
import static bayesianNetworks.datasetSchema.getDatasetSchema;
import static config.InternalConfig.TypeState;
import static config.InternalConfig.TypeNetwork;
import static config.InternalConfig.ErrorSetup;

public class FGMConfig extends Config {

    private ParameterTool parameters;

    // Constructors
    public FGMConfig(){}
    public FGMConfig(ParameterTool parameters){ this.parameters = parameters; }

    // Methods

    // User configuration
    public double epsilon(){ return parameters.getDouble("eps", DefaultParameters.eps); }
    public double delta(){ return parameters.getDouble("delta",DefaultParameters.delta); }
    public ErrorSetup errorSetup(){ return ErrorSetup.valueOf(parameters.get("errorSetup",String.valueOf(InternalConfig.errorSetup))); }
    public int workers(){ return parameters.getInt("workers",DefaultParameters.workers); }
    public long windowSize(){ return parameters.getLong("windowSize",DefaultParameters.windowSize); }
    public boolean enableWindow(){ return parameters.getBoolean("enableWindow",DefaultParameters.enableWindow); }
    public boolean enableInputString(){ return parameters.getBoolean("enableInputString",DefaultParameters.enableInputString); }
    public boolean enableWorkerStats(){ return parameters.getBoolean("enableWorkerStats",DefaultParameters.enableWorkerStats); }
    public int sourcePartition(){ return parameters.getInt("sourcePartition",DefaultParameters.sourcePartition); }

    // Internal configuration
    public String datasetSchema(){ return getDatasetSchema(parameters.get("datasetSchema",InternalConfig.datasetSchema)); }
    @Override
    public String BNSchema(){ return getBayesianNetwork(parameters.get("bn",InternalConfig.BNSchema)); }
    public long queriesSize(){ return parameters.getLong("queriesSize",InternalConfig.querySize); }
    public TypeNetwork typeNetwork(){ return TypeNetwork.valueOf(parameters.get("typeNetwork",String.valueOf(InternalConfig.typeNetwork))); }

    // FGM protocol
    public double epsilonPsi(){ return parameters.getDouble("epsilonPsi",DefaultParameters.epsilonPsi); }
    public boolean enableRebalancing(){ return parameters.getBoolean("enableRebalancing",DefaultParameters.enableRebalancing); }
    public double beta(){ return parameters.getDouble("beta",DefaultParameters.beta); }
    public long streamSize(){ return parameters.getLong("streamSize",DefaultParameters.streamSize); }
    public TypeState typeState(){ return TypeState.valueOf(parameters.get("typeState",String.valueOf(InternalConfig.typeState))); }
    public double epsilonSketch(){ return parameters.getDouble("epsilonSketch",DefaultParameters.epsilonSketch); }
    public double deltaSketch(){ return parameters.getDouble("deltaSketch",DefaultParameters.deltaSketch); }
    public int width(){ return parameters.getInt("width",DefaultParameters.width); }
    public int depth(){ return parameters.getInt("depth",DefaultParameters.depth); }

    @Override
    public String toString(){
        return "FGMConfig => epsilon : " + this.epsilon()
                + " , delta : " + this.delta()
                + " , errorSetup : " + this.errorSetup()
                + " , workers : " + this.workers()
                + " , windowSize : " + this.windowSize()
                + " , enableWindow : " + this.enableWindow()
                + " , sourcePartition : " + this.sourcePartition()
                + " , datasetSchema : " + this.datasetSchema()
                + " , queriesSize : " + this.queriesSize()
                + " , typeNetwork : " + this.typeNetwork()
                + " , typeState : " + this.typeState()
                + " , epsilonPsi : " + this.epsilonPsi()
                + " , enableRebalancing : " + this.enableRebalancing()
                + " , beta : " + this.beta()
                + " , streamSize : " + this.streamSize()
                + " , epsilonSketch : " + this.epsilonSketch()
                + " , deltaSketch : " + this.deltaSketch();
    }

}
