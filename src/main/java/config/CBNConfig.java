package config;

import org.apache.flink.api.java.utils.ParameterTool;

import static bayesianNetworks.bayesianNetwork.getBayesianNetwork;
import static bayesianNetworks.datasetSchema.getDatasetSchema;
import static config.InternalConfig.ErrorSetup;
import static config.InternalConfig.SetupCounter;
import static config.InternalConfig.TypeCounter;
import static config.InternalConfig.TypeNetwork;

public class CBNConfig extends Config {

    private ParameterTool parameters;

    // Constructors
    public CBNConfig(){}
    public CBNConfig(ParameterTool parameters){ this.parameters = parameters; }

    // Methods

    // User configuration
    public double epsilon(){ return parameters.getDouble("eps", DefaultParameters.eps); }
    public double delta(){ return parameters.getDouble("delta",DefaultParameters.delta); }
    public int workers(){ return parameters.getInt("workers",DefaultParameters.workers); }
    public long windowSize(){ return parameters.getLong("windowSize",DefaultParameters.windowSize); }
    public boolean enableWindow(){ return parameters.getBoolean("enableWindow",DefaultParameters.enableWindow); }
    public boolean enableInputString(){ return parameters.getBoolean("enableInputString",DefaultParameters.enableInputString); }
    public boolean enableWorkerStats(){ return parameters.getBoolean("enableWorkerStats",DefaultParameters.enableWorkerStats); }
    public int sourcePartition(){ return parameters.getInt("sourcePartition",DefaultParameters.sourcePartition); }
    public boolean dummyFather(){ return parameters.getBoolean("dummyFather",DefaultParameters.dummyFather); }

    // Internal configuration
    public String datasetSchema(){ return getDatasetSchema(parameters.get("datasetSchema",InternalConfig.datasetSchema)); }
    @Override
    public String BNSchema(){ return getBayesianNetwork(parameters.get("bn",InternalConfig.BNSchema)); }
    public long condition(){ return parameters.getLong("condition",InternalConfig.condition); }
    public long queriesSize(){ return parameters.getLong("queriesSize",InternalConfig.querySize); }
    public ErrorSetup errorSetup(){ return ErrorSetup.valueOf(parameters.get("errorSetup",String.valueOf(InternalConfig.errorSetup))); }
    public SetupCounter setupCounter(){ return SetupCounter.valueOf(parameters.get("setupCounter",String.valueOf(InternalConfig.setupCounter))); }
    public TypeCounter typeCounter(){ return TypeCounter.valueOf(parameters.get("typeCounter",String.valueOf(InternalConfig.typeCounter))); }
    public TypeNetwork typeNetwork(){ return TypeNetwork.valueOf(parameters.get("typeNetwork",String.valueOf(InternalConfig.typeNetwork))); }

    @Override
    public String toString(){
        return "CBNConfig => epsilon : " + this.epsilon()
                + " , delta : " + this.delta()
                + " , workers : " + this.workers()
                + " , windowSize : " + this.windowSize()
                + " , enableWindow : " + this.enableWindow()
                + " , sourcePartition : " + this.sourcePartition()
                + " , dummyFather : " + this.dummyFather()
                + " , datasetSchema : " + this.datasetSchema()
                + " , condition : " + this.condition()
                + " , queriesSize : " + this.queriesSize()
                + " , errorSetup : " + this.errorSetup()
                + " , setupCounter : " + this.setupCounter()
                + " , typeCounter : " + this.typeCounter()
                + " , typeNetwork : " + this.typeNetwork();
    }

}
