package core;
import py4j.GatewayServer;
import utils.RuntimeSettings;

public class QueryProcessorEntryPoint {
	
	private QueryProcessor proc;
	
	public QueryProcessorEntryPoint(RuntimeSettings runtimeSettings) {
		this.proc = new QueryProcessor();
		this.proc.setRuntimeSettings(runtimeSettings);
	}
	
	public QueryProcessor GetQueryProcessor() {
		return proc;
	}
	
	public static void main(String[] args) {
		RuntimeSettings settings = new RuntimeSettings();
		settings.samplePercent = 0.1;
		settings.useSampling = false;
		settings.useMultipleAggregateSingleGroupByOptimization = true;
        GatewayServer gatewayServer = new GatewayServer(new QueryProcessorEntryPoint(settings));
        gatewayServer.start();
        System.out.println("Gateway Server Started");
    }
}
