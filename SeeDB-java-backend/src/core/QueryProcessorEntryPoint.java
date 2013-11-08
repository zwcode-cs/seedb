package core;
import py4j.GatewayServer;

public class QueryProcessorEntryPoint {
	
	private QueryProcessor proc;
	
	public QueryProcessorEntryPoint() {
		this.proc = new QueryProcessor();
	}
	
	public QueryProcessor GetQueryProcessor() {
		return proc;
	}
	
	public static void main(String[] args) {
        GatewayServer gatewayServer = new GatewayServer(new QueryProcessorEntryPoint());
        gatewayServer.start();
        System.out.println("Gateway Server Started");
    }
}
