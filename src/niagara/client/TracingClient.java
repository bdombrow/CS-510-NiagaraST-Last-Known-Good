package niagara.client;
//testline
public class TracingClient extends SimpleClient {
    boolean firstTime;
    public TracingClient(String host, int port, String outputFileName) {
	cm = new ConnectionManager(
	    new TracingConnectionReader(this, host, port,
					new DTDCache(), outputFileName));
	firstTime = true;
    }

    public void processQuery(String queryText) throws ClientException {
	if (firstTime)
	    firstTime = false;
	else
	    cm = new ConnectionManager(
		new TracingConnectionReader(this, host, port,
					    new DTDCache(), outputFileName));

        m_start = System.currentTimeMillis();
        cm.executeQuery(new SynchronousQPQuery(queryText),
                        Integer.MAX_VALUE);
    }

    public void notifyFinalResult(int id) {
	queryDone();
    }

    public static void main(String args[]) {
        parseArguments(args);
        TracingClient tc = new TracingClient(host, port, outputFileName);
        tc.processQueries();
    }
}
