package niagara.connection_server;

import niagara.query_engine.QueryResult;

/** Stores the info about a query in the server */
public class ServerQueryInfo {

	// constants for the variable queryType
	public final static int QueryEngine = 1;
	public final static int AccumFile = 4;

	private int queryType;

	// The tranmitter thread for this query
	private ResultTransmitter transmitter;
	private int queryId;

	// Is this query synchronous?
	// (Implies no result padding, and closing of connection at the end)
	private boolean synchronous;

	// qe query related data
	private QueryResult queryResult;
	private String accumFileName;

	/**
	 * Constructor
	 * 
	 * @param queryId
	 *            The Server Query Id (different from QID given by QE/Client
	 * @param queryType
	 *            Tells the module to which this query belongs
	 */
	public ServerQueryInfo(int queryId, int queryType) {
		this.queryId = queryId;
		this.queryType = queryType;
	}

	public ServerQueryInfo(int queryId, int queryType, boolean synchronous) {
		this(queryId, queryType);
		this.synchronous = synchronous;
	}

	boolean isQEQuery() {
		return queryType == QueryEngine;
	}

	boolean isAccumFileQuery() {
		return queryType == AccumFile;
	}

	boolean isSynchronous() {
		return synchronous;
	}

	public int getQueryId() {
		return queryId;
	}

	public void setQueryResult(QueryResult qr) {
		queryResult = qr;
	}

	public void setTransmitter(ResultTransmitter rt) {
		transmitter = rt;
	}

	public void setAccumFileName(String afName) {
		accumFileName = afName;
	}

	public ResultTransmitter getTransmitter() {
		return transmitter;
	}

	public QueryResult getQueryResult() {
		return queryResult;
	}

	public String getAccumFileName() {
		return accumFileName;
	}
}
