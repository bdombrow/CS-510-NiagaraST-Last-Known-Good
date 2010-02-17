package niagara.connection_server;

import java.util.HashMap;

/**
 * Contains the request message in an object form and implements some utility
 * methods
 */
public class RequestMessage {

	public enum RequestType {
		EXECUTE_QE_QUERY, KILL_QUERY, SUSPEND_QUERY, RESUME_QUERY, GET_NEXT, GET_PARTIAL,
		// Execute a prepared query plan, described in XML
		EXECUTE_QP_QUERY,
		// run the garbage collector
		RUN_GC, SHUTDOWN,
		// Same as QP_QUERY, but start sending back results immediately,
		// assuming this is the only outstanding query for this client
		// (EXECUTE_QP_QUERY and GET_NEXT in one step)
		SYNCHRONOUS_QP_QUERY,
		// Just show the optimized plan
		EXPLAIN_QP_QUERY,
		// A mutant query
		MQP_QUERY, DUMPDATA,
		// Prepared (and instrumented) queries
		PREPARE_QUERY, EXECUTE_PREPARED_QUERY, KILL_PREPARED_QUERY,
		// Tunables
		SET_TUNABLE
	};

	private static HashMap<String, RequestType> queryTypeNames = new HashMap<String, RequestType>();
	static {
		queryTypeNames.put("execute_qe_query", RequestType.EXECUTE_QE_QUERY);
		queryTypeNames.put("kill_query", RequestType.KILL_QUERY);
		queryTypeNames.put("suspend_query", RequestType.SUSPEND_QUERY);
		queryTypeNames.put("resume_query", RequestType.RESUME_QUERY);
		queryTypeNames.put("get_next", RequestType.GET_NEXT);
		queryTypeNames.put("get_partial", RequestType.GET_PARTIAL);
		queryTypeNames.put("execute_qp_query", RequestType.EXECUTE_QP_QUERY);
		queryTypeNames.put("gc", RequestType.RUN_GC);
		queryTypeNames.put("shutdown", RequestType.SHUTDOWN);
		queryTypeNames.put("synchronous_qp_query",
				RequestType.SYNCHRONOUS_QP_QUERY);
		queryTypeNames.put("explain_qp_query", RequestType.EXPLAIN_QP_QUERY);
		queryTypeNames.put("mqp_query", RequestType.MQP_QUERY);
		queryTypeNames.put("dumpdata", RequestType.DUMPDATA);
		queryTypeNames.put("prepare_query", RequestType.PREPARE_QUERY);
		queryTypeNames.put("execute_prepared_query",
				RequestType.EXECUTE_PREPARED_QUERY);
		queryTypeNames.put("set_tunable", RequestType.SET_TUNABLE);
		queryTypeNames.put("kill_prepared_query",
				RequestType.KILL_PREPARED_QUERY);
	};

	private RequestType requestType;
	int serverID;
	int localID;
	private boolean sendImmediate;
	private boolean intermittent;
	// private boolean killquery;
	String requestData;
	boolean asii;

	public RequestMessage() {
		serverID = -1;
		localID = -1;
		asii = false;
	}

	public RequestMessage(RequestType qt) throws InvalidRequestTypeException {
		this.requestType = qt;
		serverID = -1;
		localID = -1;
		asii = false;
	}

	public void setRequestType(RequestType requestType) {
		this.requestType = requestType;
	}

	public void setRequestType(String s) throws InvalidRequestTypeException {
		if (queryTypeNames.containsKey(s))
			setRequestType(queryTypeNames.get(s));
		else
			throw new InvalidRequestTypeException();
	}

	// get the request type of this Message as an integer
	public RequestType getRequestType() {
		return requestType;
	}

	class InvalidRequestTypeException extends Exception {
		private static final long serialVersionUID = 1L;
	}

	public boolean isSendImmediate() {
		return sendImmediate;
	}

	public void setSendImmediate(boolean sendImmediate) {
		this.sendImmediate = sendImmediate;
	}

	public boolean isIntermittent() {
		return intermittent;
	}

	public void setIntermittent(boolean intermittent) {
		this.intermittent = intermittent;
	}
}
