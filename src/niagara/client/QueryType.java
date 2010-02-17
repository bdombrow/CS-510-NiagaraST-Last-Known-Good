package niagara.client;

/**
 * Query type constants
 */

class QueryType {
	public static final int NOTYPE = -1;
	// public static final int XMLQL = 1; no longer supported
	public static final int QP = 4; // assoc with QPQuery-created in query
									// factor
	public static final int SYNCHRONOUS_QP = 5; // Tracingclient ONLY
	public static final int EXPLAIN = 6; // was EXPLAIN_QP // ok
	public static final int MQP = 7; // MQP and LightMQP client only
	public static final int PREPARE = 8; // was PREPARE_QP - ok
	public static final int EXECUTE_PREPARED = 9; // ok
	public static final int SET_TUNABLE = 10; // ok
	public static final int KILL_PREPARED = 11; // kill a prepared kill;
	public static final int KILL_FOOBAR = 12; // kill a prepared kill;

	// merged RequestType with QueryType
	// types from Request type were:
	// RUN, think -> QP
	// EXPLAIN,
	// PREPARE,
	// EXECUTE_PREPARED,
	// SET_TUNABLE

}
