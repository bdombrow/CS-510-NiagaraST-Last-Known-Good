package niagara.query_engine;

import java.util.Date;

import niagara.connection_server.NiagraServer;
import niagara.utils.ControlFlag;
import niagara.utils.PEException;
import niagara.utils.PageStream;
import niagara.utils.ShutdownException;
import niagara.utils.SourceTupleStream;

/**
 * The QueryInfo class stores all data associated with a query and provides
 * access to that data. It is stored internal to the query engine.
 * 
 * @version 1.0
 * 
 */

public class QueryInfo {
	// These represent the states of the query
	public static final int STATE_ACTIVE = 0;
	public static final int STATE_DEAD = 1;

	// The query as a string
	private String queryString;

	// The query id
	private int queryId;

	// The source of the output stream of the query
	private PageStream outputPageStream;
	private SourceTupleStream sourceTupleStream;

	// The active query list to which the query belongs
	ActiveQueryList activeQueryList;

	// The registration time in millisecs
	private long regTime;

	// The state of this query
	private int queryState;

	// The head operator thread of the query
	private Thread headOperatorThread;

	// Flag to indicate whether this query should be removed by the physical
	// head op.
	//
	private boolean removeFromActive;

	/** The planID that was issued to this query if it was first "prepared" */
	private String planID;

	/**
	 * Constructor for QueryInfo objects.
	 * 
	 * @param queryString
	 *            the query as a string
	 * @param queryId
	 *            a unique query id
	 * @param outputPageStream
	 *            the output stream of the query
	 * @param activeQueryList
	 *            the active query list the query info is to be part of
	 * 
	 * @exception ActiveQueryList.QueryIdAlreadyPresentException
	 *                if the query id for the query is already present in
	 *                activeQueryList
	 */

	public QueryInfo(String queryString, int queryId,
			PageStream outputPageStream, ActiveQueryList activeQueryList,
			boolean remove)
			throws ActiveQueryList.QueryIdAlreadyPresentException {

		this.removeFromActive = remove;
		this.queryString = queryString;
		this.queryId = queryId;

		// Initialize the source of the output stream
		this.outputPageStream = outputPageStream;
		sourceTupleStream = new SourceTupleStream(outputPageStream);

		// Initialize the active query list
		this.activeQueryList = activeQueryList;

		// Add this object to active query list
		activeQueryList.addQueryInfo(queryId, this);

		// Initialize the creation time for this query
		regTime = new Date().getTime();

		// Initialize the state of the query
		queryState = STATE_ACTIVE;

		// Initially, there is no head operator thread
		headOperatorThread = null;
	}

	/**
	 * Returns the query string of this query object
	 * 
	 * @return the query string
	 */

	public String getQueryString() {
		return queryString;
	}

	/**
	 * Returns the query string of this query object
	 * 
	 * @return flag indicating if the physical head operator should remove this
	 *         query info object from the active query list when done;
	 */

	public boolean removeFromActiveQueries() {

		return removeFromActive;
	}

	/**
	 * Returns the time the query entered the system
	 * 
	 * @return the time the query entered the system
	 */
	public long getRegisteredTime() {

		return regTime;
	}

	/**
	 * Returns the query id of this query object
	 * 
	 * @return query id issued to the query
	 */

	public int getQueryId() {

		return queryId;
	}

	/**
	 * Returns the output stream of the query
	 * 
	 * @return output stream of the query
	 */
	/*
	 * this fcn is used to allow people to create a sink tuple stream for the
	 * top operator in the tree - should feed into query result/query info - KT
	 */
	public PageStream getOutputPageStream() {
		return outputPageStream;
	}

	/**
	 * Kills the query in the system by shutting down all the operators, if any.
	 */

	public synchronized void killQueryWithOperators() {

		// If query is already dead, nothing to do. This test is important
		// because the query may have already been completed when this is
		// called. This ensures that a wrong thread (that previously serviced
		// the query) is not interrupted.
		//
		if (queryState != STATE_DEAD) {

			// Make the state dead
			//
			queryState = STATE_DEAD;

			// If there is a head operator thread, then interrupt it to
			// kill query
			//
			if (headOperatorThread != null) {

				headOperatorThread.interrupt();
			}
		}
	}

	/**
	 * Kills the query in the system (before operators could have been invoked
	 * for it)
	 */

	public synchronized void killQueryWithoutOperators() {

		// There should not be a test to do nothing if the state is dead.
		// This is because a killQueryWithOperator may be invoked without
		// knowing the exact state of the system. If the above call was
		// invoked before operators were created for the query, then
		// even though the state is dead, it is still necessary to
		// to continue and close the stream and send error messages etc.

		// Make the state dead
		//
		queryState = STATE_DEAD;
		// KT nice if this didn't need a source tuple stream
		try {
			// Send a shut down control message to the output stream
			sourceTupleStream.putCtrlMsg(ControlFlag.SHUTDOWN, "Query Killed");
		} catch (ShutdownException e) {
			// ignore since we are shutting down anyway...
		}
	}

	/**
	 * This function sets the head operator thread associated with the query
	 * 
	 * @param headOperatorThread
	 *            The head operator thread
	 */

	public synchronized void setHeadOperatorThread(Thread headOperatorThread) {

		// First set the value
		//
		this.headOperatorThread = headOperatorThread;

		// If the query is dead, then interrupt the head operator thread
		// to kill query
		//
		if (queryState == STATE_DEAD) {

			headOperatorThread.interrupt();
		}
	}

	/**
	 * Returns the state of the query, possible values are ACTIVE, DEAD
	 * 
	 * @return queryState - the state of the query
	 */

	public synchronized int getState() {

		return queryState;
	}

	/**
	 * Removes the query info object from the active query list it is part of
	 */

	synchronized public void removeFromActiveQueryList() {

		// Change state of query to dead. This is necessary because there
		// could be other parts of the system trying to kill the query
		// just after this. They should realize that the query is already
		// dead.
		//
		queryState = STATE_DEAD;

		// Remove query from active list
		try {
			activeQueryList.removeQueryInfo(queryId);
		} catch (ActiveQueryList.NoSuchQueryException e) {
			throw new PEException("Query Info object for query id " + queryId
					+ " not present in active query list");
		}

		System.out.println("KT: Query with id " + queryId
				+ " removed from QueryEngine.activeQueries");

		// If this query was "prepared", remove it from the catalog now that
		// it is done -- it cannot be resurrected
		if (planID != null)
			NiagraServer.getCatalog().removePreparedPlan(planID);

		// Clear the interrupted flag of this thread in case it was interrupted
		// for this query - the function used did not do that and it
		// seems you can't reset interrupted fla
	}

	/**
	 * Returns a string representation of the QueryInfo object
	 * 
	 * @return string representation of query object
	 */

	public synchronized String toString() {

		// The string to return
		//
		String retStr = new String();

		retStr += "--------------------------------------------------------\n";
		retStr += "query: " + queryString + "\n";
		retStr += "query id: " + queryId + "\n";
		retStr += "registration time: " + regTime + "\n";
		retStr += "query state: " + queryState + "\n";
		retStr += "head operator thread: " + headOperatorThread + "\n";
		retStr += "---------------------------------------------------------\n";

		return retStr;
	}

	public void setPlanID(String planID) {
		this.planID = planID;
	}
}
