package niagara.query_engine;

import java.util.Enumeration;
import java.util.Hashtable;

/**
 * The ActiveQueryList class is used to store the queries that are active in the
 * query engine. Objects of type <code>QueryInfo</code> are stored in this list.
 * The public interface include the ability to add a query, remove a query and
 * retrieve a query. Methods are synchronized because multiple threads may
 * access the list.
 * 
 * 
 * @see QueryInfo
 */
@SuppressWarnings( { "serial", "unchecked" })
public class ActiveQueryList {

	// /////////////////////////////////////////////////////////////////
	// Exceptions thrown by the class //
	// /////////////////////////////////////////////////////////////////

	/**
	 * This exception is thrown if an attempt is made to add a query with an
	 * existing id
	 */

	public class QueryIdAlreadyPresentException extends Exception {

	}

	/**
	 * This exception is thrown if an attempt is made to remove a query that
	 * does not exist
	 */

	public class NoSuchQueryException extends Exception {

	}

	// /////////////////////////////////////////////////////////////////
	// These are the private members of the class //
	// /////////////////////////////////////////////////////////////////

	// A hashtable to store the active queries
	//
	private Hashtable queryList;

	// /////////////////////////////////////////////////////////////////
	// These are the methods of the class //
	// /////////////////////////////////////////////////////////////////

	/**
	 * This is the constructor for the active query list
	 */

	public ActiveQueryList() {

		// Allocate a hashtable, may want to specify init size in future.
		//
		queryList = new Hashtable();
	}

	/**
	 * Adds a query info object to the active query list. The active query list
	 * should never have a query with the same id.
	 * 
	 * @param queryId
	 *            the id of the query to be added
	 * @param queryInfo
	 *            the query information to add to the active list
	 * 
	 * @exception QueryIdAlreadyPresentException
	 *                If a query with the same id is already present in the list
	 */

	synchronized public void addQueryInfo(int queryId, QueryInfo queryInfo)
			throws QueryIdAlreadyPresentException {

		// Get the integer version of query id
		//
		Integer intQueryId = new Integer(queryId);

		// First check whether there is already a query info object with
		// same id
		//
		if (queryList.get(intQueryId) != null) {

			throw new QueryIdAlreadyPresentException();
		}

		// Add the query to the active query list
		//
		queryList.put(intQueryId, queryInfo);
	}

	/**
	 * Retrieves a query info object, given the query id
	 * 
	 * @param queryId
	 *            The id of the query whose query info is to be retrieved
	 * 
	 * @return The desired queryInfo object if it exists; null otherwise
	 */

	synchronized public QueryInfo getQueryInfo(int queryId) {

		return (QueryInfo) queryList.get(new Integer(queryId));
	}

	/**
	 * Removes a query info object, given the query id
	 * 
	 * @param queryId
	 *            The query id of query to be removed from active list
	 * 
	 * @exception NoSuchQueryException
	 *                No query with the given id exists in the list
	 */

	synchronized public void removeQueryInfo(int queryId)
			throws NoSuchQueryException {

		if (queryList.remove(new Integer(queryId)) == null) {

			throw new NoSuchQueryException();
		}
	}

	public Enumeration elements() {
		return queryList.elements();
	}

	/**
	 * Returns the string representation of list
	 * 
	 * @return string representation of list
	 */

	synchronized public String toString() {

		return queryList.toString();
	}
}
