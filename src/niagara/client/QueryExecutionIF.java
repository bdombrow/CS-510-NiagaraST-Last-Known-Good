package niagara.client;

/**
 * This interface defines the methods that are called in response to events in
 * the Gui
 * 
 */

public interface QueryExecutionIF {
	/**
	 * Execute a query
	 * 
	 * @param s
	 *            the query string
	 * @param n
	 *            limit of initial results (after this hitting getnext is
	 *            required)
	 * @return the id of the query in the registry
	 */
	public int executeQuery(Query query, int n) throws ClientException;

	/**
	 * Kill the query
	 * 
	 * @param id
	 *            the query id to kill
	 */
	public void killQuery(int id) throws ClientException;

	/**
	 * Request partial result from a query
	 * 
	 * @param id
	 *            the query id
	 */
	public void requestPartial(int id);

	/**
	 * get the type of the of the query (QueryType object)
	 * 
	 * @param id
	 *            the query id
	 * @return an itn @see QueryType or -1 if the type is bad
	 */
	public int getQueryType(int id);

	/**
	 * Request partial result from a query
	 * 
	 * @param id
	 *            the query id
	 */
	public void getNext(int id, int resultCount) throws ClientException;

	/**
	 * Checks to see if the query has received final results
	 * 
	 * @param id
	 *            the query id
	 * @return true if the result is final
	 */
	public boolean isResultFinal(int id);

	/**
	 * End the session with the server
	 */
	public void endSession();

	/**
	 * Get the query string
	 * 
	 * @param id
	 *            the id of the query
	 * @return the query string
	 */
	public String getQueryString(int id);
}
