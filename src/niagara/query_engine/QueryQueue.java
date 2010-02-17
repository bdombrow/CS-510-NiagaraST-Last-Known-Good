package niagara.query_engine;

import niagara.utils.Queue;
import niagara.utils.SynchronizedQueue;

/**
 * The QueryQueue class stores queries as they are entered into the QueryEngine.
 * Multiple clients threads (producers) may produce queries via the QueryEngine
 * method executeQuery(string), which calls the addQuery() method of the query
 * queue. A queue of primed threads wait (consumers) on the function getQuery()
 * to execute the queries. The query queue is implemented as a
 * SynchronizedQueue. The queue stores queries as QueryInfo objects.
 * 
 * @see QueryThread
 * @see QueryInfo
 * @see QueryEngine
 * @see SynchronizedQueue
 * @see Queue
 */
public class QueryQueue {
	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// Data members of the QueryQueue Class
	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	// A synchronized queue for storing the querys
	private SynchronizedQueue queryQueue;

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// Methods of the QueryQueue Class
	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	/**
	 * This is the constructor for the QueryQueue class that initializes it to
	 * an empty queue.
	 * 
	 * @param maxCapacity
	 *            The maximum capacity of the query queue
	 */
	public QueryQueue(int maxCapacity) {
		// Call the constructor of the super class
		//
		super();

		// Create a synchronized queue to server as an query queue
		//
		queryQueue = new SynchronizedQueue(maxCapacity);
	}

	/**
	 * This function adds an query to the query queue
	 * 
	 * @param query
	 *            The query to be added to the queue
	 */
	public void addQuery(QueryInfo query) {
		// Add the query to the end of the queue
		queryQueue.put(query, true);
	}

	/**
	 * This function gets an query from the query queue
	 * 
	 * @return The query at the head of the queue
	 */
	public QueryInfo getQuery() {
		// Get the query from the queue
		return (QueryInfo) queryQueue.get();
	}

	public synchronized String toString() {
		return queryQueue.toString();
	}
}
