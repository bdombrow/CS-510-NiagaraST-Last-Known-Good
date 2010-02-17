package niagara.utils;

/////////////////////////////////////////////////////////////////////////
//
//   SynchronizedQueue.java:
//   NIAGRA Project
//   
//   Jun Li
//
//   This class is derived from class Queue
//   Access to every method of a SychronizedQueue is synchronized
/////////////////////////////////////////////////////////////////////////

public class SynchronizedQueue extends Queue {
	// //////////////////////////////////////////////////////////////////////////
	// Class Methods
	// //////////////////////////////////////////////////////////////////////////

	/**
	 * This is the constructor
	 * 
	 * @param len
	 *            the length of this queue; if it's not greater than 0, it'll be
	 *            set to 1
	 **/
	public SynchronizedQueue(int len) {
		super(len);
	}

	/**
	 * @return true if the queue is empty; false otherwise
	 */
	public synchronized boolean isEmpty() {
		return super.isEmpty();
	}

	/**
	 * @return true if the queue is full; false otherwise
	 */
	public synchronized boolean isFull() {
		return super.isFull();
	}

	/**
	 * @return the object at the front of the queue; return null if the queue is
	 *         empty
	 **/
	public synchronized Object get() {
		try {
			while (isEmpty()) {
				wait();
			}

			Object o = super.get();
			notifyAll();
			return o;
		} catch (InterruptedException ie) {
			// think really this method should throw interrupted exception
			assert false : "Interrupted during synchqueue.get - what??";
			return null;
		}
	}

	/**
	 * @param o
	 *            The object to be put in the queue
	 * @param atTail
	 *            It's true if o is to be appended at the ead of the queue;
	 *            otherwise o will be put at the front of the queue
	 **/
	public synchronized boolean put(Object o, boolean atTail) {

		// System.out.println("SyncQ: top of put call");
		while (isFull()) {
			try {
				wait();
			} catch (Exception e) {
				System.out.println("error in calling wait( ) in put( )");
				System.exit(-1);
			}
		}

		if (super.put(o) != true) {
			System.out
					.println("That's impossible! Something wierd happened in SyncronizedQueue.put( )");
			System.exit(-1); // better if raising an exception
		}

		notifyAll();
		// System.out.println("SyncQ: Notified all from call to put !!!");
		return true;
	}

	public synchronized String toString() {
		return super.toString();
	}
}
