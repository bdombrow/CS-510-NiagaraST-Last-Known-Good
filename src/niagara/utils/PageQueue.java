package niagara.utils;

/**
 * Array-based implementation of a queue of TuplePages for use in streams.
 */

public class PageQueue {
	// length of the queue; the length passed to the constructor
	// plus 1, 1 slot remains unused when queue is full, so we
	// can tell the difference between full and empty
	private int length;
	private TuplePage[] queue;
	int EXPANSE_LIMIT = 100; 


	// pointers to the head and rear of the queue, respectively
	private int head = 0, tail = 0;

	// some queues can grow and never get full - this is for
	// downstream control queues which should never grow large anyway
	private boolean expandable;

	/**
	 * This is the constructor for the Queue class
	 * 
	 * @param length
	 *            the length of this queue;
	 **/
	public PageQueue(int length, String name) {
		this(length, false, name);
	}

	/**
	 * This is the constructor for the Queue class
	 * 
	 * @param length
	 *            the length of this queue;
	 **/
	public PageQueue(int len, boolean expandable, String name) {
		if (length <= 0)
			length = 5;

		length = len + 1;
		queue = new TuplePage[length];
		this.expandable = expandable;
	}

	/**
	 * @return true if the queue is empty; false otherwise
	 */
	public boolean isEmpty() {
		return (head == tail);
	}

	public boolean isNonEmpty() {
		return (head != tail);
	}

	/**
	 * @return true if the queue is full; false otherwise
	 */
	public boolean isFull() {
		return ((tail + 1) % length == head);
	}

	/**
	 * @return the object at the front of the queue; return null if the queue is
	 *         empty
	 **/
	public TuplePage get() {
		if (isNonEmpty()) {
			head = (head + 1) % length;
			TuplePage p = queue[head];
			queue[head] = null;
			return p;
		} else
			return null;
	}

	/**
	 * @param page
	 *            The tuple page to be appended to the end of the queue
	 * 
	 **/
	public void put(TuplePage page) {

		if (page == null)
			System.err.println("put a null value to page queue");

		assert !isFull() || expandable : "KT putting into full un-expandable PageQueue not allowed";
		if (!isFull()) {
			tail = (tail + 1) % length;
			queue[tail] = page;
		} else if (expandable) {
			if (expandQueue()) {
				put(page);
			} else {
				// We drop a control message, the queue is too big and we shouldn't expand.
				
				//System.err.println("Dropping control flag "
				//		+ page.getFlag().flagName() + "on stream " + name);
				//System.err.println("length of the queue: " + length);
				// queue too big, refused to expand
				//assert !page.hasTuples() : "KT - help dropping tuples";

			}
		}
	}

	private boolean expandQueue() {
		if (length >= EXPANSE_LIMIT) {
		//System.out.println(this.toString() + "refuse to expand");
			return false; // refuse to expand*/
		}

		TuplePage[] newQueue = new TuplePage[length * 2];

		for (int i = 0; i < length; i++) {
			newQueue[i] = queue[(head + i) % length];
		}

		head = 0;
		tail = length - 1;
		queue = newQueue;
		length = length * 2;
		//System.out.println(this.toString() + " expanded");

		return true;
	}

	public String toString() {

		String retString = new String();

		retString += "-----------------------------------------------\n";
		for (int i = 0; i < queue.length; i++) {
			if (head == tail) {
				retString += "(empty)\n";
				break;
			}
			if (i == head)
				retString += "HEAD " + i + ": ";
			else if (i == tail)
				retString += "TAIL " + i + ": ";
			else if ((head < tail && (i < head || i > tail)) || head > tail
					&& (i < head && i > tail))
				retString += "open " + i + ": ";
			else
				retString += "elem " + i + ": ";
			retString += queue[i];
			retString += "\n";
		}
		retString += "-----------------------------------------------\n";
		return retString;
	}

	public int getHead() {
		return head;
	}

	public int getTail() {
		return tail;
	}
}
