package niagara.utils;

/////////////////////////////////////////////////////////////////////////
//
//   Queue.java:
//   NIAGRA Project
//   
//   Jun Li
//
//   Array implementation of a queue.
// 
/////////////////////////////////////////////////////////////////////////

/**
 * Array based implementation of a queue
 * 
 */
public class Queue {
	// //////////////////////////////////////////////////////////////////////////
	// Class Data Members
	// //////////////////////////////////////////////////////////////////////////

	// length of the queue; actually it's the length passed to the constructor
	// plus 2. Two slots remain unused when the queue is "full", one of which
	// is reserved for guaranteedPut( )
	private int length = 0;

	// queue is an array of objects
	private Object[] queue = null;

	// pointers to the head and rear of the queue, respectively
	private int head = 0, tail = 0;

	// //////////////////////////////////////////////////////////////////////////
	// Class Methods
	// //////////////////////////////////////////////////////////////////////////

	/**
	 * This is the constructor for the Queue class
	 * 
	 * @param len
	 *            the length of this queue; if it's not greater than 0, it'll be
	 *            set to 2
	 **/
	public Queue(int len) {
		if (len <= 0)
			len = 1;

		length = len + 2;
		queue = new Object[length];
	}

	/**
	 * @return true if the queue is empty; false otherwise
	 */
	public boolean isEmpty() {
		return (head == tail);
	}

	/**
	 * @return true if the queue is full; false otherwise
	 */
	public boolean isFull() {
		return ((tail + 1) % length == head || (tail + 2) % length == head);
	}

	/**
	 * @return the object at the front of the queue; return null if the queue is
	 *         empty
	 **/
	public Object get() {
		if (!isEmpty()) {
			head = (head + 1) % length;
			Object o = queue[head];
			queue[head] = null;
			return o;
			// return queue[head];
		} else
			return null;
	}

	/**
	 * @param o
	 *            The object to be appended to the end of the queue
	 * 
	 * @return true if put is successful; false otherwise
	 **/
	public boolean put(Object o) {
		if (!isFull()) {
			tail = (tail + 1) % length;
			queue[tail] = o;

			return true;
		} else
			return false;
	}

	/**
	 * Append the object to the end of the queue. If the queue is full, put it
	 * in the reserved slot; if this slot is already occupied, simply overwrite
	 * it
	 * 
	 * @param o
	 *            The object to be appended to the end of the queue
	 **/
	public void guaranteedPut(Object o) {
		if ((tail + 1) % length == head)
			queue[tail] = o;
		else {
			tail = (tail + 1) % length;
			queue[tail] = o;
		}
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
}
