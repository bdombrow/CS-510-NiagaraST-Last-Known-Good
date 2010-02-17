
/** Niagara, DataManager
 */

/** <code>Mutex</code> is Mutex implemented by using
 * Java's built in Monitor.
 */
package niagara.data_manager;

class Mutex {
	private volatile boolean inMutex;

	Mutex() {
		inMutex = false;
	}

	public synchronized void lock() {
		while (inMutex) {
			try {
				wait();
			} catch (InterruptedException ie) {
				System.err.println("Mutex Wait Interrupted");
			}
		}
		inMutex = true;
	}

	public synchronized void unlock() {
		inMutex = false;
		notifyAll();
	}
}
