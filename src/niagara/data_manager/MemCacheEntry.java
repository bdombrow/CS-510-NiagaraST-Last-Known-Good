package niagara.data_manager;

import java.util.Vector;

import niagara.utils.PEException;

import org.w3c.dom.Document;

/***
 * Niagara. DataManager. MemCacheEntry
 */

@SuppressWarnings("unchecked")
class MemCacheEntry {
	Object key; // usually should be a string
	Object val; // usually a Document

	// stats
	long size;
	boolean dirty;
	int once; // 
	int age;
	int flag;
	volatile int pinCount;
	MemCache cache;

	private long timespan;
	private volatile long timeStamp;

	private Vector waitingThreads;

	// relationship to other entry
	MemCacheEntry prev;
	MemCacheEntry next;

	MemCacheEntry(Object key, Object val) {
		this.key = key;
		this.val = val;
		this.once = 0;
		this.pinCount = 0;
		dirty = false;
		this.size = 1;
		waitingThreads = null;
		cache = null;
		timespan = 0;
	}

	public void setTimeSpan(long ts) {
		timespan = ts;
	}

	public long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(long ts) {
		// System.err.println("MemCaEntry.setTS: Set TimeStamp: " + key + " " +
		// ts);
		timeStamp = ts;
	}

	public void shrink() {
		System.out.println("Shrinking .... ");
		if (val == null) {
			// System.err.println("Trying to shink null vec");
			return;
		}
		if (val instanceof Vector) {
			throw new PEException(
					"XXX vpapad: should never get here with the trigger code removed");
		}
	}

	public void flush() {
		if (dirty) {
			// System.err.println("Flushing a MemCacheEntry");
			if (key instanceof String && val instanceof Document)
				CacheUtil.flushXML((String) key, (Document) val);
			else if (key instanceof String && val instanceof Vector) {
				shrink();
				CacheUtil.flushVec((String) key, timespan, (Vector) val);
			}
		}
	}

	public void setOnce(int o) {
		once = o;
	}

	public boolean isOnce() {
		return (once != 0);
	}

	public boolean minusOnce() {
		once--;
		if (once == 0)
			return true;
		return false;
	}

	public int getOnce() {
		return once;
	}

	public String getType() {
		if (val == null)
			return (new String("Type is NULL"));
		if (val instanceof Document)
			return (new String("Type is Doc"));
		if (val instanceof Vector) {
			Vector v = (Vector) val;
			return (new String("Type is Vec: size " + v.size()));
		}
		return (new String("Unsupported Type in MemCacheEntry"));
	}

	public void setDirty(boolean d) {
		dirty = d;
		if (d)
			timeStamp = System.currentTimeMillis();
	}

	public boolean isDirty() {
		return dirty;
	}

	public synchronized void setval(Object v) {
		// System.err.println("Set Val of " + key);
		// System.err.println("Val is " + v);
		val = v;
	}

	public synchronized int getPinCount() {
		return pinCount;
	}

	public synchronized void setPinCount(int c) {
		pinCount = c;
	}

	public synchronized void addPinCount() {
		pinCount++;
	}

	public synchronized int minusPinCount() {
		if (pinCount > 0)
			pinCount--;
		return pinCount;
	}

	public synchronized void setCache(MemCache toset) {
		cache = toset;
	}

	public synchronized boolean isCacheNull() {
		return (cache == null);
	}

	public void _initWaiting() {
		waitingThreads = new Vector();
	}

	public synchronized void _addWaitingThread(FetchThread fth) {
		waitingThreads.addElement(fth);
	}

	public synchronized boolean _removeWaitingThread(FetchThread fth) {
		waitingThreads.removeElement(fth);
		if (waitingThreads.size() == 0)
			return true;
		return false;
	}

	public synchronized boolean _notifyWaitingThreads() {
		if (waitingThreads.size() == 0)
			return false;
		for (int i = 0; i < waitingThreads.size(); i++) {
			((FetchThread) waitingThreads.elementAt(i))._notify();
		}
		return true;
	}
}
