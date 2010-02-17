package niagara.data_manager;

import java.util.Vector;

@SuppressWarnings("unchecked")
class MemCacheFIFO extends MemCache {
	private Vector entryList;

	MemCacheFIFO(long totalS, long lw, long hw) {
		totalSize = totalS;
		currentSize = 0;
		highWatermark = hw;
		lowWatermark = lw;
		entryList = new Vector();
	}

	public void release() throws CacheReleaseException {
		for (int i = 0; i < entryList.size(); i++) {
			MemCacheEntry entry = (MemCacheEntry) entryList.elementAt(i);
			if (entry.getPinCount() == 0) {
				_entryHash.remove(entry.key);
				entry.flush();
			} else
				entry.setCache(null);
		}
		entryList = null;
	}

	synchronized void addentry(MemCacheEntry entry) {
		// Object key = entry.key;
		entryList.addElement(entry);
		currentSize += entry.size;
		// System.err.println("Adding " + key + " to Cache.  CurrentSize " +
		// currentSize);
		// System.err.println("Global Size " + _entryHash.size());
		replace();
		// System.err.println("After Replace " + _entryHash.size() + " "
		// + currentSize);
	}

	synchronized void removeentry(Object key) {
		MemCacheEntry entry = (MemCacheEntry) _entryHash.get(key);
		// System.err.println("Removeentry " + key);
		if (entry != null) {
			if (entryList.removeElement(entry)) {
				if (entry.getPinCount() == 0) {
					// System.err.println("Remove entry.  Remove from cache");
					// System.err.println("Now entryHash size " +
					// _entryHash.size());
					_entryHash.remove(key);
					entry.flush();
				} else {
					entry.setCache(null);
					// System.err.println("Remove entry.  Leave it pinned");
				}
				currentSize -= entry.size;
			}
		}
	}

	synchronized void replace() {
		if (currentSize < highWatermark)
			return;
		while (currentSize > lowWatermark) {
			MemCacheEntry todel = (MemCacheEntry) entryList.elementAt(0);
			removeentry(todel);
			currentSize--;
		}
	}
}
