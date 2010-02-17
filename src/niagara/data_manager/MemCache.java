package niagara.data_manager;

import java.util.Enumeration;
import java.util.Hashtable;

import niagara.ndom.DOMFactory;
import niagara.utils.PEException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Niagra DataManager Replace algorithm for Cache/Disk Management
 * 
 */

@SuppressWarnings("unchecked")
abstract class MemCache implements DMCache {
	// System wide entryHash, thus _Sharing_ between
	// different rep_alg cache is automatic.
	public static Hashtable _entryHash = new Hashtable();
	public static Mutex _mutex = new Mutex();

	// algorithm data
	protected long totalSize;
	protected long currentSize;
	protected long highWatermark;
	protected long lowWatermark;

	protected DMCache lowerCache;

	abstract void addentry(MemCacheEntry entry);

	abstract void removeentry(Object key);

	abstract void replace();

	public static void total_release() {
		_mutex.lock();
		// System.err.println("Releasing MemCache ... size " +
		// _entryHash.size());
		Enumeration en = _entryHash.elements();
		while (en.hasMoreElements()) {
			// System.err.println("Releasing 1 cache entry");
			MemCacheEntry entry = (MemCacheEntry) en.nextElement();
			entry.flush();
		}
		_mutex.unlock();
	}

	public boolean _add(Object obj, FetchThread fth) {
		String key = CacheUtil.normalizePath(obj);
		MemCacheEntry me = (MemCacheEntry) _entryHash.get(key);
		if (me == null) {
			System.err.println("$$$$$$$ ERROR ERROR : _add: " + obj);
			return false;
		}

		// System.err.println("In MC::_add: cacheSize " +_entryHash.size());
		if (me._removeWaitingThread(fth)) {
			// I am the last waiting thread, so I need to put
			// it in _MY_ cache.
			// System.err.println("Will add entry " + obj);
			// System.err.println("In MC::_add: cacheSize " +_entryHash.size());
			addentry(me);
			// System.err.println("Successfully added" + _entryHash.size());
		}
		return true;
	}

	public void setLowerCache(DMCache p) {
		lowerCache = p;
	}

	/**
	 * Cache LookUp routine. If a file or URL is available in MemCache, it is
	 * returned imediately. Otherwise passed to DiskCache. If locally cached in
	 * Disk or is local file, it is parsed, and returned. Otherwise passed to
	 * WebCache.
	 * 
	 * @param key
	 *            , file or URL to fetch
	 * @param th
	 *            , the fetch thread.
	 * @return Document fetched. NULL if fetch goes to web.
	 */
	public synchronized Object fetch(Object k, Object th)
			throws CacheFetchException {
		FetchThread fth = (FetchThread) th;
		String key = CacheUtil.normalizePath(k);
		MemCacheEntry ret = (MemCacheEntry) _entryHash.get(key);
		// System.out.println("******** MemCache::Fetch : " + k);
		if (ret == null) { // Cache Miss.
			// System.err.println("MemCache Miss");
			_mutex.lock(); // LOCK mutex befor update _entryHash.
			ret = (MemCacheEntry) _entryHash.get(key);
			if (ret == null) { // Nobody else mess with this key.
				ret = new MemCacheEntry(key, null);
				ret._initWaiting();
				_entryHash.put(key, ret);
				_mutex.unlock();
				Object nret = null;
				try {
					nret = lowerCache.fetch(key, ret);
				} catch (CacheFetchException cfe) {
					// Exception in DiskCache. Use a dummy ...
					// // System.err.println("Got a ONCE file.");
					ret.setOnce(1);
					nret = DOMFactory.newDocument();
				}
				if (nret == null) { // Miss bad. Goto web and return null
					// System.err.println("MemCache Miss BAD.  Return NULL");
					if (fth == null) { // No fetching Thread.
						// This is really bad miss. No fetching
						// thread means the fetch is called _WITHIN_
						// trigger or dm. They should be only on local
						// cached files. In Fact. They _SHOULD_ call
						// fetch_reload.
						try {
							unpin(key);
						} catch (CacheUnpinException upe) {
							// this exception was ignored before... KT
							throw new PEException("CacheUnpinException "
									+ upe.getMessage());
						}
						throw (new CacheFetchException("Pin Remote File"));
					}
					ret._addWaitingThread(fth);
					return null;
				} else { // Hijacked locally. Happily return.
					// System.err.println("MC:: DISK HIT");
					_mutex.lock();
					ret.setval(nret);
					Element root = ((Document) nret).getDocumentElement();
					if (root != null) {
						String ts = root.getAttribute("TIMESPAN");
						if (ts != null && !ts.equals("")) {
							ret.setTimeSpan(Long.parseLong(ts));
						}
					}

					if (ret.isOnce()) {
						// System.out.println("MC:: DISK HIT ONCE FILE");
						_entryHash.put(key, ret);
					} else {
						boolean testW = ret._notifyWaitingThreads();
						if (!testW) {
							this.addentry(ret);
						}
					}
					// System.err.print("MC:: UNLOCKING MUTEX");
					_mutex.unlock();
					return nret;
				}
			} else { // ret = null in first access, but !=null after mutex.
				Object toret = ret.val;
				if (toret == null)
					ret._addWaitingThread(fth);
				_mutex.unlock();
				return toret;
			}
		} else { // ret != null in first access.
			Object toret = ret.val;
			if (toret == null) { // OOPS. Hit on a _not_completed_fetch_
				// System.err.println("Hit a _not_completed_fetch_ " + key);
				_mutex.lock();
				toret = ret.val;
				if (toret == null)
					ret._addWaitingThread(fth);
				_mutex.unlock();
				return toret;
			}

			// System.err.println("%%$$%%^^ CacheHIT " + key);
			// System.err.println("Will return " + getType(key));
			return toret;
		}
	}

	/**
	 * This is the _BLOCKING_ version of fetch
	 */
	public Object fetch_reload(Object k, Object th) throws CacheFetchException {
		MemCacheEntry me = null;
		String key = CacheUtil.normalizePath(k);
		_mutex.lock();
		me = (MemCacheEntry) _entryHash.get(key);
		if (me != null) {
			// System.err.println("%%%%%% Cache_Reload. HIT" + k + " on " +
			// me.val);
			_mutex.unlock();
			return me.val;
		}

		me = new MemCacheEntry(key, null);
		_entryHash.put(key, me);

		me.addPinCount();
		_mutex.unlock();
		Object v = lowerCache.fetch_reload(key, me);
		if (v == null) {
			// System.err.println("Fetch Reload: got NULL from lower" + key);
			v = DOMFactory.newDocument();
		} else {
			Element root = ((Document) v).getDocumentElement();
			if (root != null) {
				String ts = root.getAttribute("TIMESPAN");
				if (ts != null && !ts.equals("")) {
					long tsp = Long.parseLong(ts);
					me.setTimeSpan(tsp);
				}
			}
		}
		_mutex.lock();
		me.setval(v);
		_mutex.unlock();
		addentry(me);
		me.minusPinCount();
		return me.val;
	}

	/**
	 * pin the key=>val pair in Memory. The pair maybe pined mutiple times.
	 */
	public void pin(Object key) throws CachePinException {
		MemCacheEntry kme = null;
		Object keykey = null;
		if (key instanceof MemCacheEntry) {
			kme = (MemCacheEntry) key;
			keykey = kme.key;
			_mutex.lock();
			MemCacheEntry me = (MemCacheEntry) _entryHash.get(keykey);
			if (me != null) {
				me.addPinCount();
				if (kme != null) {
					me.setval(kme.val);
					if (kme.isOnce())
						me.setOnce(kme.getOnce());
				}
			}
			_mutex.unlock();
		} else {
			_mutex.lock();
			MemCacheEntry me = (MemCacheEntry) _entryHash.get(key);
			if (me != null) {
				me.addPinCount();
				_mutex.unlock();
			} else {
				_mutex.unlock();
				Object val = null;
				try {
					val = fetch_reload(key, null);
				} catch (CacheFetchException cfe) {
					throw (new CachePinException("Pin Null"));
				}
				if (val != null)
					pin(new MemCacheEntry(key, val));
			}
		}
	}

	/**
	 * Unpin the key->val pair. The pair is dropped from memory only if the
	 * pinCount of MemCacheEntry is 0.
	 */
	public void unpin(Object key) throws CacheUnpinException {
		MemCacheEntry me = (MemCacheEntry) _entryHash.get(key);
		if (me == null)
			throw (new CacheUnpinException("Unpin NULL"));
		int pinc = me.minusPinCount();
		if (pinc == 0) {
			if (me.cache == null) {
				_mutex.lock();
				_entryHash.remove(key);
				System.out.println("Unpin remove a entry " + key);
				System.out.println("Now entryHash size " + _entryHash.size());
				_mutex.unlock();
				me.flush();
			}
		}
	}
}
