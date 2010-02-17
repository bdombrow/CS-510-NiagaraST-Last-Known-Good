package niagara.data_manager;

import java.util.Vector;

import niagara.utils.PEException;
import niagara.utils.ShutdownException;
import niagara.utils.SinkTupleStream;
import niagara.xmlql_parser.regExp;

import org.w3c.dom.Document;

/**
 * Initial implementation of data manager no memory management essentially
 * managing two directories
 * 
 * @version 1
 */
@SuppressWarnings("unchecked")
public class CacheManager {
	private String cacheDir;
	private DiskCache diskCache;
	private MemCache memCache;

	// private Vector otherCacheVec;

	// private long vClock;

	// vClock(virtual Clock) is used to syncronize with EventDector.
	// CacheManager and ED are 2 threads, so they see different real
	// clock. CacheManager use virtual clock set by ED to avoid
	// confusion. Idea is simple, whenever ED communicate with DM,
	// it pass ED's clock. DM adjust vClock accordingly.

	public CacheManager(String path) {
		cacheDir = path;
		// System.err.println("Using " + cacheDir + " as Cache Dir");
		diskCache = new DiskCache(cacheDir);
		memCache = new MemCacheFIFO(20, 10, 15);
		memCache.setLowerCache(diskCache);
		// otherCacheVec = new Vector();
		// vClock = 0;
	}

	public synchronized boolean getDocuments(Vector xmlURLList,
			regExp pathExpr, SinkTupleStream stream) throws ShutdownException {
		// int numFetches = xmlURLList.size();
		// System.err.println("Getting " + numFetches + " docs");
		// Put in the fetchInfo object in service queue
		//
		String tmps = (String) xmlURLList.elementAt(0);
		if (CacheUtil.isAccumFile(tmps)) {
			getAccumFile(tmps, stream);
		} else if (CacheUtil.isOrdinary(tmps)) {
			// System.err.println("CacheM: Trying get Normal file " + tmps);
			// Vector dottedPaths = null; // DMUtil.convertPath(pathExpr);
			// FetchRequest newRequest = new FetchRequest(stream, xmlURLList,
			// dottedPaths);
			// FetchThread fth = new FetchThread(newRequest, memCache);
		} else {
			// XXX vpapad: we should never get here
			// what the !@#!@# does isOrdinary() do?
			assert false;
			// System.err.println("CacheM. getDoc " + tmps);
			// getTrigDocument(tmps, pathExpr, stream);
		}
		return true;
	}

	/**
	 * Function to retrieve the most current document associated with an
	 * Accumulate File. Finds the document using the global Accumulate File
	 * Directory
	 * 
	 * @param afName
	 *            The name of the accum file
	 * @param s
	 *            Stream to put result into.
	 * 
	 */
	private void getAccumFile(String afName, SinkTupleStream outputStream)
			throws ShutdownException {
		try {
			Document accumDoc = (Document) DataManager.AccumFileDir.get(afName);
			outputStream.put(accumDoc);
			outputStream.endOfStream();
		} catch (java.lang.InterruptedException e) {
			throw new PEException("What happened?!" + e.getMessage());
		}
		return;
	}

	public void shutdown() {
		// System.err.println("shutting down cache manager ...");
		try {
			MemCache.total_release();
			diskCache.release();
		} catch (CacheReleaseException cre) {
			// System.err.println("Cannot Gracefully shutdown CacheManager");
		}
	}
}
