package niagara.data_manager;

/**
 * Niagra DataManager
 * Replace algorithm for Cache/Disk Management
 * 
 */
import java.io.File;
import java.io.IOException;

import niagara.utils.CUtil;
import niagara.utils.ParseException;

import org.w3c.dom.Document;

class DiskCache implements DMCache {

	protected String dirName;
	protected DMCache lowerCache;

	/**
	 * Constructor.
	 * 
	 * @param path
	 *            the Dir Path name for storing files
	 */
	public DiskCache(String path) {
		dirName = path;
		lowerCache = new WebCache(10, this);
	}

	/** clean up. Just let lowerCache clean up */
	public void release() throws CacheReleaseException {
		lowerCache.release();
	}

	/** Pin a file. Keep a copy of XML file in DiskCache dir */
	public void pin(Object k) throws CachePinException {
		if (!(k instanceof String))
			throw (new CachePinException("Pin Non String"));
		String key = (String) k;
		try {
			if (CacheUtil.isUrl(key))
				CacheUtil.fetchUrl(key, dirName + CacheUtil.urlToFile(key));
			else {
				CacheUtil.fetchLocal(key, dirName + CacheUtil.pathToFile(key));
			}
		} catch (IOException ioe) {
			throw (new CachePinException("Cannot Pin File"));
		}
	}

	/** Unpin a file. Delete the saved copy from DiskCacheDir */
	public void unpin(Object k) throws CacheUnpinException {
		if (!(k instanceof String))
			throw (new CacheUnpinException("Unpin Non String"));
		String key = (String) k;
		String dfn = null;
		if (CacheUtil.isUrl(key))
			dfn = dirName + CacheUtil.urlToFile(key);
		else
			dfn = dirName + CacheUtil.pathToFile(key);
		// try {
		File tmpF = new File(dfn);
		if (tmpF.exists())
			tmpF.delete();
		// } catch (Exception fe) {
		// throw(new CacheUnpinException("Unpin failed"));
		// }
	}

	/**
	 * Given File or URL name, fetch Document
	 * 
	 * @param k
	 *            , the key (filename, url) to fetch
	 * @param me
	 *            , MemCacheEntry to hold waitingThread
	 */
	public Object fetch(Object k, Object me) throws CacheFetchException {

		// MemCacheEntry mentry = (MemCacheEntry)me;

		if (k == null || !(k instanceof String)) {
			throw (new CacheFetchException("Non String Key"));
			// return null;
		}

		Document doc = null;
		String key = (String) k;
		String dfn = null;

		try {
			if (!CacheUtil.isUrl(key)) { // Hijack local file
				doc = CUtil.parseXML(key);
				if (doc == null) {
					// System.err.println("Local doc not found.");
					throw (new CacheFetchException("NO LOCAL FILE"));
				}
				// System.out.println("++++++ DC::Fetch Fetching local File " +
				// k);
			} else { // URL from WEB.
				dfn = dirName + CacheUtil.urlToFile(key);
				File tmpF = new File(dfn);
				if (tmpF.exists()) { // Hijack locally pinned URL
					doc = CUtil.parseXML(dfn);
				}
				if (doc == null && me != null) // go to WEB.
					lowerCache.fetch(key, me);
			}
		} catch (ParseException pe) {
			throw new CacheFetchException("Error parsing " + key);
		}
		return doc;
	}

	/**
	 * Given filename or URL, fetch Document. This Method blocks to get back a
	 * result.
	 * 
	 * @param k
	 *            , key.
	 * @param me
	 *            , not used.
	 */
	public Object fetch_reload(Object k, Object me) throws CacheFetchException {
		try {
			if (k == null || !(k instanceof String))
				return null;
			// System.err.println("Parsing ... " + k);
			return CUtil.parseXML((String) k);
		} catch (ParseException pe) {
			throw new CacheFetchException("Error parsing: " + k);
		}
	}
}
