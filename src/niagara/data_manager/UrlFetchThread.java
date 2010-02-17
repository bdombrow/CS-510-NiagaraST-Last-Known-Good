package niagara.data_manager;

import niagara.ndom.DOMFactory;
import niagara.utils.CUtil;
import niagara.utils.ParseException;

import org.w3c.dom.Document;

/**
 * The UrlFetchThread class is used to fetch a url to local file.
 */
public class UrlFetchThread implements Runnable {

	private Thread thread;
	private UrlQueue urlQueue;

	// private DMCache hCache;

	public UrlFetchThread(UrlQueue urlQueue, DMCache hc) {

		this.urlQueue = urlQueue;
		// hCache = hc;
		thread = new Thread(this);
		thread.setDaemon(true);
		thread.start();
	}

	/**
	 * This is the run method invoked by the Java thread - it simply grabs the
	 * next query, executes it, and then repeats.
	 */
	public void run() {
		do {
			UrlQueueElement nextUrl = (UrlQueueElement) urlQueue.getUrl();
			// try {
			// System.err.println("URL Fetch Thread running");
			execute(nextUrl);
			// } catch (Exception e) {
			// System.err.println("URL Fecth exception");
			// e.printStackTrace();
			// }
		} while (true);
	}

	public void interrupt() {
		thread.interrupt();
	}

	private boolean execute(UrlQueueElement urlObj) {
		if (urlObj == null) {
			return false;
		}
		String url = urlObj.getUrl();
		MemCacheEntry me = urlObj.getMemCacheEntry();

		Document doc = null;
		try {
			doc = CUtil.parseXML(url);

		} catch (ParseException e) {
			System.err
					.println("WARNING: Exception occurred during parsing of: "
							+ url);
			return false;
		}

		if (doc == null) {
			System.err.println("Fetch URL failed. Creating null document");
			doc = DOMFactory.newDocument();
		}
		if (me != null) {
			me.setval(doc);
			me._notifyWaitingThreads();
		}

		return true;
	}
}
