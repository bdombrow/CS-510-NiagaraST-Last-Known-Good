package niagara.data_manager;

import java.util.Vector;

import niagara.utils.ControlFlag;
import niagara.utils.PEException;
import niagara.utils.ShutdownException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/***
 * Niagara DataManager FetchThread takes a FetchRequest and service it
 */
class FetchThread implements Runnable {
	private FetchRequest req;
	private Thread thr;
	private int blockCount;
	private MemCache cache;

	FetchThread(FetchRequest req, MemCache dmc) {
		this.req = req;
		this.cache = dmc;
		blockCount = 0;
		thr = new Thread(this, "FetchThread");
		thr.setDaemon(true);
		thr.start();
		return;
	}

	@SuppressWarnings("unchecked")
	public void run() {
		Vector tmpUrl = new Vector();
		Object ret = null;
		blockCount = 0;
		try {
			for (int i = 0; i < req.urls.size(); i++) {
				ret = cache.fetch(req.urls.elementAt(i), this);
				if (ret == null) {
					blockCount++;
					tmpUrl.add(req.urls.elementAt(i));
				} else {
					Element tmpele = ((Document) ret).getDocumentElement();
					if (tmpele == null) {
						throw new PEException("Null document found");
					}
					req.s.put((Node) ret);
				}
			}
			outer: while (tmpUrl.size() != 0) {
				while (!notified)
					_wait();

				for (int i = 0; i < tmpUrl.size(); i++) {
					ret = null;
					Object obj = tmpUrl.elementAt(i);
					ret = MemCache._entryHash.get(obj);
					if (ret != null) {
						Object val = ((MemCacheEntry) ret).val;
						if (val != null) {
							req.s.put((Node) val);
						} else {
							System.err.println("HOW DO YOU GET HERE? "
									+ obj.toString());
						}
						blockCount--;
						tmpUrl.removeElement(obj);
						cache._add(obj, this);
						continue outer;
					}
				}
				notified = false;
			}
		} catch (CacheFetchException cfe) {
			throw new PEException(" !!! *** Fetch Failed");
		} catch (InterruptedException ie) {
			try {
				// REFACTOR
				req.s.putCtrlMsg(ControlFlag.SHUTDOWN, "Interrupted");
			} catch (ShutdownException se) { // ignore
			} catch (InterruptedException ine) {
			} // ignore
		} catch (ShutdownException se) {
			// nothing to do since our one output stream already got shutdown
		}

		// All Fetch Done!
		try {
			req.s.endOfStream();
		} catch (InterruptedException e) {
			// ignore...
		} catch (ShutdownException se) {
			// ignore...
		}
	}

	volatile boolean notified = false;

	public synchronized void _notify() {
		notified = true;
		notifyAll();
	}

	public synchronized void _wait() {
		try {
			wait();
			// should implement time out stuff ?
		} catch (InterruptedException ie) {
			// how to gracefully exit?
		}
	}

}
