package niagara.data_manager;

import java.util.Vector;

/**
 * The DTDInfo class is used to associate stats and urls sets with a DTD. This
 * is just a skelton implementation for now to define the interface. This should
 * be much easier to use than parsing the XML doc in the Exec. Engine.
 */

@SuppressWarnings("unchecked")
public class DTDInfo {

	// The dtdid: url where dtd was found
	//
	private String dtdid;

	// The ulr list associated with this dtd
	//
	private Vector urls;

	// The stats for this dtd
	//
	private DTDStats stats;

	/**
	 * DTDInfo constructor. Initialize the dtdid, url vector, and stats.
	 * 
	 */
	DTDInfo(String dtdid) {
		this.dtdid = dtdid;
		urls = new Vector();
		stats = null;
	}

	DTDInfo() {
		this.dtdid = null;
		urls = new Vector();
		stats = null;
	}

	public DTDStats getDTDStats() {
		return stats;
	}

	public Vector getURLs() {
		return urls;
	}

	public String getDTDId() {
		return dtdid;
	}

	public void addURL(String url) {
		urls.addElement(url);
	}

	public void addStats(DTDStats stats) {
		this.stats = stats;
	}
}

/**
 * This class must be defined at some point, just a placeholder for now
 * 
 * 
 */
class DTDStats {
}
