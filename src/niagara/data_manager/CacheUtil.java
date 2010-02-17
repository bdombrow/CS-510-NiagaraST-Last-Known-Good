package niagara.data_manager;

import gnu.regexp.RE;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Vector;

import niagara.ndom.DOMFactory;
import niagara.utils.PEException;
import niagara.utils.Tuple;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

/***
 * Niagara DataManager Cache Utility
 */
@SuppressWarnings("unchecked")
public class CacheUtil {
	private static RE initUrlRE() {
		RE ret = null;
		try {
			ret = new RE("http:.*", RE.REG_ICASE);
		} catch (gnu.regexp.REException e) {
			// KT - not sure what to do, throw this for now
			// previous code just ignored this exception @*#$*@
			throw new PEException("gnu regexp exception: " + e.getMessage());
		}
		return ret;
	}

	private static RE initPathRE() {
		RE ret = null;
		try {
			ret = new RE("file:(.*)", RE.REG_ICASE);
		} catch (gnu.regexp.REException e) {
			// KT - not sure what to do, throw this for now
			// previous code just ignored this exception @*#$*@
			throw new PEException("gnu regexp exception: " + e.getMessage());
		}
		return ret;
	}

	private static RE initRE(String re) {
		RE ret = null;
		try {
			ret = new RE(re);
		} catch (gnu.regexp.REException e) {
			// KT - not sure what to do, throw this for now, previous
			// code just ignored this exception @*#$*@
			throw new PEException("gnu regexp exception: " + e.getMessage());
		}
		return ret;
	}

	public static final RE UrlRex = initUrlRE();
	public static final RE PathRex = initPathRE();
	public static final RE TrigTmpRex = initRE("^TRIG_.*");

	public static boolean isUrl(String s) {
		return UrlRex.isMatch(s);
	}

	public static boolean isTrigTmp(String s) {
		int tmp = s.lastIndexOf('/');
		String ts = s.substring(tmp + 1);
		return TrigTmpRex.isMatch(ts);
	}

	public static boolean isOrdinary(String s) {
		if (s.lastIndexOf('&') == -1)
			return true;
		else
			return false;
	}

	public static boolean isAccumFile(String name) {
		if (DataManager.AccumFileDir.containsKey(name)) {
			return true;
		} else {
			return false;
		}
	}

	public static String fileToUrl(String f) {
		int lastSlash = f.lastIndexOf('/');
		String url = f.substring(lastSlash + 1);
		url = url.replace('%', ':');
		url = url.replace('@', '~');
		return url.replace('#', '/');
	}

	public static String urlToFile(String u) {
		String ret = u.replace('/', '#');
		ret = ret.replace('~', '#');
		return ret.replace(':', '#');
	}

	public static String normalizePath(Object k) {
		String f = (String) k;
		if (isUrl(f))
			return f;
		else if (PathRex.isMatch(f))
			f = f.substring(5);
		File tmpF = new File(f);
		String absF = tmpF.getAbsolutePath();
		return absF;
	}

	public static String pathToFile(String f) {
		// file: discarded
		String ret = null;
		if (PathRex.isMatch(f))
			ret = f.substring(5);
		else
			ret = f;
		// convert path to absolute path. Save trouble.
		File tmpF = new File(ret);
		if (!tmpF.exists())
			return null;
		String absF = tmpF.getAbsolutePath();
		ret = absF.replace('/', '#');
		return ret;
	}

	public static void fetchUrl(String url, String dfn) throws IOException {
		FileWriter fw = new FileWriter(dfn);
		URL Url = new URL(url);
		BufferedReader in = new BufferedReader(new InputStreamReader(Url
				.openConnection().getInputStream()));
		String inputline = null;
		while ((inputline = in.readLine()) != null) {
			// // System.err.println(inputline);
			fw.write(inputline + "\n");
		}
		fw.close();
		System.out.println("Fetch url done!");
	}

	public static void fetchLocal(String f, String dfn) throws IOException {
		String fn = f;
		if (PathRex.isMatch(f))
			fn = f.substring(5);
		// System.err.println("Fetching local file : " + fn);
		FileInputStream fin = new FileInputStream(fn);
		FileOutputStream fout = new FileOutputStream(dfn);
		BufferedInputStream bin = new BufferedInputStream(fin);
		BufferedOutputStream bout = new BufferedOutputStream(fout);
		byte[] tmpB = new byte[4096];
		int count = 0;
		while ((count = bin.read(tmpB, 0, 4096)) != -1) {
			// System.err.println("++++++++ WRITTING >>>>> ");
			bout.write(tmpB, 0, count);
		}
		bout.close();
	}

	// If return 0, means timeStamp not supported by URL.
	// or local file not exist.
	public static long getTimeStamp(String s) {
		long ret = 0;
		if (!CacheUtil.isUrl(s)) {
			File tmpF = new File(s);
			if (!tmpF.exists())
				return 0;
			ret = tmpF.lastModified();
		} else {
			try {
				URL Url = new URL(s);
				URLConnection uc = Url.openConnection();
				ret = uc.getLastModified();
			} catch (java.net.MalformedURLException mue) {
				System.err.println("Cannot get network TimeStamp "
						+ mue.getMessage());
				mue.printStackTrace();
				return 0;
			} catch (java.io.IOException ioe) {
				System.err.println("IO exception getting time stamp "
						+ ioe.getMessage());
				ioe.printStackTrace();
				return 0;
			}
		}
		return ret;
	}

	public static void flushXML(String fname, Document doc) {
		// System.err.println(" @@@@@@@ Flushing " + fname);
		Element root = doc.getDocumentElement();
		String tsp = root.getAttribute("TIMESPAN");
		// System.err.println("time span is " + tsp);
		if (tsp != null && !tsp.equals("") && Long.parseLong(tsp) == 0) {
			return;
		}
		String dirty = root.getAttribute("DIRTY");
		if (dirty != null && !dirty.equals(""))
			root.setAttribute("DIRTY", "FALSE");
		// try {
		// PrintWriter pw = new PrintWriter(new FileOutputStream(fname));
		System.err
				.println("Unable to print document - print method not supported by Document");
		// KT doc.print(pw);
		// } catch (IOException ioe) {
		// ioe.printStackTrace();
		// }
	}

	public static void flushVec(String fname, long timespan, Vector vec) {
		Document doc = DOMFactory.newDocument();
		Element root = doc.createElement("ROOT");
		root.setAttribute("TIMESPAN", "" + timespan);
		root.setAttribute("DIRTY", "FALSE");
		// long threshold = System.currentTimeMillis() - timespan;
		for (int i = 0; i < vec.size(); i++) {
			Tuple tmp = (Tuple) vec.elementAt(i);
			root.appendChild(tmp.toEle(doc));
		}
		doc.appendChild(root);
		flushXML(fname, doc);
	}
}
