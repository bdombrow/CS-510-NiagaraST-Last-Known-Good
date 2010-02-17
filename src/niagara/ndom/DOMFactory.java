package niagara.ndom;

import java.util.Hashtable;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * <code>DOMFactory</code> allows us to choose a DOM implementation at runtime.
 */
@SuppressWarnings("unchecked")
public class DOMFactory {

	/**
	 * <code>impl</code> is the default DOM implementation used for new parsers
	 * and documents.
	 * 
	 */
	private static DOMImplementation impl;

	private static Hashtable name2impl;
	static {
		name2impl = new Hashtable();
		name2impl.put("xml4j", new XML4J());
		name2impl.put("xerces", new XercesJ());
		name2impl.put("xerces2", new XercesJ2());
		name2impl.put("saxdom", new SAXDOM());

		// new xerces parser is the default
		impl = (DOMImplementation) name2impl.get("xerces2");
	}

	/**
	 * <code>setImpl</code> sets the default DOM implementation.
	 * 
	 * @param name
	 *            a <code>String</code> value
	 */
	public static void setImpl(String name) {
		if (name2impl.get(name) == null)
			System.err.println("DOM Implementation " + name
					+ " is not registered!");
		else
			impl = (DOMImplementation) name2impl.get(name);
	}

	/**
	 * <code>newDocument</code> creates a new Document with the default
	 * implementation.
	 * 
	 * @return a <code>Document</code> value
	 */
	public static Document newDocument() {
		return impl.newDocument();
	}

	/**
	 * <code>newDocument</code> creates a new Document with the requestd
	 * implementation.
	 * 
	 * @return a <code>Document</code> value
	 */
	public static Document newDocument(String implementation) {
		return ((DOMImplementation) name2impl.get(implementation))
				.newDocument();
	}

	/**
	 * <code>newParser</code> creates a new Parser with the default
	 * implementation.
	 * 
	 * @return a <code>DOMParser</code> value
	 */
	public static DOMParser newParser() {
		return impl.newParser();
	}

	/**
	 * @return a new validating parser with the default implementation, if
	 *         that's possible, otherwise return a regular parser.
	 * 
	 * @return a <code>DOMParser</code> value
	 */
	public static DOMParser newValidatingParser() {
		return impl.newValidatingParser();
	}

	/**
	 * <code>newParser</code> creates a new Parser for the requested
	 * implementation.
	 * 
	 * @return a <code>DOMParser</code> value
	 */
	public static DOMParser newParser(String implementation) {
		return ((DOMImplementation) name2impl.get(implementation)).newParser();
	}

	/**
	 * Returns a version of a node that can be inserted in a (possibly different
	 * than its current) document (by deep cloning it if needed)
	 * 
	 * This method is here just for the benefit of the old TXDOM parser -- it is
	 * part of DOM Level 2
	 * 
	 * @param d
	 *            a <code>Document</code>
	 * @param n
	 *            a <code>Node</code>
	 * @return the (possibly cloned) <code>Node</code>
	 */
	public static Node importNode(Document d, Node n) {
		if (n.getOwnerDocument() == d)
			return n;
		else
			return impl.importNode(d, n);
	}

}
