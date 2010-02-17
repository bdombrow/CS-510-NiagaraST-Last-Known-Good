package niagara.ndom.saxdom;


import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import niagara.utils.PEException;

import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * BufferManager preallocates and maintains an array of pages of SAX events,
 * and manages their translation into DOM events.
 *
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

@SuppressWarnings("unchecked")
public class BufferManager {
	protected static Page[] pages;

	protected static Stack free_pages;

	private static int page_size;

	protected static Map firstPageRegistry;
	protected static Map lastPageRegistry;

	/** The garbage collector will enqueue garbage collected
	 * documents here */
	protected static ReferenceQueue usedDocuments;

	/** We will wait for <code>timeout</code> milliseconds
	 * for free pages to appear before throwing an insufficient
	 * memory exception */
	private static final int timeout = 1000;

	// No public constructor, BufferManager is a singleton
	private BufferManager(int size, int page_size) {
		pages = new Page[size];

		free_pages = new Stack();

		firstPageRegistry = new HashMap(size);
		lastPageRegistry = new HashMap(size);

		usedDocuments = new ReferenceQueue();

		for (int i = 0; i < size; i++) {
			pages[i] = new Page(page_size, i);
			free_pages.push(pages[i]);
		}

		BufferManager.page_size = page_size;

		PageReclaimer pr = new PageReclaimer();
		pr.setDaemon(true);
		pr.start();
	}

	public static void createBufferManager(int size, int page_size) {
		new BufferManager(size, page_size);

		System.out.println(
				"SAXDOM buffer manager initialized: "
				+ size
				+ " pages of "
				+ page_size
				+ " events.");
	}

	public static void addFreePage(Page page) {
		synchronized (free_pages) {
			boolean wasEmpty = free_pages.empty();
			free_pages.push(page);
			// Since we're adding just one page, we only notify one consumer
			if (wasEmpty)
				free_pages.notify();
		}
	}

	public static Reference registerFirstPage(DocumentImpl d, Page page) {
		Reference ref = new WeakReference(d, usedDocuments);
		synchronized (pages) {
			firstPageRegistry.put(ref, page);
			lastPageRegistry.put(ref, page);
		}
		return ref;
	}

	public static void registerLastPage(Reference ref, Page page) {
		synchronized (pages) {
			lastPageRegistry.put(ref, page);
		}
	}

	class PageReclaimer extends Thread {
		public PageReclaimer() {
			super("SAXDOM Page Reclaimer");
		}

		public void run() {
			while (true) {
				try {
					free(usedDocuments.remove());
				} catch (InterruptedException e) {
					continue;
				}
			}
		}
	}

	static void free(Reference r) {
		assert r != null;

		Page first, last;
		synchronized (pages) {
			first= (Page) firstPageRegistry.remove(r);
			last= (Page) lastPageRegistry.remove(r);
		}

		assert first != null && last != null;

		Page page= first;
		do {
			Page next= page.getNext();
			page.unpin();
			if (page == last)
				break;
			page= next;
		} while (page != null);
	}

	public static Page getFreePage() {
		synchronized (free_pages) {
			if (free_pages.empty()) {
				System.gc();
				try {
					free_pages.wait(timeout);
				} catch (InterruptedException e) {
				}
			}

			// Did we time out?
					if (free_pages.empty())
						throw new InsufficientMemoryException();

					return (Page) free_pages.pop();
		}
	}

	// Accessors and utility methods 

	public static Page getPage(int index) {
		return pages[index / page_size];
	}

	public static int getOffset(int index) {
		return index % page_size;
	}

	private static int getIndex(Page page, int offset) {
		return page.getNumber() * page_size + offset;
	}

	private static byte getEventType(int index) {
		return getPage(index).getEventType(getOffset(index));
	}

	private static String getEventString(int index) {
		return getPage(index).getEventString(getOffset(index));
	}

	// DOM methods 

	public static Element getFirstElementChild(DocumentImpl doc, int index) {
		Page page = getPage(index);
		int offset = getOffset(index);

		while (true) {
			if (offset == page_size - 1) {
				page = page.getNext();
				offset = 0;
			} else
				offset++;

			switch (page.getEventType(offset)) {
			case SAXEvent.ATTR_NAME :
			case SAXEvent.ATTR_VALUE :
			case SAXEvent.TEXT :
			case SAXEvent.NAMESPACE_URI :
				continue;
			case SAXEvent.END_ELEMENT :
			case SAXEvent.END_DOCUMENT :
				return null;
			case SAXEvent.START_ELEMENT :
				return (Element) makeNode(doc, getIndex(page, offset));
			default :
				throw new PEException("Unexpected event type");
			}
		}
	}

	public static String getTagName(int index) {
		//XXX (ptucker) we don't store the prefix with elements, only the
		// namespace URI. Since this wants prefix:localname, we
		// for now will output only the local name.
		return getEventString(index);
	}

	public static String getName(int index) {
		//XXX (ptucker) we don't store the prefix with elements, only the
		// namespace URI. Since this wants prefix:localname, we
		// for now will output only the local name.
		return getEventString(index);
	}

	public static String getValue(int index) {
		throw new PEException("Not Implemented Yet!");
	}

	public static Element getOwnerElement(int index) {
		throw new PEException("Not Implemented Yet!");
	}

	public static Node getParentNode(DocumentImpl doc, int index) {
		throw new PEException("Not Implemented Yet!");
	}

	public static String getAttributeValue(int index) {
		Page page = getPage(index);
		int offset = getOffset(index);
		if (offset == page_size - 1) {
			page = page.getNext();
			offset = 0;
		} else
			offset++;

		if (page.getEventType(offset) != SAXEvent.ATTR_VALUE)
			throw new PEException("Could not find attribute value");

		return page.getEventString(offset);
	}

	public static String getAttribute(int elementIndex, String attrName) {
		Page page = getPage(elementIndex);
		int offset = getOffset(elementIndex);

		boolean found = false;

		loop : while (true) {
			if (offset == page_size - 1) {
				page = page.getNext();
				offset = 0;
			} else
				offset++;

			if (found)
				return page.getEventString(offset);

			switch (page.getEventType(offset)) {
			case SAXEvent.ATTR_NAME :
				if (page.getEventString(offset).equals(attrName))
					found = true;
				continue loop;
			case SAXEvent.ATTR_VALUE :
				continue loop;
			default :
				break loop;
			}
		}

		return "";
	}

	public static boolean hasAttribute(int index, String name) {
		throw new PEException("Not Implemented Yet!");
	}

	public static Attr getAttributeNS(
			int index,
			String namespaceURI,
			String localName) {
		throw new PEException("Not Implemented Yet!");
	}

	public static boolean hasAttributeNS(
			int index,
			String namespaceURI,
			String localName) {
		throw new PEException("Not Implemented Yet!");
	}

	public static Attr getAttributeNodeNS(
			int index,
			String namespaceURI,
			String localName) {
		throw new PEException("Not Implemented Yet!");
	}

	/**
	 * Find the attribute of the element with index <code>elementIndex</code>
	 * with name <code>name</code>
	 * @return the corresponding Attr node, or null, if there is none
	 */
	public static Attr getAttributeNode(
			DocumentImpl doc,
			int elementIndex,
			String attrName) {
		Page page = getPage(elementIndex);
		int offset = getOffset(elementIndex);

		loop : while (true) {
			if (offset == page_size - 1) {
				page = page.getNext();
				offset = 0;
			} else
				offset++;

			switch (page.getEventType(offset)) {
			case SAXEvent.ATTR_NAME :
				if (page.getEventString(offset).equals(attrName))
					return (Attr) makeNode(doc, getIndex(page, offset));
				continue loop;
			case SAXEvent.ATTR_VALUE :
				continue loop;
			default :
				break loop;
			}
		}

		return null;
	}

	/**
	 * Find the <code>attrOffset</code>-th attribute of the element
	 * with index <code>elementIndex</code>
	 * @return the corresponding Attr node, or null, if there is none
	 */
	static Node getAttributeByIndex(
			DocumentImpl doc,
			int elementIndex,
			int attrOffset) {
		int count = 0;

		if (attrOffset < 0)
			return null;

		Page page = getPage(elementIndex);
		int offset = getOffset(elementIndex);

		loop : while (true) {
			if (offset == page_size - 1) {
				page = page.getNext();
				offset = 0;
			} else
				offset++;

			switch (page.getEventType(offset)) {
			case SAXEvent.ATTR_NAME :
				if (count < attrOffset) {
					count++;
					continue loop;
				} else
					break loop;
			case SAXEvent.ATTR_VALUE :
				continue loop;
			default :
				break loop;
			}
		}

		if (count == attrOffset)
			return makeNode(doc, getIndex(page, offset));
		else
			return null;
	}

	/**
	 * Find the number of attributes of the element with index 
	 * <code>elementIndex</code>
	 * @return the number of attributes
	 */
	static int getNumberOfAttributes(DocumentImpl doc, int elementIndex) {
		int count = 0;

		Page page = getPage(elementIndex);
		int offset = getOffset(elementIndex);

		loop : while (true) {
			if (offset == page_size - 1) {
				page = page.getNext();
				offset = 0;
			} else
				offset++;

			switch (page.getEventType(offset)) {
			case SAXEvent.ATTR_NAME :
				count++;
				continue loop;
			case SAXEvent.ATTR_VALUE :
				continue loop;
			default :
				break loop;
			}
		}

		return count;
	}

	public static NodeList getChildNodes(DocumentImpl doc, int index) {
		ArrayList al = new ArrayList();
		int child = getFirstChildIndex(index);

		while (child >= 0) {
			al.add(makeNode(doc, child));
			child = getNextSiblingIndex(child);
		}

		return new NodeListImpl(al);
	}

	public static Node makeNode(DocumentImpl doc, int index) {
		byte et = getEventType(index);
		switch (et) {
		case SAXEvent.START_ELEMENT :
			return new ElementImpl(doc, index);
		case SAXEvent.ATTR_NAME :
			return new AttrImpl(doc, index);
		case SAXEvent.ATTR_VALUE :
			// XXX vpapad: this is a fake TextImpl node we create
			// to comply with the DOM spec 
			return new TextImpl(doc, index);
		case SAXEvent.TEXT :
			return new TextImpl(doc, index);
		default :
			throw new PEException(
					"makeNode() can't handle this event type: " + et);
		}

	}

	public static boolean hasChildNodes(int index) {
		return getFirstChildIndex(index) >= 0;
	}

	public static Node fakeAttributeChildren(DocumentImpl doc, int index) {
		// XXX vpapad: according to the DOM spec, attribute have
		// Text nodes as children. That's not the way we implement it
		// so we have to fake that here.
		Page page = getPage(index);
		int offset = getOffset(index);
		if (offset == page_size - 1) {
			page = page.getNext();
			offset = 0;
		} else
			offset++;
		return makeNode(doc, getIndex(page, offset));
	}

	public static NodeList fakeGetChildNodes(DocumentImpl doc, int index) {
		ArrayList al = new ArrayList(1);
		Node n = fakeAttributeChildren(doc, index);
		al.add(n);
		return new NodeListImpl(al);
	}

	public static Node getFirstChild(DocumentImpl doc, int index) {
		int firstChildIndex = getFirstChildIndex(index);
		if (firstChildIndex < 0)
			return null;
		return makeNode(doc, firstChildIndex);
	}

	private static int getFirstChildIndex(int index) {
		Page page = getPage(index);
		int offset = getOffset(index);

		while (true) {
			if (offset == page_size - 1) {
				page = page.getNext();
				offset = 0;
			} else
				offset++;

			switch (page.getEventType(offset)) {
			case SAXEvent.ATTR_NAME :
			case SAXEvent.ATTR_VALUE :
			case SAXEvent.NAMESPACE_URI :
				continue;
			case SAXEvent.END_ELEMENT :
			case SAXEvent.END_DOCUMENT :
				return -1;
			case SAXEvent.START_ELEMENT :
			case SAXEvent.TEXT :
				return getIndex(page, offset);
			default :
				throw new PEException("Unexpected event type");
			}
		}
	}

	public static Node getLastChild(DocumentImpl doc, int index) {
		int lastChildIndex = getLastChildIndex(index);
		if (lastChildIndex < 0)
			return null;
		return makeNode(doc, lastChildIndex);
	}

	private static int getLastChildIndex(int index) {
		Page page = getPage(index);
		int offset = getOffset(index);

		while (true) {
			if (offset == page_size - 1) {
				page = page.getNext();
				offset = 0;
			} else
				offset++;

			switch (page.getEventType(offset)) {
			case SAXEvent.ATTR_NAME :
			case SAXEvent.ATTR_VALUE :
			case SAXEvent.NAMESPACE_URI :
				continue;
			case SAXEvent.END_ELEMENT :
				return -1;
			case SAXEvent.START_ELEMENT :
			case SAXEvent.TEXT :
				int nextSiblingIndex = page.getNextSibling(offset);
				while (nextSiblingIndex >= 0) {
					page = getPage(nextSiblingIndex);
					offset = getOffset(nextSiblingIndex);
					nextSiblingIndex = page.getNextSibling(offset);
				}
				return getIndex(page, offset);
			default :
				throw new PEException("Unexpected event type");
			}
		}
	}

	public static Node getNextSibling(DocumentImpl doc, int index) {
		int sibling = getPage(index).getNextSibling(getOffset(index));
		if (sibling == -1)
			return null;
		else
			return makeNode(doc, sibling);
	}

	public static int getNextSiblingIndex(int index) {
		return getPage(index).getNextSibling(getOffset(index));
	}

	public static NamedNodeMap getAttributes(DocumentImpl doc, int index) {
		return new NamedNodeMapImpl(doc, index);
	}

	public static boolean hasAttributes(int index) {
		Page page = getPage(index);
		int offset = getOffset(index);

		while (true) {
			if (offset == page_size - 1) {
				page = page.getNext();
				offset = 0;
			} else
				offset++;

			switch (page.getEventType(offset)) {
			case SAXEvent.ATTR_NAME :
				return true;
			case SAXEvent.START_ELEMENT :
			case SAXEvent.END_ELEMENT :
			case SAXEvent.TEXT :
				return false;
			default :
				throw new PEException("Unexpected event type");
			}
		}
	}

	public static Node getPreviousSibling(DocumentImpl doc, int index) {
		int previousSiblingIndex = getPreviousSiblingIndex(index);
		if (previousSiblingIndex < 0)
			return null;

		return makeNode(doc, previousSiblingIndex);
	}

	public static int getPreviousSiblingIndex(int index) {
		Page page = getPage(index);
		int offset = getOffset(index);

		if (offset == 0) {
			page = page.getPrevious();
			offset = page_size - 1;
		} else
			offset--;

		switch (page.getEventType(offset)) {
		case SAXEvent.START_DOCUMENT :
		case SAXEvent.START_ELEMENT :
		case SAXEvent.ATTR_NAME :
		case SAXEvent.ATTR_VALUE :
		case SAXEvent.NAMESPACE_URI :
			return -1;
		case SAXEvent.END_ELEMENT :
			return page.getNextSibling(offset);
		case SAXEvent.TEXT :
			return getIndex(page, offset);
		default :
			throw new PEException("Unexpected event type in getPreviousSibling");
		}
	}

	public static String getData(int index) {
		return getEventString(index);
	}

	public static NodeList getElementsByTagName(DocumentImpl doc, int index, String tagname) {
		throw new PEException("Not Implemented Yet!");
	}

	public static NodeList getElementsByTagNameNS(
			int index,
			String namespaceURI,
			String localName) {
		throw new PEException("Not Implemented Yet!");
	}

	public static String getNamespaceURI(int index) {
		Page page = getPage(index);
		int offset = getOffset(index);
		if (offset == page_size - 1) {
			page = page.getNext();
			offset = 0;
		} else
			offset++;

		if (page.getEventType(offset) != SAXEvent.NAMESPACE_URI)
			return null;

		return page.getEventString(offset);
	}

	public static String getPrefix(int index) {
		throw new PEException("Not Implemented Yet!");
	}

	public static String getLocalName(int index) {
		return getEventString(index);
	}

	public static String getNotationName(int index) {
		throw new PEException("Not Implemented Yet!");
	}

	public static String getPublicId(int index) {
		throw new PEException("Not Implemented Yet!");
	}

	public static String getSystemId(int index) {
		throw new PEException("Not Implemented Yet!");
	}

	public static String getTarget(int index) {
		throw new PEException("Not Implemented Yet!");
	}

	/** Set "to" as the next sibling of "from" */
	public static void setNextSibling(int from, int to) {
		pages[from / page_size].setNextSibling(from % page_size, to);
	}

	public static void flatten(int index, StringBuffer sb, boolean prettyprint) {
		switch (getEventType(index)) {
		case SAXEvent.START_ELEMENT :
			flattenElement(index, sb, prettyprint);
			break;
		case SAXEvent.START_DOCUMENT :
			flattenElement(getFirstChildIndex(index), sb, prettyprint);
			break;
		case SAXEvent.ATTR_NAME :
			// It is not clear what an attribute should serialize to.
			// For now, output ' name="value"'
			sb
			.append(" ")
			.append(getEventString(index))
			.append("=")
			.append("\"")
			.append(getAttributeValue(index))
			.append("\"");
			break;
		case SAXEvent.TEXT :
			sb.append(getEventString(index));
			break;
		default : 
			throw new PEException("Unexpected event type: " + getEventType(index));
		}
	}

	public static int numFreePages() {
		return free_pages.size();
	}

	private static void flattenElement(int index, StringBuffer sb, boolean prettyprint) {
		Page page = getPage(index);
		int offset = getOffset(index);

		int depth = -1;
		boolean[] closedStartTag = new boolean[1025];
		boolean prevWasStartEl = false;

		while (true) {
			switch (page.getEventType(offset)) {
			case SAXEvent.START_ELEMENT :
				if (depth >= 0 && !closedStartTag[depth]) {
					sb.append(">");
					closedStartTag[depth] = true;
				}
				if(prevWasStartEl && prettyprint)
					sb.append("\n");
				sb.append("<").append(page.getEventString(offset));
				depth++;
				closedStartTag[depth] = false;
				prevWasStartEl = true; 
				break;
			case SAXEvent.END_ELEMENT :
				if (closedStartTag[depth]) {
					sb.append("</").append(
							page.getEventString(offset)).append(
									">");
				} else
					sb.append("/>");
				closedStartTag[depth] = true;
				depth--;
				if(prettyprint)
					sb.append("\n");
				if (depth == -1)
					return;
				prevWasStartEl = false; 
				break;
			case SAXEvent.TEXT :
				if (depth < 0)
					throw new PEException("Text node outside document");
				if (!closedStartTag[depth]) {
					sb.append(">");
					closedStartTag[depth] = true;
				}
				sb.append(page.getEventString(offset));
				prevWasStartEl = false; 
				break;
			case SAXEvent.ATTR_NAME :
				if (depth < 0 || closedStartTag[depth])
					throw new PEException("Attribute outside an element");
				sb.append(" ").append(page.getEventString(offset));
				prevWasStartEl = false; 
				break;
			case SAXEvent.ATTR_VALUE :
				if (depth < 0 || closedStartTag[depth])
					throw new PEException("Attribute outside an element");
				sb.append("=").append("\"").append(
						page.getEventString(offset)).append(
								"\"");
				prevWasStartEl = false; 
				break;
			default :
				throw new PEException("Unexpected event type: " + page.getEventType(offset));
			}

			if (offset == page_size - 1) {
				page = page.getNext();
				offset = 0;
			} else
				offset++;
		}
	}

	/**
	 * @return Returns the page size.
	 */
	public static int getPageSize() {
		return page_size;
	}
}
