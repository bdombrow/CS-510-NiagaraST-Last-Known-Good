/**
 * $Id: BufferManager.java,v 1.4 2002/03/28 03:05:33 vpapad Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

/**
 *
 * BufferManager preallocates and maintains an array of pages of SAX events,
 * and manages their translation into DOM events.
 *
 */

import java.util.Stack;
import java.util.Vector;

import org.w3c.dom.*;

import niagara.utils.PEException;

public class BufferManager {
    private static Page[] pages;

    private static Stack free_pages;

    private static int page_size;

    // No public constructor, BufferManager is a singleton
    private BufferManager(int size, int page_size) {
        pages = new Page[size]; 
        free_pages = new Stack();
        
        for (int i = 0; i < size; i++) {
            pages[i] = new Page(page_size, i);
            free_pages.push(pages[i]);
        }

        this.page_size = page_size;
    }

    public static void createBufferManager(int size, int page_size) {
        System.out.println("SAXDOM buffer manager initialized: " 
                           + size + " pages of " + page_size + " events.");
        
        new BufferManager(size, page_size);
    }

    public static void addFreePage(Page page) {
        free_pages.push(page);
    }

    public static Page getFreePage() {
        if (free_pages.empty())
            // XXX vpapad: Run away! Run away!
            throw new InsufficientMemoryException();
        return (Page) free_pages.pop();
    }
    

    public static Page getPage(int index) {
        return pages[index / page_size];
    }

    public static int getOffset(int index) {
        return index % page_size;
    }

    protected static int getIndex(Page page, int offset) {
        return page.getNumber()*page_size + offset;
    }

    protected static byte getEventType(int index) {
        return getPage(index).getEventType(getOffset(index));
    }
    protected static String getEventString(int index) {
        return getPage(index).getEventString(getOffset(index));
    }

    public static Element getFirstElementChild(DocumentImpl doc, int index) {
        EventIterator ei = new EventIterator(index);
        do { 
            byte event_type = ei.nextEventType();
            if (event_type == SAXEvent.START_ELEMENT) {
                return new ElementImpl(doc, ei.getIndex());
            }
            if (event_type == SAXEvent.END_ELEMENT || 
                event_type == SAXEvent.END_DOCUMENT) {
                System.out.println("XXX vpapad getFEC returns null " +
                    " for doc:" + doc + " at: " + index);
                return null;
            }
        } while (true);
    }

    // DOM methods 

    public static String getTagName(int index) {
        return getEventString(index);
    }

    public static String getName(int index) {
        return getEventString(index);
    }

    public static String getValue(int index) {
        throw new PEException("Not Implemented Yet!");
    }

    public static Element getOwnerElement(int index) {
        throw new PEException("Not Implemented Yet!");
    }

    public static Node getParentNode(int index) {
        throw new PEException("Not Implemented Yet!");
    }

    public static Attr getAttribute(int index, String name) {
        throw new PEException("Not Implemented Yet!");
    }

    public static boolean hasAttribute(int index, String name) {
        throw new PEException("Not Implemented Yet!");
    }

    public static Attr getAttributeNS(int index, String namespaceURI, 
                                      String localName) {
        throw new PEException("Not Implemented Yet!");
    }

    public static boolean hasAttributeNS(int index, String namespaceURI, 
                                      String localName) {
        throw new PEException("Not Implemented Yet!");
    }

    public static Attr getAttributeNodeNS(int index, String namespaceURI, 
                                      String localName) {
        throw new PEException("Not Implemented Yet!");
    }

    public static Attr getAttributeNode(int index, String name) {
        throw new PEException("Not Implemented Yet!");
    }

    public static NodeList getChildNodes(DocumentImpl doc, int index) {
        // Only documents and elements have children
        byte et = getEventType(index);
        if (et != SAXEvent.START_DOCUMENT && et != SAXEvent.START_ELEMENT) {
            // XXX vpapad: Should this be null, or an empty NodeList ???
            return null;
        }

        EventIterator ei = new EventIterator(index);
        ei.forwardChild();
        if (ei.getIndex() == index) 
            return new NodeListImpl(new Node[] {});

        Vector children = new Vector();
        int lastIndex = ei.getIndex();
        do {
            children.add(makeNode(doc, lastIndex));
            ei.forwardSibling();
            if (ei.getIndex() == lastIndex)
                break;
            else
                lastIndex = ei.getIndex();
        } while (true);
        
        Node[] nodes = new Node[children.size()];
        for (int i = 0; i < nodes.length; i++) 
            nodes[i] = (Node) children.get(i);

        return new NodeListImpl(nodes);
    }

    public static Node makeNode(DocumentImpl doc, int index) {
        byte et = getEventType(index);
        switch (et) {
        case SAXEvent.START_ELEMENT:
            return new ElementImpl(doc, index);
        case SAXEvent.ATTR_NAME:
            return new AttrImpl(doc, index);
        case SAXEvent.TEXT:
            return new TextImpl(doc, index);
        default:
            throw new PEException("makeNode() can't handle this event.");
        }
        
    }

    public static boolean hasChildNodes(int index) {
        throw new PEException("Not Implemented Yet!");
    }

    public static Node getFirstChild(DocumentImpl doc, int index) {
        EventIterator ei = new EventIterator(index);
        ei.forwardChild();
        if (ei.getIndex() == index) 
            return null;
        else return makeNode(doc, ei.getIndex());

    }

    public static Node getLastChild(int index) {
        throw new PEException("Not Implemented Yet!");
    }

    public static Node getNextSibling(DocumentImpl doc, int index) {
        EventIterator ei = new EventIterator(index);
        ei.forwardSibling();
        int nextIndex = ei.getIndex();
        if (nextIndex == index)
            return null;
        else
            return makeNode(doc, nextIndex);
    }

    public static NamedNodeMap getAttributes(DocumentImpl doc, int index) {
        NamedNodeMap nnm = new NamedNodeMapImpl(doc);
        
        EventIterator ei = new EventIterator(index);
        do  {
            ei.forwardEvent();
            byte et = ei.getEventType();
            if (et == SAXEvent.ATTR_NAME) 
                nnm.setNamedItem(new AttrImpl(doc, ei.getIndex()));
            else if (et == SAXEvent.ATTR_VALUE) 
                continue;
            else break;
        } while (true);

        return nnm;
    }

    public static boolean hasAttributes(int index) {
        throw new PEException("Not Implemented Yet!");
    }

    public static Node getPreviousSibling(int index) {
        throw new PEException("Not Implemented Yet!");
    }
    
    public static String getData(int index) {
        return getEventString(index);
    }

    public static NodeList getElementsByTagName(int index, String tagname) {
        throw new PEException("Not Implemented Yet!");
    }

    public static NodeList getElementsByTagNameNS(int index, 
                                                  String namespaceURI, 
                                                  String localName) {
        throw new PEException("Not Implemented Yet!");
    }

    public static String getNamespaceURI(int index) {
        throw new PEException("Not Implemented Yet!");
    }

    public static String getPrefix(int index) {
        throw new PEException("Not Implemented Yet!");
    }

    public static String getLocalName(int index) {
        throw new PEException("Not Implemented Yet!");
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

}

/** EventIterator is an iterator interface for getting consecutive
 *  events from a SAXDOM document
 */
class EventIterator {
    private Page page;
    private int offset;

    public EventIterator(int index) {
        page = BufferManager.getPage(index);
        offset = BufferManager.getOffset(index);
    }
    
    /**
     * Move to the next event
     */
    public void forwardEvent() {
        byte type = page.getEventType(offset);
        if (type == SAXEvent.END_DOCUMENT) {
            throw new PEException("Attempt to move past end of document.");
        }
        
        offset++;
        if (offset == page.getSize()) {
            // Move to the next page
            page = page.getNext();
            offset = 0;
        }
    }

    /**
     * Move to the first child of this event (do nothing if there is none)
     */
    public void forwardChild() {
        // Save current location, in case our search is unsuccessful
        Page orgPage = page; 
        int orgOffset = offset;

        byte et;

        do {
        // Skip over attribute events
            forwardEvent();
            et = getEventType();
            if (et != SAXEvent.ATTR_NAME && et != SAXEvent.ATTR_VALUE)
                break;
        } while (true);
        
        et = getEventType();
        if (et == SAXEvent.END_ELEMENT || et == SAXEvent.END_DOCUMENT) {
            // Search unsuccessful
            page = orgPage;
            offset = orgOffset;
        }
    }

    /**
     * Skip forward to the next event representing a sibling
     * of the current event (do nothing if there is no such event)
     */
    public void forwardSibling() {
        // Save current location, in case our search is unsuccessful
        Page orgPage = page; 
        int orgOffset = offset;

        byte et = getEventType();
        if (et == SAXEvent.TEXT) {
            forwardEvent();

            if (getEventType() == SAXEvent.END_ELEMENT) {
                // No next sibling
                page = orgPage;
                offset = orgOffset;
            }
            return;
        } else if (et == SAXEvent.START_ELEMENT) {
            int depth = 0;
            
            do {
                forwardEvent();
                et = getEventType();
                
                if (depth == -1) { // We're in the enclosing level
                    if (et == SAXEvent.END_ELEMENT || 
                        et == SAXEvent.END_DOCUMENT) {
                        // Search was unsuccessful - go back 
                        page = orgPage;
                        offset = orgOffset;
                        return;
                } else  // Search was successful
                    return;
                } else {
                    if (et == SAXEvent.START_ELEMENT)
                        depth++;
                    else if (et == SAXEvent.END_ELEMENT)
                        depth--;
                }
            } while(true);
        } else 
            throw new PEException("forwardSimpling can't handle this event.");
    }

    public byte getEventType() {
        return page.getEventType(offset);
    }

    public byte nextEventType() {
        forwardEvent();
        return getEventType();
    }

    public String getEventString() {
        return page.getEventString(offset);
    }

    public int getIndex() {
        return BufferManager.getIndex(page, offset);
    }
}
