/**
 * $Id: BufferManager.java,v 1.1 2002/03/26 22:07:49 vpapad Exp $
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
import org.w3c.dom.*;

public class BufferManager {
    private static BufferManager bm;
    
    private Page[] pages;

    private Stack free_pages;

    // No public constructor, BufferManager is a singleton
    private BufferManager(int size, int page_size) {
        pages = new Page[size]; 
        free_pages = new Stack();
        
        for (int i = 0; i < size; i++) {
            pages[i] = new Page(page_size);
            free_pages.push(pages[i]);
        }
    }

    public static void createBufferManager(int size, int page_size) {
        System.out.println("SAXDOM buffer manager initialized: " 
                           + size + " pages of " + page_size + " events.");
        
        bm = new BufferManager(size, page_size);
    }

    public static void addFreePage(Page page) {
        bm.free_pages.push(page);
    }

    public static Page getFreePage() {
        if (bm.free_pages.empty())
            // XXX vpapad: Run away! Run away!
            throw new InsufficientMemoryException();
        return (Page) bm.free_pages.pop();
    }
    



    // DOM methods 

    public static String getTagName(int index) {
        return null;
    }

    public static String getName(int index) {
        return null;
    }

    public static String getValue(int index) {
        return null;
    }

    public static Element getOwnerElement(int index) {
        return null;
    }

    public static Node getParentNode(int index) {
        return null;
    }

    public static Attr getAttribute(int index, String name) {
        return null;
    }

    public static boolean hasAttribute(int index, String name) {
        return false;
    }

    public static Attr getAttributeNS(int index, String namespaceURI, 
                                      String localName) {
        return null;
    }

    public static boolean hasAttributeNS(int index, String namespaceURI, 
                                      String localName) {
        return false;
    }

    public static Attr getAttributeNodeNS(int index, String namespaceURI, 
                                      String localName) {
        return null;
    }

    public static Attr getAttributeNode(int index, String name) {
        return null;
    }

    public static NodeList getChildNodes(int index) {
        return null;
    }

    public static boolean hasChildNodes(int index) {
        return false;
    }

    public static Node getFirstChild(int index) {
        return null;
    }

    public static Node getLastChild(int index) {
        return null;
    }

    public static Node getNextSibling(int index) {
        return null;
    }

    public static NamedNodeMap getAttributes(int index) {
        return null;
    }

    public static boolean hasAttributes(int index) {
        return false;
    }

    public static Node getPreviousSibling(int index) {
        return null;
    }
    
    public static String getData(int index) {
        return null;
    }

    public static NodeList getElementsByTagName(int index, String tagname) {
        return null;
    }

    public static NodeList getElementsByTagNameNS(int index, 
                                                  String namespaceURI, 
                                                  String localName) {
        return null;
    }

    public static String getNamespaceURI(int index) {
        return null;
    }

    public static String getPrefix(int index) {
        return null;
    }

    public static String getLocalName(int index) {
        return null;
    }

    public static String getNotationName(int index) {
        return null;
    }

    public static String getPublicId(int index) {
        return null;
    }

    public static String getSystemId(int index) {
        return null;
    }

    public static String getTarget(int index) {
        return null;
    }

}
