/**
 * $Id: Page.java,v 1.7 2002/04/06 02:15:08 vpapad Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

import niagara.ndom.SAXDOMParser;
import niagara.utils.PEException;

/**
 * Page data describe a sequence of SAX events, using three parallel arrays:
 * an array of bytes, describing the type of each event, an array of
 * (pointers to) String objects, and an array of pointers to each event's
 * next sibling.
 *
 * SAXDOM documents contain a sequence of pages. Pages are linked together
 * in a doubly-linked list. Pages are reference counted. Documents that use 
 * them pin and unpin them in memory. 
 */
public class Page {
    private byte[] event_type;
    private String[] event_string;

    private int[] next_sibling;

    private Page previous;
    private Page next;

    private int number;

    private int pin_count;

    private int current_offset;

    // Parser that is currently adding events to this page
    private SAXDOMParser parser; 

    public Page(int size, int number) {
        event_type = new byte[size];
        event_string = new String[size];
        next_sibling = new int[size];

        this.number = number;

        clear();
    }

    /** Prepare page for reuse 
     */
    private synchronized void clear() {
        for (int i = 0; i < event_type.length; i++) {
            event_type[i] = SAXEvent.EMPTY;
            event_string[i] = null;
            next_sibling[i] = -1;
        }

        if (previous != null)
            previous.setNext(null);

        if (next != null) 
            next.setPrevious(null);

        previous = next = null;
        pin_count = 0;
        current_offset = 0;
        parser = null;
    }

    public void pin() {
        pin_count++;
    }
    
    public void unpin() {
        pin_count--;

        if (pin_count == 0) {
            clear();
            BufferManager.addFreePage(this);
        }
    }

    public void addEvent(DocumentImpl doc, byte type, String string) {
        if (current_offset < event_type.length) {
            event_type[current_offset] = type;
            event_string[current_offset] = string;

            current_offset++;
        } else { // page full
            Page page = BufferManager.getFreePage();
            // XXX vpapad: Page, this is parser. Parser, page!
            page.setParser(parser);
	    parser.setPage(page);

            doc.addPage(page);
            page.setPrevious(this);
            setNext(page);

            page.addEvent(doc, type, string);
	}
    }

    public byte getEventType(int offset) {
        if (pin_count <= 0) { // XXX vpapad
            System.out.println("XXX vpapad: accessing freed page" + this);
        }
        
        return event_type[offset];
    }

    public String getEventString(int offset) {
        if (pin_count <= 0) { // XXX vpapad
            System.out.println("XXX vpapad: accessing freed page");
        }

        return event_string[offset];
    }

    public int getNextSibling(int offset) {
        if (pin_count <= 0) { // XXX vpapad
            System.out.println("XXX vpapad: accessing freed page");
        }
        return next_sibling[offset];
    }

    public void setNextSibling(int offset, int index) {
        next_sibling[offset] = index;
    }

    public void setPrevious(Page previous) {
        this.previous = previous;
    }

    public void setNext(Page next) {
        this.next = next;
    }

    public Page getNext() {
        return next;
    }

    public Page getPrevious() {
        return previous;
    }

    public void setParser(SAXDOMParser parser) {
        this.parser = parser;
    }

    public int getLastIndex() {
        if (current_offset == 0) 
            throw new PEException("getLastIndex called on empty page");

        return number * event_type.length + current_offset - 1;
    }

    public int getLastOffset() {
        if (current_offset == 0) 
            throw new PEException("getLastIndex called on empty page");
        
        return current_offset - 1;
    }

    public int getSize() {
        return event_type.length;
    }

    public boolean isFull() {
        return (current_offset == event_type.length);
    }

    public int getNumber() {
        return number;
    }
    
    public void show() {
        show(0, event_type.length);
    }

    public void show(int start, int end) {
        if (start < 0)
            start = 0;
        if (end >= event_type.length)
            end = event_type.length;

        for (int i = start; i < end; i++) {
            System.err.print("" + i + ": " + event_type[i] + "/");
            if (event_string[i] == null) 
                System.err.println();
            else 
                System.err.println(event_string[i]);
        }
    }
}
