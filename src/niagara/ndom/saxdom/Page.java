/**
 * $Id: Page.java,v 1.4 2002/03/28 03:05:33 vpapad Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

/**
 * Page data describe a sequence of SAX events, using two parallel arrays:
 * an array of bytes, describing the type of each event, and an array of
 * (pointers to) String objects.
 *
 * SAXDOM documents contain a sequence of pages. Pages are linked together
 * in a doubly-linked list. Pages are reference counted. Documents that use 
 * them pin and unpin them in memory. 
 */
public class Page {
    private byte[] event_type;
    private String[] event_string;

    private Page previous;
    private Page next;

    private int number;

    private int pin_count;

    private int current_offset;

    public Page(int size, int number) {
        event_type = new byte[size];
        event_string = new String[size];

        previous = next = null;
        pin_count = 0;
        current_offset = 0;
        
        this.number = number;
    }

    /** Prepare page for reuse 
     */
    private synchronized void clear() {
        for (int i = 0; i < event_type.length; i++) {
            event_type[i] = SAXEvent.EMPTY;
            event_string[i] = null;
        }

        if (previous != null)
            previous.setNext(null);

        if (next != null) 
            next.setPrevious(null);

        previous = next = null;
        pin_count = 0;
        current_offset = 0;
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
        } else if (next == null) { // last page of document
            Page page = BufferManager.getFreePage();
            doc.addPage(page);
            page.setPrevious(this);
            setNext(page);

            page.addEvent(doc, type, string);
        } else { // there is a next page
            next.addEvent(doc, type, string);
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

    public void setPrevious(Page previous) {
        this.previous = previous;
    }

    public void setNext(Page next) {
        this.next = next;
    }

    public Page getNext() {
        return next;
    }

    public int getCurrentOffset() {
        return current_offset;
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
