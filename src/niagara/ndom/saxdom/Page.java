/**
 * $Id: Page.java,v 1.1 2002/03/26 22:07:50 vpapad Exp $
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

    private int pin_count;

    private int current_event;

    public Page(int size) {
        event_type = new byte[size];
        event_string = new String[size];

        previous = next = null;
        pin_count = 0;
        current_event = 0;
    }

    /** Prepare page for reuse 
     */
    private void clear() {
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
        current_event = 0;
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
        Page page;

        if (current_event < event_type.length)
            page = this;
        else {
            page = BufferManager.getFreePage();
            page.setPrevious(this);
            setNext(page);

            page.setPrevious(this);
            setNext(page);
            page.addEvent(doc, type, string);
            return;
        }

        if (!doc.includesPage(page))
            doc.addPage(page);
        
        current_event++;
    }

    public byte getEventType(int index) {
        return event_type[index];
    }

    public String getEventString(int index) {
        return event_string[index];
    }

    public void setPrevious(Page previous) {
        this.previous = previous;
    }

    public void setNext(Page next) {
        this.next = next;
    }
}
