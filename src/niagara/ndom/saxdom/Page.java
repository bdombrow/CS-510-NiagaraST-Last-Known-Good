/**
 * $Id: Page.java,v 1.15 2003/12/24 01:59:51 vpapad Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

import java.util.Arrays;

import niagara.ndom.SAXDOMParser;
import niagara.utils.PEException;

/**
 * Page data describe a sequence of SAX events, using three parallel arrays: an
 * array of bytes, describing the type of each event, an array of (pointers to)
 * String objects, and an array of pointers to each event's next sibling.
 * 
 * SAXDOM documents contain a sequence of pages. Pages are linked together in a
 * doubly-linked list. Pages are reference counted. Documents that use them pin
 * and unpin them in memory.
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

    /**
	 * The number of events per SAXDOM page. All in-memory SAXDOM pages have
	 * the same size.
	 */
    private final int size;

    public static final int NO_NEXT_SIBLING = -1;

    public Page(int size, int number) {
        this.size = size;
        event_type = new byte[size];
        event_string = new String[size];
        next_sibling = new int[size];

        this.number = number;
        clear();
    }

    /**
	 * Prepare page for reuse
	 */
    private void clear() {
        Arrays.fill(event_string, null);
        Arrays.fill(event_type, SAXEvent.EMPTY);
        Arrays.fill(next_sibling, NO_NEXT_SIBLING);

        if (previous != null)
            previous.setNext(null);

        if (next != null)
            next.setPrevious(null);

        previous = next = null;
        pin_count = current_offset = 0;
        parser = null;
    }

    public void pin() {
        pin_count++;
    }

    public void unpin(int pins) {
        pin_count -= pins;

        if (pin_count == 0) {
            clear();
            BufferManager.addFreePage(this);
        }
    }

    public void addEvent(DocumentImpl doc, byte type, String string) {
        if (current_offset < size) {
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
        assert(
            pin_count > 0 && offset <= current_offset) : "accessing freed page";
        return event_type[offset];
    }

    public String getEventString(int offset) {
        assert(
            pin_count > 0 && offset <= current_offset) : "accessing freed page";
        return event_string[offset];
    }

    public int getNextSibling(int offset) {
        assert pin_count > 0
            && offset <= current_offset : "accessing freed page";
        return next_sibling[offset];
    }

    public void setNextSibling(int offset, int index) {
        next_sibling[offset] = index;
    }

    public void setPrevious(Page previous) {
        this.previous = previous;
    }

    void setNext(Page next) {
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

        return number * size + current_offset - 1;
    }

    /** If we start a document on this page now, what will its index be? */
    public int getFirstIndex() {
        return number * size + current_offset;
    }

    public int getLastOffset() {
        if (current_offset == 0)
            throw new PEException("getLastIndex called on empty page");

        return current_offset - 1;
    }

    public boolean isFull() {
        return (current_offset == size);
    }

    public int getNumber() {
        return number;
    }

    public void show() {
        show(0, size);
    }

    public void show(int start, int end) {
        if (start < 0)
            start = 0;
        if (end >= size)
            end = size;

        for (int i = start; i < end; i++) {
            System.err.print("" + i + ": " + event_type[i] + "/");
            if (event_string[i] != null)
                System.err.print(event_string[i]);
            System.err.println(" -> " + next_sibling[i] % size);
        }
    }

    /**
	 * Bulk load events until we encounter END_DOCUMENT, reach the end of the
	 * page, or the end of the input buffers
	 * 
	 * @return the offset for the END_DOCUMENT if we encountered one, the
	 *              length of the page if we've reached it without finding
	 *              END_DOCUMENT, or -1 if we've reached the end of the input buffers
	 *              before document end.
	 */
    public int loadEvents(
        int offset,
        int length,
        byte[] types,
        String[] strings) {
        int i;
        int limit = Math.min(size, current_offset + length);
        int dstBase = offset - current_offset;
        for (i = current_offset; i < limit; i++) {
            event_type[i] = types[dstBase + i];
            event_string[i] = strings[dstBase + i];
            if (event_type[i] == SAXEvent.END_DOCUMENT)
                break;
        }
        if (i == current_offset + length && i != size) {
            current_offset = i;
            return -1;
        }
        current_offset = i;
        if (current_offset < size)
            current_offset++;
        return current_offset;
    }

    public boolean endsWithEndDocument() {
      return event_type[size - 1] == SAXEvent.END_DOCUMENT;  
    }
    
    public void fixDocument(int[] openNodes) {
        // Fill out the next_sibling pointers for this document
        assert event_type[current_offset - 1] == SAXEvent.END_DOCUMENT;
        fixDocument(current_offset - 1, openNodes, 0);
    }

    protected void fixDocument(int offset, int[] openNodes, int depth) {
        int i;
        int baseIndex = number * size;
        for (i = offset; i >= 0; i--) {
            // End events save the next sibling (if any) in their next_sibling
            // slot
            // and their own index in openNodes[height]
            // Start events grab their next sibling field from their
            // corresponding end
            // event, and save their own index there
            switch (event_type[i]) {
                case SAXEvent.END_DOCUMENT :
                    assert depth == 0;
                    openNodes[depth] = NO_NEXT_SIBLING;
                    break;
                case SAXEvent.END_ELEMENT :
                    next_sibling[i] = openNodes[depth];
                    openNodes[depth] = baseIndex + i;
                    depth++;
                    openNodes[depth] = NO_NEXT_SIBLING;
                    break;
                case SAXEvent.TEXT :
                    next_sibling[i] = openNodes[depth];
                    openNodes[depth] = baseIndex + i;
                    break;
                case SAXEvent.START_ELEMENT :
                    depth--;
                    int end_index = openNodes[depth];
                    next_sibling[i] =
                        BufferManager.getNextSiblingIndex(end_index);
                    openNodes[depth] = baseIndex + i;
                    BufferManager.setNextSibling(end_index, openNodes[depth]);
                    break;
                case SAXEvent.START_DOCUMENT :
                    assert depth == 0;
                    return;
                case SAXEvent.ATTR_NAME :
                case SAXEvent.ATTR_VALUE :
                case SAXEvent.NAMESPACE_URI :
                    break;
                default :
                    throw new PEException("Unknown SAX event");
            }
        }
        if (i == -1)
            getPrevious().fixDocument(size - 1, openNodes, depth);
    }

    /**
	 * What is the size, in number of events, of the document that starts at
	 * <code>offset</code>?
	 */
    int documentSize(int offset) {
        int i = offset;
        while (i < size && event_type[i] != SAXEvent.END_DOCUMENT)
            i++;
        if (i == size)
            return i - offset + next.documentSize(0);
        else // We must have found END_DOCUMENT
            return i - offset + 1;
    }

    /**
	 * Copy <code>howMany</code> event types and strings from offset <code>from</code>
	 * into arrays <code>types</code> and <code>strings</code>, starting
	 * at destination offset <code>dest_from</code>.
	 */
    void copyEvents(
        int from,
        int howMany,
        int dest_from,
        byte[] types,
        String[] strings) {
        int to = from + howMany - 1;
        if (to >= size)
            to = size - 1;

        for (int i = from; i <= to; i++) {
            assert event_type[i] != SAXEvent.EMPTY;
            types[dest_from - from + i] = event_type[i];
            strings[dest_from - from + i] = event_string[i];
        }

        int copied = to - from + 1;
        if (howMany - copied > 0)
            next.copyEvents(
                0,
                howMany - copied,
                dest_from + copied,
                types,
                strings);
    }
}
