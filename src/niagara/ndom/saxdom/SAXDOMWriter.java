/* $Id: SAXDOMWriter.java,v 1.2 2004/02/11 01:11:32 vpapad Exp $ */
package niagara.ndom.saxdom;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.HashMap;

import java.nio.ByteBuffer;

import niagara.utils.ShutdownException;

/**
 * Writing SAXDOM documents in a pre-parsed, binary format.
 */

public class SAXDOMWriter extends SAXDOMIO {
    private HashMap string2index;
    private ArrayList index2string;
    private CharsetEncoder encoder;
    private ByteBuffer sbuf;
    private String[] strings;
    private short[] string_lengths;

    public SAXDOMWriter(FileOutputStream fos) {
        super();
        strings= new String[MAX_EVENTS];
        string_lengths= new short[MAX_EVENTS];
        string2index= new HashMap();
        index2string= new ArrayList();
        channel= fos.getChannel();
        encoder= Charset.forName("UTF-8").newEncoder();
        sbuf= ByteBuffer.allocateDirect(Short.MAX_VALUE);
    }

    /** Write a document out */
    public void writeDocument(DocumentImpl doc) throws ShutdownException {
        int start= doc.getIndex();
        int offset= BufferManager.getOffset(start);
        Page page= BufferManager.getPage(start);
        int size= page.documentSize(offset);

        while (size > 0) {
            // Fill our buffer with events
            short nRead= (short) Math.min(size, MAX_EVENTS - current_offset);

            page.copyEvents(offset, nRead, current_offset, types, strings);
            size -= nRead;
            offset += nRead;
            int pageSize= BufferManager.getPageSize();
            if (size > 0 && offset >= pageSize)
                do {
                    offset -= pageSize;
                    page= page.getNext();
                } while (offset >= pageSize);
            current_offset += nRead;
            assert current_offset == MAX_EVENTS || size == 0;
            // Write out as many full pages as possible
            writeOutPages(false);
        }
    }

    public void flush() throws ShutdownException {
        if (current_offset != 0)
            writeOutPages(true);
        done();
    }

    /**
     * Write out as many pages as possible.
     * 
     * @param force Should we write non-full pages?
     * @throws ShutdownException
     */
    private void writeOutPages(boolean force) throws ShutdownException {
        int sz= 0;
        int size= EMPTY_SIZE + 3 * current_offset;
        int currIdx= 0;
        short totalStringsLength= 0;
        string2index.clear();
        boolean stopped= false;
        for (int i= 0; i < current_offset; i++) {
            String s= strings[i];
            if (!string2index.containsKey(s)) {
                string2index.put(s, null);
                encoder.reset();
                size += 2;
                if (s != null) {
                    try {
                        int len= encoder.encode(CharBuffer.wrap(s)).limit();
                        if (len > Short.MAX_VALUE)
                            throw new ShutdownException(
                                "Cannot store strings larger than "
                                    + Short.MAX_VALUE);
                        string_lengths[currIdx++]= (short) s.length();
                        size += len;
                        totalStringsLength += len;
                    } catch (CharacterCodingException cce) {
                        throw new ShutdownException(
                            "Cannot encode string " + s + " in UTF-8");
                    }
                } else
                    string_lengths[currIdx++]= 0;
                if (size >= PAGE_SIZE) {
                    sz= i;
                    stopped= true;
                    break;
                }
            }
        }
        if (!stopped)
            sz= current_offset;

        if (!force && sz == current_offset && current_offset < MAX_EVENTS)
            return;

        string2index.clear();
        index2string.clear();

        // Index of the next string we'll add to the buffer
        short current_index= 0;
        //Number of events we can write out
        short writableEvents;
        for (writableEvents= 0; writableEvents < sz; writableEvents++) {
            // An event costs at least 3 bytes
            String s= strings[writableEvents];
            boolean newString= !string2index.containsKey(s);
            if (newString) {
                string2index.put(s, new Short(current_index));
                index2string.add(s);
                current_index++;
            }
        }

        assert writableEvents > 0;

        // Write the page at last
        // Number of events
        buffer.putShort((short) (writableEvents));
        // The event type array
        buffer.put(types, 0, writableEvents);
        // The event string array
        for (int i= 0; i < writableEvents; i++)
            buffer.putShort(
                ((Short) string2index.get(strings[i])).shortValue());

        // The number of strings
        buffer.putShort((short) (current_index));
        buffer.putShort(totalStringsLength);
        // The string contents
        for (int i= 0; i < current_index; i++) {
            String s= (String) index2string.get(i);
            if (s == null || s.length() == 0)
                continue;
            encoder.encode(CharBuffer.wrap(s), sbuf, true);
            encoder.reset();
            sbuf.flip();
            buffer.put(sbuf);
            sbuf.clear();
        }
        // The string lengths
        buffer.asShortBuffer().put(string_lengths, 0, current_index);
        buffer.position(buffer.position() + 2 * current_index);

        try {
            buffer.flip();
            channel.write(buffer);
            buffer.clear();
        } catch (IOException ioe) {
            throw new ShutdownException(
                "IO Exception while storing SAXDOM data: " + ioe.getMessage());
        }

        for (int i= writableEvents; i < current_offset; i++) {
            types[i - writableEvents]= types[i];
            strings[i - writableEvents]= strings[i];
        }
        current_offset -= writableEvents;

        if (current_offset > 0)
            writeOutPages(force);
    }
}