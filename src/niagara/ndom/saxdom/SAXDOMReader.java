/* $Id: SAXDOMReader.java,v 1.3 2004/02/11 01:11:32 vpapad Exp $ */
package niagara.ndom.saxdom;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import niagara.utils.PEException;
import niagara.utils.ShutdownException;
import niagara.utils.SinkTupleStream;

import org.xml.sax.SAXException;

/**
 * Read SAXDOM documents in a binary format.
 */
public class SAXDOMReader extends SAXDOMIO {
    private Page page;
    private SinkTupleStream outputStream;
    
    private static final boolean producingOutput = true;

    public SAXDOMReader(FileInputStream fis) {
        super();
        channel = fis.getChannel();
        if (BufferManager.getPageSize() > MAX_EVENTS)
            throw new PEException("In-memory page size should not be larger than on-disk page size");
    }

    /** Read the next document in */
    public void readDocuments(SinkTupleStream outputStream)
        throws SAXException, IOException {
        this.outputStream = outputStream;

        short[] string_indexes = new short[MAX_EVENTS];
        String[] all_strings = new String[MAX_EVENTS];
        int[] open_nodes = new int[1024];

        // UTF-8 decoding apparatus
        ByteBuffer sbuf = buffer.duplicate();
        CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
        CharBuffer cbuf = CharBuffer.allocate(PAGE_SIZE/2);
        
        int pageSize = BufferManager.getPageSize();
        while (true) {
            // Read a page in
            if (channel.read(buffer) < 0 && buffer.position() == 0) {
                // We're done
                setPage(null);
                return;
            }
            buffer.flip();
            // Get number of events
            current_offset = buffer.getShort();
            // Get event types
            buffer.get(types, 0, current_offset);
            // Get event strings
            buffer.asShortBuffer().get(string_indexes, 0, current_offset);
            buffer.position(buffer.position() + 2 * current_offset);
            // Get number of strings
            short nStrings = buffer.getShort();
            // Get total string size
            short totalStringsLength = buffer.getShort();
            // Get string contents
            int pos = buffer.position();
            sbuf.limit(pos + totalStringsLength).position(pos);
            decoder.decode(sbuf, cbuf, true);
            cbuf.flip();
            String bigString = cbuf.toString();
            buffer.position(pos + totalStringsLength);
            cbuf.clear();
            int start = 0;
            for (int i = 0; i < nStrings; i++) {
                // Get string length
                short sLength = buffer.getShort();
                if (sLength == 0) {
                    all_strings[i] = null;
                    continue;
                }
                int end = start + sLength;
                all_strings[i] = bigString.substring(start, end);
                start = end;
            }
            buffer.compact();

            int off = 0;
            int last = 0;
            while (off < current_offset) {
                if (page == null) {
                    setPage(BufferManager.getFreePage());
                    last = 0;
                }
                int prevLast = last == pageSize ? 0 : last;
                last =
                    page.loadEvents(
                        off,
                        current_offset - off,
                        types,
						string_indexes,
                        all_strings,
						open_nodes,
						this);
                if (last == -1)
                    break;
                else 
                    off += last - prevLast;
            }
        }
    }

    void sendDocument(DocumentImpl doc) throws SAXException {
        try {
            if (producingOutput)
                outputStream.put(doc);
        } catch (InterruptedException ie) {
            throw new PEException("InterruptedException in SAXDOMReader");
        } catch (ShutdownException se) {
            throw new SAXException("Query shutdown " + se.getMessage());
        }
    }

    void setPage(Page page) {
        // Unpin previous page, if any
        if (this.page != null)
            this.page.unpin();

        this.page = page;

        // Pin current page, so that it doesn't go away
        // under our feet
        if (page != null)
            page.pin();
    }
}
