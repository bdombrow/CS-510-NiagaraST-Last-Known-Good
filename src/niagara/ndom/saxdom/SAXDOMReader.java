/* $Id: SAXDOMReader.java,v 1.1 2003/12/24 01:59:51 vpapad Exp $ */
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

    // XXX vpapad: We could make this a container, but creating and
    // casting Integers back and forth is not a nice thing to do for
    // each DOM node in a document. A value of 1024 for the size of
    // open_nodes covers all documents I've seen!
    private int[] open_nodes;
    private DocumentImpl doc;

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
        open_nodes = new int[1024];

        // UTF-8 decoding apparatus
        byte[] sb = new byte[Short.MAX_VALUE];
        ByteBuffer sbuf = ByteBuffer.wrap(sb);
        CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
        CharBuffer cbuf = ByteBuffer.allocate(Short.MAX_VALUE).asCharBuffer();

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
            for (int i = 0; i < nStrings; i++) {
                // Get string length
                short sLength = buffer.getShort();
                if (sLength == 0) {
                    all_strings[i] = null;
                    continue;
                }
                // Get string contents
                buffer.get(sb, 0, sLength);
                sbuf.limit(sLength);
                sbuf.position(0);
                decoder.decode(sbuf, cbuf, true);
                cbuf.flip();
                all_strings[i] = cbuf.toString();
                cbuf.clear();
            }
            buffer.compact();

            // Populate strings
            for (int i = 0; i < current_offset; i++)
                strings[i] = all_strings[string_indexes[i]];

            int off = 0;
            int last = 0;
            while (off < current_offset) {
                if (page == null) {
                    setPage(BufferManager.getFreePage());
                    last = 0;
                }
                if (doc == null)
                    doc = new DocumentImpl(page, page.getFirstIndex());
                int prevLast = last == BufferManager.getPageSize() ? 0 : last;
                last =
                    page.loadEvents(off, current_offset - off, types, strings);
                if (last == BufferManager.getPageSize()) {
                    // Page ended but did the document end too?
                    if (page.endsWithEndDocument()) {
                        sendDocument();
                        setPage(null);
                    } else {
                        Page newPage = BufferManager.getFreePage();
                        page.setNext(newPage);
                        newPage.setPrevious(page);
                        setPage(newPage);
                        doc.addPage(newPage);
                    }
                    off += last - prevLast;
                } else if (last == -1) {
                    // Our buffers ended, page and document did not
                    break;
                } else if (last < BufferManager.getPageSize()) {
                    // Document ended, page did not
                    off += last - prevLast;
                    sendDocument();
                }
            }
        }
    }

    private void sendDocument() throws SAXException {
        try {
            page.fixDocument(open_nodes);
            if (producingOutput)
                outputStream.put(doc);
        } catch (InterruptedException ie) {
            throw new PEException("InterruptedException in SAXDOMReader");
        } catch (ShutdownException se) {
            throw new SAXException("Query shutdown " + se.getMessage());
        }
        doc = null;
    }

    private void setPage(Page page) {
        // Unpin previous page, if any
        if (this.page != null)
            this.page.unpin(1);

        this.page = page;

        // Pin current page, so that it doesn't go away
        // under our feet
        if (page != null)
            page.pin();
    }
}