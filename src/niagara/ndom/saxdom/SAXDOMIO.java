/* $Id: SAXDOMIO.java,v 1.2 2004/02/11 01:11:32 vpapad Exp $ */
package niagara.ndom.saxdom;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import niagara.utils.ShutdownException;

/**
 * Reading and writing SAXDOM documents in a binary format.
 */

abstract public class SAXDOMIO {
    protected byte[] types;

    /** The offset of the next event that can be stored in our arrays */
    protected short current_offset;

    /** The on-disk size of the page in bytes */
    protected int PAGE_SIZE = 64 * 1024;
    /** The size of an empty page */
    // We must store at least two shorts per page
    // (number of events, and number of unique strings)
    protected int EMPTY_SIZE = 2 * 2;
    /** The maximum number of events we will put in a page */
    protected short MAX_EVENTS = 4096;

    protected FileChannel channel;
    protected ByteBuffer buffer;
    protected SAXDOMIO() {
        types = new byte[MAX_EVENTS];
        buffer = ByteBuffer.allocateDirect(PAGE_SIZE);
    }

    public void done() throws ShutdownException {
        try {
            channel.close();
        } catch (IOException e) {
            throw new ShutdownException(
                "IO Error while closing file: " + e.getMessage());
        }
    }
}