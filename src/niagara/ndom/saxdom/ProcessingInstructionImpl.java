/**
 * $Id: ProcessingInstructionImpl.java,v 1.1 2002/03/26 22:07:50 vpapad Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

import org.w3c.dom.*;

public class ProcessingInstructionImpl extends NodeImpl
    implements ProcessingInstruction {
    
    public ProcessingInstructionImpl(Document doc, int index) {
        super(doc, index);
    }

    public short getNodeType() {
        return Node.PROCESSING_INSTRUCTION_NODE;
    }

    public String getNodeName() {
        // XXX vpapad: What are we supposed to return here?
        return null;
    }

    public String getTarget() {
        return BufferManager.getTarget(index);
    }

    public String getData() {
        return BufferManager.getData(index);
    }

    public void setData(String data) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "SAXDOM objects are read-only.");
    }

}
