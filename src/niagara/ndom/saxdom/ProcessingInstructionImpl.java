package niagara.ndom.saxdom;

import org.w3c.dom.DOMException;
import org.w3c.dom.Node;
import org.w3c.dom.ProcessingInstruction;

/**
 * 
 * A read-only implementation of the DOM Level 2 interface, using an array of
 * SAX events as the underlying data store.
 * 
 */

public class ProcessingInstructionImpl extends NodeImpl implements
		ProcessingInstruction {

	public ProcessingInstructionImpl(DocumentImpl doc, int index) {
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
