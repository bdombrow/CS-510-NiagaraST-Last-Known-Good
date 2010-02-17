package niagara.ndom.saxdom;

import org.w3c.dom.DOMException;
import org.w3c.dom.Node;

/**
 * 
 * A read-only implementation of the DOM Level 2 interface, using an array of
 * SAX events as the underlying data store.
 * 
 */

public abstract class CharacterDataImpl extends NodeImpl {

	public CharacterDataImpl(DocumentImpl doc, int index) {
		super(doc, index);
	}

	public String getNodeValue() throws DOMException {
		return BufferManager.getData(index);
	}

	public String getData() throws DOMException {
		return BufferManager.getData(index);
	}

	public void setData(String data) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"SAXDOM objects are read-only.");
	}

	public int getLength() {
		return BufferManager.getData(index).length();
	}

	public String substringData(int offset, int count) throws DOMException {
		if (offset < 0 || offset >= getLength() || count < 0) {
			throw new DOMException(DOMException.INDEX_SIZE_ERR,
					"Invalid index in character data access.");
		}

		return BufferManager.getData(index).substring(offset, offset + count);
	}

	public void appendData(String arg) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"SAXDOM objects are read-only.");
	}

	public void insertData(int offset, String arg) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"SAXDOM objects are read-only.");
	}

	public void deleteData(int offset, int count) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"SAXDOM objects are read-only.");
	}

	public void replaceData(int offset, int count, String arg)
			throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"SAXDOM objects are read-only.");
	}

	/**
	 * @see org.w3c.dom.Node#getFirstChild()
	 */
	public Node getFirstChild() {
		return null;
	}

}
