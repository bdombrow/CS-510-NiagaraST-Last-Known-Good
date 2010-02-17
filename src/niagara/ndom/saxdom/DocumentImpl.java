package niagara.ndom.saxdom;

import java.lang.ref.Reference;

import niagara.utils.PEException;

import org.w3c.dom.Attr;
import org.w3c.dom.CDATASection;
import org.w3c.dom.Comment;
import org.w3c.dom.DOMConfiguration;
import org.w3c.dom.DOMException;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.DocumentType;
import org.w3c.dom.Element;
import org.w3c.dom.EntityReference;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.ProcessingInstruction;
import org.w3c.dom.Text;

/**
 * 
 * A read-only implementation of the DOM Level 2 interface, using an array of
 * SAX events as the underlying data store.
 * 
 */
@SuppressWarnings("unchecked")
public class DocumentImpl extends NodeImpl implements Document {

	/**
	 * A week reference to this document, used by BufferManager to keep track of
	 * our pages, so that it can free them up when the document is garbage
	 * collected.
	 */
	private Reference ref;

	private int pins;

	public DocumentImpl(Page firstPage, int index) {
		super(null, index);

		doc = this;

		firstPage.pin();
		ref = BufferManager.registerFirstPage(this, firstPage);
		pins = 1;
	}

	public short getNodeType() {
		return Node.DOCUMENT_NODE;
	}

	public String getNodeName() {
		return "#document";
	}

	public void addPage(Page page) {
		page.pin();
		BufferManager.registerLastPage(ref, page);
	}

	public void pin() {
		pins++;
	}

	public void unpin() {
		pins--;
		if (pins == 0)
			BufferManager.free(ref);
	}

	// We don't support DTDs yet.
	public DocumentType getDoctype() {
		return null;
	}

	public DOMImplementation getImplementation() {
		return DOMImplementationImpl.getImplementation();
	}

	public Element getDocumentElement() {
		return BufferManager.getFirstElementChild(this, index);
	}

	// The specification does not allow a NO_MODIFICATION_ALLOWED exception
	// here, but we throw one anyway.
	public Element createElement(String tagName) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"SAXDOM objects are read-only.");
	}

	public DocumentFragment createDocumentFragment() {
		return new DocumentFragmentImpl(this);
	}

	// The specification does not allow us to throw any exception here,
	// so we just return null. Oh well.
	public Text createTextNode(String data) {
		return null;
	}

	// The specification does not allow us to throw any exception here,
	// so we just return null. Oh well.
	public Comment createComment(String data) {
		return null;
	}

	// The specification does not allow a NO_MODIFICATION_ALLOWED exception
	// here, but we throw one anyway.
	public CDATASection createCDATASection(String data) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"SAXDOM objects are read-only.");
	}

	// The specification does not allow a NO_MODIFICATION_ALLOWED exception
	// here, but we throw one anyway.
	public ProcessingInstruction createProcessingInstruction(String target,
			String data) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"SAXDOM objects are read-only.");
	}

	// The specification does not allow a NO_MODIFICATION_ALLOWED exception
	// here, but we throw one anyway.
	public Attr createAttribute(String name) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"SAXDOM objects are read-only.");
	}

	// The specification does not allow a NO_MODIFICATION_ALLOWED exception
	// here, but we throw one anyway.
	public EntityReference createEntityReference(String name)
			throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"SAXDOM objects are read-only.");
	}

	// At last, something we CAN do!
	public NodeList getElementsByTagName(String tagname) {
		return BufferManager.getElementsByTagName(this, index, tagname);
	}

	// The specification does not allow a NO_MODIFICATION_ALLOWED exception
	// here, but we throw one anyway.
	public Node importNode(Node importedNode, boolean deep) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"SAXDOM objects are read-only.");
	}

	// The specification does not allow a NO_MODIFICATION_ALLOWED exception
	// here, but we throw one anyway.
	public Element createElementNS(String namespaceURI, String qualifiedName)
			throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"SAXDOM objects are read-only.");
	}

	// The specification does not allow a NO_MODIFICATION_ALLOWED exception
	// here, but we throw one anyway.
	public Attr createAttributeNS(String namespaceURI, String qualifiedName)
			throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"SAXDOM objects are read-only.");
	}

	public NodeList getElementsByTagNameNS(String namespaceURI, String localName) {
		return BufferManager.getElementsByTagNameNS(index, namespaceURI,
				localName);
	}

	// We cannot do this yet, since we don't know which attributes
	// are IDs, since we don't understand DTDs.
	public Element getElementById(String elementId) {
		return null;
	}

	public String getDocumentURI() {
		throw new PEException("Not implemented yet");
	}

	public DOMConfiguration getDomConfig() {
		throw new PEException("Not implemented yet");
	}

	public String getInputEncoding() {
		throw new PEException("Not implemented yet");
	}

	public String getXmlEncoding() {
		throw new PEException("Not implemented yet");
	}

	public boolean getXmlStandalone() {
		throw new PEException("Not implemented yet");
	}

	public String getXmlVersion() {
		throw new PEException("Not implemented yet");
	}

	public void normalizeDocument() {
		throw new PEException("Not implemented yet");
	}

	public Node renameNode(Node n, String namespaceURI, String qualifiedName)
			throws DOMException {
		throw new PEException("Not implemented yet");
	}

	public void setDocumentURI(String documentURI) {
		throw new PEException("Not implemented yet");
	}

	public void setXmlStandalone(boolean xmlStandalone) throws DOMException {
		throw new PEException("Not implemented yet");
	}

	public void setXmlVersion(String xmlVersion) throws DOMException {
		throw new PEException("Not implemented yet");
	}
}
