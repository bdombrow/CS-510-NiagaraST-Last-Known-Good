package niagara.ndom.saxdom;

/**
 * 
 * A read-only implementation of the DOM Level 2 interface, using an array of
 * SAX events as the underlying data store.
 * 
 */

public class CDATASectionImpl extends TextImpl {

	public CDATASectionImpl(DocumentImpl doc, int index) {
		super(doc, index);
	}

}
