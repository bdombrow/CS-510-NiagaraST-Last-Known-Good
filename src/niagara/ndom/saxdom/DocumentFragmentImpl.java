/**
 * $Id: DocumentFragmentImpl.java,v 1.1 2002/03/26 22:07:50 vpapad Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

import org.w3c.dom.*;

public class DocumentFragmentImpl extends NodeImpl 
    implements DocumentFragment {
    private Document doc;

    public DocumentFragmentImpl(Document doc) {
        // XXX vpapad: I don't like this -1 business, but...
        super(doc, -1);
    }

    public short getNodeType() {
        return Node.DOCUMENT_FRAGMENT_NODE;
    }

    public String getNodeName() {
        // XXX vpapad: What are we supposed to return here?
        return null;
    }
}
