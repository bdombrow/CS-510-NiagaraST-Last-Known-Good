/**
 * $Id: DOMImplementationImpl.java,v 1.1 2002/03/26 22:07:50 vpapad Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

import org.w3c.dom.*;

/** 
 * No real functionality in this class yet - all methods return null/false
 *
 */
public class DOMImplementationImpl implements DOMImplementation {
    private static DOMImplementation dom_implementation;

    public static DOMImplementation getImplementation() { 
        // XXX vpapad: this looks fishy, but has to do for now
        if (dom_implementation == null) 
            dom_implementation = new DOMImplementationImpl();
        return dom_implementation;
    }

    public boolean hasFeature(String feature, String version) {
        return false;
    }

    public DocumentType createDocumentType(String qualifiedName, 
                                           String publicId, String systemId)
        throws DOMException {
        return null;
    }

    public Document createDocument(String namespaceURI, 
                                   String qualifiedName, DocumentType doctype)
        throws DOMException {
        return null;
    }

}
