/**
 * $Id: CDATASectionImpl.java,v 1.3 2003/07/18 01:04:43 tufte Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

public class CDATASectionImpl extends TextImpl {

    public CDATASectionImpl(DocumentImpl doc, int index) {
        super(doc, index);
    }

}
