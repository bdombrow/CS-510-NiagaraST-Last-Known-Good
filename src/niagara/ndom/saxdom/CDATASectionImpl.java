/**
 * $Id: CDATASectionImpl.java,v 1.1 2002/03/26 22:07:49 vpapad Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

import org.w3c.dom.*;

public class CDATASectionImpl extends TextImpl {

    public CDATASectionImpl(Document doc, int index) {
        super(doc, index);
    }

}
