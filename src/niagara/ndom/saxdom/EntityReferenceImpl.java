/**
 * $Id: EntityReferenceImpl.java,v 1.1 2002/03/26 22:07:50 vpapad Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

import org.w3c.dom.*;

public class EntityReferenceImpl extends NodeImpl implements EntityReference {

    public EntityReferenceImpl(Document doc, int index) {
        super(doc, index);
    }

    public short getNodeType() {
        return Node.ENTITY_REFERENCE_NODE;
    }

    public String getNodeName() {
        // XXX vpapad: What should we return here?
        return null;
    }

}
