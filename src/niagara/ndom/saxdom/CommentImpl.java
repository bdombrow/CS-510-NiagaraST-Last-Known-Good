/**
 * $Id: CommentImpl.java,v 1.2 2002/03/27 10:12:10 vpapad Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

import org.w3c.dom.*;


public class CommentImpl extends CharacterDataImpl {

    public CommentImpl(DocumentImpl doc, int index) {
        super(doc, index);
    }

    public short getNodeType() {
        return Node.COMMENT_NODE;
    }

    public String getNodeName() {
        // XXX vpapad: What are we supposed to return here?
        return null;
    }
}
