/* $Id: SimpleAtomicEvaluator.java,v 1.1 2003/12/24 01:49:03 vpapad Exp $ */
package niagara.physical;

import org.w3c.dom.Node;

import niagara.query_engine.*;
import niagara.utils.Tuple;
import niagara.xmlql_parser.schemaAttribute;

/** A simplified version of AtomicEvaluator, for cases where we just
 * want the string value of a tuple attribute, without following paths */
public class SimpleAtomicEvaluator {
    private String name;
    private int streamId;
    private int attributeId;

    /** Initialize an evaluator with a variable's name,
     *  which later on must be resolved with resolveVariables */
    public SimpleAtomicEvaluator(String name) {
        this.name = name;
    }

    public SimpleAtomicEvaluator(schemaAttribute sa) {
        this.streamId = sa.getStreamId();
        this.attributeId = sa.getAttrId();
    }
    
    public void resolveVariables(TupleSchema ts, int streamId) {
        if (!ts.contains(name))
            return;
        this.streamId = streamId;
        this.attributeId = ts.getPosition(name);
    }

    /**
     * This function returns the atomic value of the matching tuple attribute
     * as a single String
     *
     * @param t1    Left incoming tuple
     * @param t2    Right incoming tuple
     */
    public String getAtomicValue(
        Tuple t1,
        Tuple t2) {
        Tuple tuple;

        if (streamId == 0)
            tuple = t1;
        else
            tuple = t2;
		Node n = tuple.getAttribute(attributeId);
		if(n == null)
			return null;
        else
        	return getAtomicValue(n);
    }
    
    public static final String getAtomicValue(Node node) {
        // XXX vpapad: We must get the semantics of atomic values straight!!!
        switch (node.getNodeType()) {
            case Node.ATTRIBUTE_NODE :
                return node.getNodeValue();
                // XXX vpapad: What the original code did,
                // text nodes don't have children
            case Node.TEXT_NODE :
                return null;
                // XXX vpapad: this is what the original code ended up doing,
                // assumes that first child is a text node, otherwise
                // weird things will happen
            default :
                if (node.hasChildNodes())
                    return node.getFirstChild().getNodeValue();
                else
                    return null;
        }
    }
}
