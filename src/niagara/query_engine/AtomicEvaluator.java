package niagara.query_engine;

import java.util.ArrayList;
import org.w3c.dom.*;

import niagara.utils.*;
import niagara.xmlql_parser.syntax_tree.*;

public class AtomicEvaluator {
    boolean isConstant;
    String constant;

    int streamId;
    int attributeId;
    PathExprEvaluator pev;

    StreamTupleElement[] oneTuple;
    NodeVector reachableNodes;

    public AtomicEvaluator(Object value) {
        if (value instanceof String) {
            isConstant = true;
            constant = (String) value;
        } else {
            isConstant = false;
            schemaAttribute sa = (schemaAttribute) value;
	    
            streamId = sa.getStreamId();
	    attributeId = sa.getAttrId();
            pev = new PathExprEvaluator(sa.getPath());
        }
        
        oneTuple = new StreamTupleElement[1];
        reachableNodes = new NodeVector();
    }
    
    /**
     * This function finds the atomic values associated with a given value
     * in a predicate.
     *
     * @param tuples The tuples over which the value is defined
     * @param values An ArrayList to add the atomic values in
     *
     * KT - looks to me like atomic values are always strings
     * XXX vpapad: They are!
     */

    void getAtomicValues (StreamTupleElement[] tuples, ArrayList values) {
	// If the value is of type string, then just add it to result
	//
	if (isConstant) 
	    values.add(constant);
	else {
	    // Invoke the path expression evaluator to get the nodes reachable
	    // using the path expression
            pev.getMatches(tuples[streamId].
			   getAttribute(attributeId),
                           reachableNodes);

	    // For each reachable node, get the atomic value associated with
	    // it
	    int numReachableNodes = reachableNodes.size();

	    for (int nodeid = 0; nodeid < numReachableNodes; ++nodeid) {

		// First get the node
		//
		Node node = reachableNodes.get(nodeid);

		if (node.getNodeType() == Node.ATTRIBUTE_NODE) {
		    values.add(node.getNodeValue());
		    continue;
		}

		// Now get its first child
		//
		Node firstChild = node.getFirstChild();

		// If such a child exists, then add its value to the result
		//
		if (firstChild != null) {
		    
		    values.add(firstChild.getNodeValue());
		}
	    }
            reachableNodes.clear();
        }
    }
    
    void getAtomicValues(StreamTupleElement tuple, ArrayList values) {
        oneTuple[0] = tuple;
        getAtomicValues(oneTuple, values);
    }
}
