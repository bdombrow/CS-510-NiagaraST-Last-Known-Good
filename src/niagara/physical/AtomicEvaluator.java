/* $Id: AtomicEvaluator.java,v 1.2 2004/05/20 22:10:22 vpapad Exp $ */
package niagara.physical;

import java.util.ArrayList;

import org.w3c.dom.Node;

import niagara.logical.path.Epsilon;
import niagara.logical.path.RE;
import niagara.query_engine.*;
import niagara.utils.NodeVector;
import niagara.utils.Tuple;
import niagara.xmlql_parser.schemaAttribute;

public class AtomicEvaluator {
    private String name;
    private RE path;
    
    int streamId;
    int attributeId;

    PathExprEvaluator pev;

    NodeVector reachableNodes;

    /** Initialize an evaluator with a variable's name,
     *  which later on must be resolved with resolveVariables */
    public AtomicEvaluator(String name) {
        this.name = name;
        this.path = new Epsilon();
    }

    public AtomicEvaluator(String name, RE path) {
        this.name = name;
        // If path is null using an AtomicEvaluator is overkill.
        // Use SimpleAtomicEvaluator whenever possible.
        this.path = path;
    }
    
    /** Initialize an evaluator for a specific input attribute */
    public AtomicEvaluator(schemaAttribute sa) {
        resolveVariables(sa);
    }

    public void resolveVariables(TupleSchema ts, int streamId) {
        if (!ts.contains(name)) return;
        this.streamId = streamId;
        this.attributeId = ts.getPosition(name);
        pev = new PathExprEvaluator(path);
        reachableNodes = new NodeVector();
    }

    public void resolveVariables(schemaAttribute sa) {
        streamId = sa.getStreamId();
        attributeId = sa.getAttrId();
        pev = new PathExprEvaluator(sa.getPath());
        reachableNodes = new NodeVector();
    }

    /**
     * This function appends the atomic values associated with a given value
     * in a predicate to an ArrayList
     *
     * @param t1    Left incoming tuple
     * @param t2    Right incoming tuple
     * @param values An ArrayList to add the atomic values in
     *
     * KT - looks to me like atomic values are always strings
     * XXX vpapad: They are!
     */

    public void getAtomicValues(
        Tuple t1,
        Tuple t2,
        ArrayList values) {
        Tuple tuple;
        
        if (streamId == 0)
            tuple = t1;
        else
            tuple = t2;

		Node n = tuple.getAttribute(attributeId);
		if(n == null)
			return ;
        getAtomicValues(tuple.getAttribute(attributeId), values);
    }
    
    public final void getAtomicValues(Node n, ArrayList values) {
        // Invoke the path expression evaluator to get the nodes reachable
        // using the path expression
        pev.getMatches(n, reachableNodes);

        // For each reachable node, get the atomic value associated with it
        int numReachableNodes = reachableNodes.size();

        for (int nodeid = 0; nodeid < numReachableNodes; ++nodeid) {
            // First get the node
            Node node = reachableNodes.get(nodeid);

            if (node.getNodeType() == Node.ATTRIBUTE_NODE) {
                values.add(node.getNodeValue());
                continue;
            }

            // Now get its first child
            Node firstChild = node.getFirstChild();

            // If such a child exists, then add its value to the result
            if (firstChild != null) {
                values.add(firstChild.getNodeValue());
            }
        }
        reachableNodes.clear();
    } 
 
     public void getAtomicValues(
        Tuple t1,
        ArrayList values) {
        getAtomicValues(t1.getAttribute(attributeId), values);
    }
}
