/* $Id: AtomicEvaluator.java,v 1.7 2002/12/10 01:17:45 vpapad Exp $ */
package niagara.query_engine;

import java.util.ArrayList;
import java.util.HashMap;

import org.w3c.dom.Node;

import niagara.utils.NodeVector;
import niagara.utils.StreamTupleElement;
import niagara.xmlql_parser.syntax_tree.regExp;
import niagara.xmlql_parser.syntax_tree.schemaAttribute;

public class AtomicEvaluator {
    private String name;
    private regExp path;
    
    int streamId;
    int attributeId;

    PathExprEvaluator pev;

    NodeVector reachableNodes;

    /** Initialize an evaluator with a variable's name,
     *  which later on must be resolved with resolveVariables */
    public AtomicEvaluator(String name) {
        this.name = name;
    }

    public AtomicEvaluator(String name, regExp path) {
        this.name = name;
        // XXX vpapad: we never really use path, it is an opportunity
        // for optimization since it avoids an extra unnest
        // alternatively using PathExprEvaluator just to get the
        // tuple attribute itself is overkill
        // - use SimpleAtomicEvaluator where possible
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
        StreamTupleElement t1,
        StreamTupleElement t2,
        ArrayList values) {
        StreamTupleElement tuple;
        
        if (streamId == 0)
            tuple = t1;
        else
            tuple = t2;

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
        StreamTupleElement t1,
        ArrayList values) {
        getAtomicValues(t1.getAttribute(attributeId), values);
    }
}
