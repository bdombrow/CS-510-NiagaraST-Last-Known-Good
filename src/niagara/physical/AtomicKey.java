/* $Id: AtomicKey.java,v 1.1 2003/12/24 01:49:04 vpapad Exp $ */

package niagara.physical;

import niagara.xmlql_parser.*;
import niagara.utils.*;
import niagara.utils.type_system.*;
import org.w3c.dom.Node;

/**
 * Implementation <code> AtomicKey </code> class which 
 * represents the smallest unit of a key - that is one
 * path and how to match on that path (i.e. content at the
 * end of that path, tag existence, etc.)
 * A list of AtomicKeys makes a LocalKey.
 */

class AtomicKey {

    private int mergeType; /*must be MatchTemplate.TAG_EXISTENCE or .CONTENT */
    private PathExprEvaluator pathEvaluator;
    private NodeHelper nodeHelper;
    private boolean isNever;

    AtomicKey(int mergeType, regExp path, NodeHelper nodeHelper) {
	this.mergeType = mergeType;
	pathEvaluator = new PathExprEvaluator(path);
	this.nodeHelper = nodeHelper;
	isNever = path.isNever();
    }

    void getMatches(Node n, NodeVector results) {
	pathEvaluator.getMatches(n, results);
    } 

    int mergeType() {
	return mergeType;
    }

    NodeHelper nodeHelper() {
	return nodeHelper;
    }

    public boolean isNever() {
	return isNever;
    }
}
