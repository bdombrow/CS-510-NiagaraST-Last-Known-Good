package niagara.utils.type_system;

/**
 * Base class for all NumberNodeHelpers - just implements >, >=, <= in
 * terms of a < function which must be implemented by each individual
 * helper
 *
 * @version 1.0
 *
 * @author Kristin Tufte
 */

import org.w3c.dom.*;

public abstract class NumberNodeHelperBase implements NumberNodeHelper {
    public abstract boolean average(Node lNode, Node rNode, 
				    Node resultNode);
    public abstract boolean sum(Node lNode, Node rNode, Node resultNode);
    public abstract boolean lessThan(Node lNode, Node rNode);
    public abstract boolean nodeEquals(Node lNode, Node rNode);

    public boolean greaterThan(Node lNode, Node rNode) {
	return !lessThan(lNode, rNode);
    }
    
    public boolean greaterOrEqual(Node lNode, Node rNode) {
	return (greaterThan(lNode,rNode) || nodeEquals(lNode, rNode));
    }
	
    public boolean lessOrEqual(Node lNode, Node rNode) {
	return (lessThan(lNode,rNode) || nodeEquals(lNode, rNode));
    }
}

