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

import niagara.utils.nitree.NINode;

abstract class NumberNodeHelperBase implements NumberNodeHelper {
    public abstract boolean average(NINode lNode, NINode rNode, 
				    NINode resultNode);
    public abstract boolean sum(NINode lNode, NINode rNode, NINode resultNode);
    public abstract boolean lessThan(NINode lNode, NINode rNode);
    public abstract boolean nodeEquals(NINode lNode, NINode rNode);

    public boolean greaterThan(NINode lNode, NINode rNode) {
	return !lessThan(lNode, rNode);
    }
    
    public boolean greaterOrEqual(NINode lNode, NINode rNode) {
	return (greaterThan(lNode,rNode) || nodeEquals(lNode, rNode));
    }
	
    public boolean lessOrEqual(NINode lNode, NINode rNode) {
	return (lessThan(lNode,rNode) || nodeEquals(lNode, rNode));
    }
}

