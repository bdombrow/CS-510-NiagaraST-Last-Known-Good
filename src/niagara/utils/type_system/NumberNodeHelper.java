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

public interface NumberNodeHelper extends NodeHelper {
    boolean average(Node lNode, Node rNode, Node resultNode);
    boolean sum(Node lNode, Node rNode, Node resultNode);
    boolean nodeEquals(Node lNode, Node rNode);
    boolean lessThan(Node lNode, Node rNode);
    boolean greaterThan(Node lNode, Node rNode);    
    boolean greaterOrEqual(Node lNode, Node rNode);	
    boolean lessOrEqual(Node lNode, Node rNode);
}

