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

interface NumberNodeHelper {
    boolean average(NINode lNode, NINode rNode, NINode resultNode);
    boolean sum(NINode lNode, NINode rNode, NINode resultNode);
    boolean nodeEquals(NINode lNode, NINode rNode);
    boolean lessThan(NINode lNode, NINode rNode);
    boolean greaterThan(NINode lNode, NINode rNode);    
    boolean greaterOrEqual(NINode lNode, NINode rNode);	
    boolean lessOrEqual(NINode lNode, NINode rNode);
}

