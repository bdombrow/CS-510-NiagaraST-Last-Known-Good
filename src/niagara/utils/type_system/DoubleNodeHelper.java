package niagara.utils.type_system;

/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

import niagara.utils.nitree.*;
import niagara.utils.PEException;

/**
 * Class <code> DoubleNodeHelper </code> which performs functions
 * on two nodes, interpreting the values as double.
 *
 * Intended to add a "double type" to the niagara system.
 *
 * @version 1.0
 *
 * @author Kristin Tufte
 */


class DoubleNodeHelper extends NumberNodeHelperBase 
    implements NumberNodeHelper {

    private Class myClass;

    public DoubleNodeHelper() {
	try {
	    myClass = Class.forName("Double");
	} catch (ClassNotFoundException e) {
	    throw new PEException("This should never happen");
	}
    }

    public Class getNodeClass() { return myClass; }

    public Object valueOf(NINode node) {
	return Double.valueOf(node.getNodeValue());
    }
    
    public boolean nodeEquals(NINode lNode, NINode rNode) {
	/* should I do some optimization here and try to store
	   a local value in the NINodes??
	   Double lVal = (Double)lNode.getLocalValue();
	   Double rVal = (Double)rNode.getLocalValue();
	   if(lVal != null && rVal != null) {
	   return lVal.equals(rVal)
	   } 
	*/
	
	return valueOf(lNode).
	    equals(valueOf(rNode));
    }
    
    public boolean lessThan(NINode lNode, NINode rNode) {
	return ((Double)valueOf(lNode)).doubleValue() <
	    ((Double)valueOf(rNode)).doubleValue();
    }
    
    public boolean average(NINode lNode, NINode rNode, 
				  NINode resultNode) {
	double lVal = ((Double)valueOf(lNode)).doubleValue();
	double rVal = ((Double)valueOf(rNode)).doubleValue();
	resultNode.setNodeValue(String.valueOf((lVal+rVal)/2));
	return true;
    }

    public boolean sum(NINode lNode, NINode rNode, NINode resultNode) {
	double lVal = ((Double)valueOf(lNode)).doubleValue();
	double rVal = ((Double)valueOf(rNode)).doubleValue();
	resultNode.setNodeValue(String.valueOf(lVal+rVal));
	return true;
    }
}
