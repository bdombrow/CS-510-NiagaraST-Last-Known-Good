package niagara.utils.type_system;

/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

import org.w3c.dom.*;

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


public class DoubleNodeHelper extends NumberNodeHelperBase 
    implements NumberNodeHelper {

    private Class myClass;

    public DoubleNodeHelper() {
	try {
	    myClass = Class.forName("java.lang.Double");
	} catch (ClassNotFoundException e) {
	    throw new PEException("Coudn't get class for double");
	}
    }

    public Class getNodeClass() { return myClass; }

    public Object valueOf(Node node) {
	return Double.valueOf(node.getNodeValue().trim());
    }
    
    public boolean nodeEquals(Node lNode, Node rNode) {
	/* should I do some optimization here and try to store
	   a local value in the Nodes??
	   Double lVal = (Double)lNode.getLocalValue();
	   Double rVal = (Double)rNode.getLocalValue();
	   if(lVal != null && rVal != null) {
	   return lVal.equals(rVal)
	   } 
	*/
	
	return valueOf(lNode).
	    equals(valueOf(rNode));
    }
    
    public boolean lessThan(Node lNode, Node rNode) {
	return ((Double)valueOf(lNode)).doubleValue() <
	    ((Double)valueOf(rNode)).doubleValue();
    }
    
    public boolean average(Node lNode, Node rNode, 
			   Node resultNode) {
	double lVal = ((Double)valueOf(lNode)).doubleValue();
	double rVal = ((Double)valueOf(rNode)).doubleValue();
	resultNode.setNodeValue(String.valueOf((lVal+rVal)/2));
	return true;
    }

    public boolean sum(Node lNode, Node rNode, Node resultNode) {
	double lVal = ((Double)valueOf(lNode)).doubleValue();
	double rVal = ((Double)valueOf(rNode)).doubleValue();
	resultNode.setNodeValue(String.valueOf(lVal+rVal));
	return true;
    }

    public String getName() {
        return "DoubleNodeHelper";
    }

    public String toString() {
        return getName();
    }

}
