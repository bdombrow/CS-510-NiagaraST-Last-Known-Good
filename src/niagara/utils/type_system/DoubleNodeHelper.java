package niagara.utils.type_system;

/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

import org.w3c.dom.*;

import niagara.utils.*;

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
	return Double.valueOf(DOMHelper.getTextValue(node).trim());
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

	if(lNode == null) {
	    if(rNode == null)
		return true;
	    else
		return false;
	}

	return valueOf(lNode).
	    equals(valueOf(rNode));
    }
    
    public boolean lessThan(Node lNode, Node rNode) {
	return ((Double)valueOf(lNode)).doubleValue() <
	    ((Double)valueOf(rNode)).doubleValue();
    }
    
    public boolean average(Node lNode, Node rNode, Node resultNode) {
	int rCnt = getCount(rNode);
	double rVal = ((Double)valueOf(rNode)).doubleValue()*rCnt;

	if(rCnt != 1) {
	    System.out.println("KT throwing PEException - HOPE IT IS HANDLED RIGHT");
	    throw new PEException("KT why is this happening?");
	}

	if(lNode == null) {
	    DOMHelper.setTextValue(resultNode, String.valueOf(rVal/rCnt));
	    setCount(resultNode, rCnt);
	} else {
	    int lCnt = getCount(lNode);
	    double lVal = ((Double)valueOf(lNode)).doubleValue()*lCnt;
	    DOMHelper.setTextValue(resultNode, String.valueOf((lVal+rVal)/(lCnt+rCnt)));
	    setCount(resultNode, lCnt+rCnt);
	}
	return true;
    }

    public boolean sum(Node lNode, Node rNode, Node resultNode) {

	double rVal = ((Double)valueOf(rNode)).doubleValue();

	if(lNode == null) {
	    DOMHelper.setTextValue(resultNode, String.valueOf(rVal));
	} else {
	    double lVal = ((Double)valueOf(lNode)).doubleValue();
	    DOMHelper.setTextValue(resultNode, String.valueOf(lVal+rVal));
	}
	return true;
    }

    public String getName() {
        return "DoubleNodeHelper";
    }

    public String toString() {
        return getName();
    }

}
