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
 * Class <code> IntegerNodeHelper </code> which performs functions
 * on two nodes, interpreting the values as ints.
 *
 * Intended to add a "int type" to the niagara system.
 *
 * @version 1.0
 *
 * @author Kristin Tufte
 */


/** 
 * Class <code> IntegerNodeHelper </code> implements numeric
 * functions on nodes whose content is interpreted as integers
 */
public class IntegerNodeHelper extends NumberNodeHelperBase
    implements NumberNodeHelper {

    private Class myClass;

    public IntegerNodeHelper() {
	try {
	    myClass = Class.forName("java.lang.Integer");
	} catch (ClassNotFoundException e) {
	    throw new PEException("Class not found for integer");
	}
    }

    public Class getNodeClass() { return myClass; }

    public Object valueOf(Node node) {
	String valString = node.getNodeValue().trim();
	return Integer.valueOf(valString);
    }
    
    public boolean nodeEquals(Node lNode, Node rNode) {
	/* should I do some optimization here and try to store
	 * a local value in the Nodes??
	 * Double lVal = (Double)lNode.getLocalValue();
	 * Double rVal = (Double)rNode.getLocalValue();
	 * if(lVal != null && rVal != null) {
	 * return lVal.equals(rVal)
	 *  } 
	*/
	
	return valueOf(lNode).equals(valueOf(rNode));
    }
    
    public boolean lessThan(Node lNode, Node rNode) {
	return ((Integer)valueOf(lNode)).intValue() < 
	    ((Integer)valueOf(rNode)).intValue();
    }

    /** 
     * Averages the content of two nodes - content is interpreted
     * as integers
     * 
     * @param lNode First node input to average
     * @param rNode Second node input to average
     * @param resultNode Place to put the result of the average calculation
     *
     * @ return Returns true if a new value is calculated (always)
     */
    public boolean average(Node lNode, Node rNode, 
			   Node resultNode) {
	int lVal = ((Integer)valueOf(lNode)).intValue();
	int rVal = ((Integer)valueOf(rNode)).intValue();
	resultNode.setNodeValue(String.valueOf((lVal+rVal)/2.0));
	return true;
    }

    public boolean sum(Node lNode, Node rNode, Node resultNode) {
	int lVal = ((Integer)valueOf(lNode)).intValue();
	int rVal = ((Integer)valueOf(rNode)).intValue();
	resultNode.setNodeValue(String.valueOf(lVal+rVal));
	return true;
    }

    public String getName() {
        return "IntegerNodeHelper";
    }

    public String toString() {
        return getName();
    }

}
