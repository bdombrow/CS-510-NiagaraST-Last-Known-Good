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
 * Class <code> StringNodeHelper </code> which performs functions
 * on two nodes, interpreting the values as strings.
 *
 * Intended to add a "string type" to the niagara system.
 *
 * @version 1.0
 *
 * @author Kristin Tufte
 */


public class StringNodeHelper implements NodeHelper {
    
    private Class myClass;

    public StringNodeHelper() {
	try {
	    myClass = Class.forName("java.lang.String");
	} catch (ClassNotFoundException e) {
	    throw new PEException(); /* should never get here */
	}
    }

    public Class getNodeClass() { return myClass;}

    public Object valueOf(Node node) {
	return DOMHelper.getTextValue(node);
    }

    public boolean nodeEquals(Node lNode, Node rNode) {
	if(lNode == null) {
	    if(rNode == null) {
		return true;
	    }
	    else
		return false;
	}
	String tvl = DOMHelper.getTextValue(lNode);
	String tvr = DOMHelper.getTextValue(rNode); 
	if(tvl == null || tvr == null) {
	    if(tvl == null && tvr == null) {
		return true;
	    }
	    else
		return false;
	}
	return tvr.equals(tvl);
    }
    public String getName() {
        return "StringNodeHelper";
    }

    public String toString() {
        return getName();
    }

}
