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
 * Class <code> StringNodeHelper </code> which performs functions
 * on two nodes, interpreting the values as strings.
 *
 * Intended to add a "string type" to the niagara system.
 *
 * @version 1.0
 *
 * @author Kristin Tufte
 */


class StringNodeHelper implements NodeHelper {
    
    private Class myClass;

    public StringNodeHelper() {
	try {
	    myClass = Class.forName("String");
	} catch (ClassNotFoundException e) {
	    throw new PEException(); /* should never get here */
	}
    }

    public Class getNodeClass() { return myClass;}

    public Object valueOf(NINode node) {
	return node.getNodeValue();
    }

    public boolean nodeEquals(NINode lNode, NINode rNode) {
	return lNode.getNodeValue().equals(rNode.getNodeValue());
    }

}
