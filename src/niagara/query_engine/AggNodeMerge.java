package niagara.query_engine;

/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */


/**
 * A <code> MinNodeMerge </code> element will merge two nodes
 * together using a specified aggregate function.
 * 
 * @version 1.0
 *
 * @author Kristin Tufte
 */

import java.lang.reflect.*;
import java.io.*;

import org.w3c.dom.*;

import niagara.utils.type_system.*;
import niagara.utils.*;

class AggNodeMerge extends NodeMerge {

    /* both valid for only aggregate merge */
    private Method aggMethod; /* typically a NodeHelper method */
    private Object aggObject; /* on which aggMethod should be called */
    
    AggNodeMerge(int mergeType, NumberNodeHelper _comparator, String fcn) 
	throws MTException {
	try {
	    setInnerOuter(mergeType);
	    
	    aggObject = _comparator;
	    Class nodeClass = Class.forName("org.w3c.dom.Node");
	    Class intClass = Class.forName("java.lang.Integer");
	    Class[] paramTypes = {nodeClass, nodeClass, nodeClass};
	    aggMethod = aggObject.getClass().getDeclaredMethod(fcn, 
							       paramTypes);
	    name = null;
	} catch (NoSuchMethodException e) {
	    throw new MTException("NoSuchMethod (" + fcn + ")" + e.getMessage());
	} catch (ClassNotFoundException e) {
	    throw new PEException("Class for Node not found" + e.getMessage());
	}
    }

    /**
     * Function to merge two nodes (can be attrs or elements)
     *
     * @param lNode left node to be merged
     * @param rNode right node to be merged
     * @param resultNode - the place to put the new result
     * 
     * @return Returns true if resultNode changed, false if
     *         no updates need to be made based on this merge
     */

    boolean merge(Node lNode, Node rNode, Node resultNode) 
	throws ShutdownException {
	Object[] args = {lNode, rNode, resultNode};
	try {
	    return ((Boolean)aggMethod.invoke(aggObject, args)).booleanValue();
	} catch (IllegalAccessException e1){
	    throw new 
		ShutdownException("Invalid Method Invocation - Illegal Access - on " + aggMethod.getName() + "  " + e1.getMessage());
	} catch (InvocationTargetException e2) {
	    if(aggObject == null) {
		System.out.println("aggObject is null");
	    } else {
		System.out.println("aggObject is " + aggObject.getClass().getName());
	    }
	    throw new 
		ShutdownException("Invalid Method Invocation - Invocation Target - on " + aggMethod.getName() + "  " + e2.getMessage());
	}
    }

    public void dump(PrintStream os) {
	os.println("Aggregate Merge:");
	os.print("Function: " + aggMethod.getName());
	os.print("Helper: " + ((NodeHelper)aggObject).getName());
	os.println();
    }

    public String toString() {
	return "Aggregate Merge " + aggMethod.getName();
    }

    public String getName() {
	return "Aggregate";
    }
}

