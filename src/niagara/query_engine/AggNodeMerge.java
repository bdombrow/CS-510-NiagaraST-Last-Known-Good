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

import java.lang.*;
import java.lang.reflect.*;
import java.io.*;

import niagara.utils.type_system.*;
import niagara.utils.nitree.*;
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
	    Class niNodeClass = Class.forName("niagara.utils.nitree.NINode");
	    Class[] paramTypes = {niNodeClass, niNodeClass, niNodeClass};
	    aggMethod = aggObject.getClass().getDeclaredMethod(fcn, 
							       paramTypes);
	    name = null;
	} catch (NoSuchMethodException e) {
	    throw new MTException("NoSuchMethod " + e.getMessage());
	} catch (ClassNotFoundException e) {
	    throw new PEException("Class for NINode not found" + e.getMessage());
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

    boolean merge(NINode lNode, NINode rNode, NINode resultNode) 
	throws OpExecException {
	Object[] args = {lNode, rNode, resultNode};
	try {
	    return ((Boolean)aggMethod.invoke(aggObject, args)).booleanValue();
	} catch (IllegalAccessException e1){
	    throw new 
		OpExecException("Invalid Method Invocation - Illegal Access - on " + aggMethod.getName() + "  " + e1.getMessage());
	} catch (InvocationTargetException e2) {
	    throw new 
		OpExecException("Invalid Method Invocation - Invocation Target - on" + aggMethod.getName() + "  " + e2.getMessage());
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
