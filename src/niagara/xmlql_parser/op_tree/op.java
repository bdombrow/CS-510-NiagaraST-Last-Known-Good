/**********************************************************************
  $Id: op.java,v 1.9 2002/09/20 23:07:10 vpapad Exp $


  NIAGARA -- Net Data Management System                                 
                                                                        
  Copyright (c)    Computer Sciences Department, University of          
                       Wisconsin -- Madison                             
  All Rights Reserved.                                                  
                                                                        
  Permission to use, copy, modify and distribute this software and      
  its documentation is hereby granted, provided that both the           
  copyright notice and this permission notice appear in all copies      
  of the software, derivative works or modified versions, and any       
  portions thereof, and that both notices appear in supporting          
  documentation.                                                        
                                                                        
  THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY    
  OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS "        
  AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND         
  FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.   
                                                                        
  This software was developed with support by DARPA through             
   Rome Research Laboratory Contract No. F30602-97-2-0247.  
**********************************************************************/


/**
 * This class is used as a base class for all the classes that represents
 * different logical operators.
 *
 */

package niagara.xmlql_parser.op_tree;

import java.util.*;
import niagara.xmlql_parser.syntax_tree.*;
import niagara.connection_server.NiagraServer;

import niagara.optimizer.colombia.LogicalOp;

public abstract class op extends LogicalOp {
    // This is the index of the selected algorithm
    int selectedAlgorithmIndex;

    /**
     * Constructor
     */
    public op() {
        // Initially, no algorithm is selected
        selectedAlgorithmIndex = -1;
    }

    /**
     * @return name of the operators
     */
    public String getName() {
        return NiagraServer.getCatalog().getOperatorName(getClass().getName());
    }

    /**
     * @return the list of Physical Operator Classes that implements this 
     *         operator
     */
    public Class[] getListOfAlgo() {
        return NiagraServer.getCatalog().getPhysical(getClass().getName());
    }

    /**
     * This function sets sets the selected algorithm index
     *
     * @param Index of selected algorithm in list of algorithms
     */

    public void setSelectedAlgoIndex (int index) {
        // Set the index
        selectedAlgorithmIndex = index;
    }

    /**
     * Select the physical algorithm implemented by this class
     *
     * @param className name of the physical algorithm class 
     */

    public void setSelectedAlgorithm (String className) 
	throws ClassNotFoundException, InvalidAlgorithmException  {
	Class c = Class.forName(className);
        
        Class[] algoList = getListOfAlgo();
	for (int i=0; i < algoList.length; i++) {
	    if (algoList[i] == c) {
		setSelectedAlgoIndex(i);
		return;
	    }
	}
    }

    public class InvalidAlgorithmException extends Exception {}

    /**
     * @return the index of the selected algorithm 
     */

    public int getSelectedAlgoIndex() {
        return selectedAlgorithmIndex;
    }

    /**
     * This function returns the selected algorithm class. If no algorithm
     * is selected, it returns null
     *
     * @return Returns the class of selected algorithm; if no selected
     *         algorithm, returns null
     */

    public Class getSelectedAlgo () {
        // If there is no selected algorithm return null
        if (selectedAlgorithmIndex < 0) {
            return null;
        }
        else {
            // Return selected algorith
            return getListOfAlgo()[selectedAlgorithmIndex];
        }
    }

    /**
     * This function is defined in the dtdScan operator to set documenet sources
     * and dtd to which they conform
     *
     * @param list of data sources to query
     * @param dtd to which these data sources conforms to
     */
    public void setDtdScan(Vector v, String s) {}

    /**
     * To set parameters of scan operator
     *
     * @param parent whose child has to be scanned
     * @param regular expression to scan
     */
    public void setScan(schemaAttribute attr, regExp toScan) {}

    /**
     * to set parameter of select operator
     *
     * @param list of predicates
     */
    public void setSelect(Vector v) {}

    /**
     * to set parameter for join operator
     *
     * @param join predicates
     * @param list of attributes of the left relation for equi-join
     * @param list of attributes of the right relation for equi-join
     */
    public void setJoin(predicate p, Vector l, Vector r) {}

    /**
     * to print the information on the screen
     */

    public abstract void dump();


    /**
     * @return number of output streams of this operator
     */
    public int getNumberOfOutputStreams() {
	return 1;
    }

    public String dumpAttributesInXML() {
        return "";
    }


    public String dumpChildrenInXML() {
        return "";
    }

    // source ops are those that read off an input source and
    // feed that source into the operator tree, sourceOps have
    // different control structures than normal operators and
    // are not inherited from PhysicalOperator, sourceOps can
    // not be at the head of a query
    public boolean isSourceOp() {
	return false; // default is false
    }
}



