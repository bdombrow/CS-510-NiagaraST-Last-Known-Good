
/**********************************************************************
  $Id: predicate.java,v 1.3 2001/07/17 06:53:29 vpapad Exp $


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
 * This is an abstract class for constructing binary tree to represent 
 * a predicate. This class stores the operator, while its two subclass
 * predArithOpNode and predLogOpNode stores the operands (former stores
 * arithmetic operands, while later stores predicates itself).
 *
 */
package niagara.xmlql_parser.syntax_tree;


import java.util.*;

import org.w3c.dom.*;

public abstract class predicate extends condition {

//    Operators 	-- opType.AND, opType.OR, opType.NOT, 
//			   opType.EQ, opType.NEQ, opType.GEQ,
//			   opType.LEQ, opType.GT, opType.LT
//
	protected int operator;   // arithmetic or logical
	protected Vector varList; // list of variables encountered in
				  // the subtree rooted at this node

	/**
	 * Constructor
	 *
	 * @param the operator
	 */

	public predicate(int op) {
		operator = op;
		varList = new Vector();
	}

	/**
	 * Constructor without any arguments
	 */

        public predicate() {
            operator = -1;
            varList = new Vector();
        }

	/**
	 * @return operator
	 */

	public int getOperator() {
		return operator;
	}

	/**
	 * @return the list of variables
	 */

	public Vector getVarList() {
		return varList;
	}

	/**
	 * abstract method to print the node on the screen
	 *
	 * @param number of tabs at the beginning of each line
	 */

	public abstract void dump(int depth);

	/**
	 * replace a variable with its corresponding schemaAttribute
	 *
	 * @param the variable table
	 */

	public abstract void replaceVar(varTbl tableofvar);

	/**
	 * for join predicate, the variable could be either in the left
	 * or right data stream (InClause).
	 *
	 * @param the variable table for left data stream
	 * @param the variable table for right data stream
	 */

	public abstract void replaceVar(varTbl leftVarTbl, varTbl rightVarTbl);
    
    public String toXML() {
        return "<pred op='" + opType.getName(getOperator()) + "'>" + 
            dumpChildrenInXML() + "</pred>";
    }
    
    public abstract String dumpChildrenInXML();
}
