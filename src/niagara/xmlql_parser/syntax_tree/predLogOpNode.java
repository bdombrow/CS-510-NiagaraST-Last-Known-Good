
/**********************************************************************
  $Id: predLogOpNode.java,v 1.1 2000/05/30 21:03:30 tufte Exp $


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
*
* class for representing logical operators in a predicate tree. This class
* extends the abstract class predicate which contains the logical operator.
* Two operands which themselves are predicate are its member.
*
*
*/
package niagara.xmlql_parser.syntax_tree;

import com.ibm.xml.parser.*;
import org.w3c.dom.*;
import java.util.*;

public class predLogOpNode extends predicate {
	
	private predicate lchild;  // left operand for the logical operator
	private predicate rchild;  // right operand if the logical operator
				   //    is binary

	/**
	 * Constructor for binary operator
	 *
	 * constructs the list of variables encountered so far from the
	 * the list of variable in the left subtree and the right subtree.
	 *
	 * @param the logical operator
	 * @param the left operand
	 * @param the right operand
	 */

	public predLogOpNode(int op, predicate lc, predicate rc) {
		super(op);
		lchild = lc;
		rchild = rc;
		
		String var;
		for(int i=0;i<lchild.varList.size();i++) {
			var = (String)lchild.varList.elementAt(i);
			if(!varList.contains(var))
				varList.addElement(var);
		}
		for(int i=0;i<rchild.varList.size();i++) {
			var = (String)rchild.varList.elementAt(i);
			if(!varList.contains(var))
				varList.addElement(var);
		}
	}

	/**
	 * Constructor for unary operator
	 *
	 * @param the logical operator
	 * @param the operand
	 */

	public predLogOpNode(int op, predicate lc) {
		super(op);
		lchild = lc;
		rchild = null;
		for(int i=0;i<lchild.varList.size();i++) 
			varList.addElement(lchild.varList.elementAt(i));
	}

	/**
	 * @return the left child or the only child in case of unary operators
	 */

	public predicate getChild() {
		return lchild;
	}

	/**
	 * @return the left child or operand
	 */

	public predicate getLeftChild() {
		return lchild;
	}

	/**
	 * @return the right child or operand
	 */

	public predicate getRightChild() {
		return rchild;
	}

	/**
	 * replace the existence of variable with its schemaAttribute that 
	 * stores the position of corresponding element or attribute in the
	 * generated schema.
	 *
	 * @param the variable table
	 */

	public void replaceVar(varTbl tableofvar) {
		if(lchild != null) 
			lchild.replaceVar(tableofvar);
		if(rchild != null)
			rchild.replaceVar(tableofvar);
	}

	/**
	 * same as above, except the variables could be from two different
	 * InClause. i.e. the predicate represents a join between two
	 * data streams.
	 *
	 * @param the variable table for the left stream
	 * @param the variable table for the right stream
	 */

	public void replaceVar(varTbl leftVarTbl, varTbl rightVarTbl) {
		if(lchild != null) 
			lchild.replaceVar(leftVarTbl, rightVarTbl);
		if(rchild != null)
			rchild.replaceVar(leftVarTbl, rightVarTbl);
	}

	/**
	 * print this node on the standard output
	 * 
	 * @param number of tabs at the beginning of each line
	 */

	public void dump(int depth) {
		System.out.println("Log Node");
		
		System.out.println();
		Util.genTab(depth);
		switch(getOperator()){
			case opType.AND: System.out.println("AND"); break;
			case opType.NOT: System.out.println("NOT"); break;
			case opType.OR: System.out.println("OR"); break;
		}
	
		Util.genTab(depth);
		System.out.print("[left]");
		if(lchild instanceof predArithOpNode)
			((predArithOpNode)lchild).dump(depth+1);
		else
			((predLogOpNode)lchild).dump(depth+1);

		if(getOperator() == opType.NOT)
			return;

		Util.genTab(depth);
		System.out.print("[right]");
		if(rchild instanceof predArithOpNode)
			((predArithOpNode)rchild).dump(depth+1);
		else
			((predLogOpNode)rchild).dump(depth+1);
	}

}
