
/**********************************************************************
  $Id: regExpOpNode.java,v 1.1 2000/05/30 21:03:30 tufte Exp $


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
 * This class is used to represent operators in a regular expression with
 * the operands. 
 *
 */
package niagara.xmlql_parser.syntax_tree;

import java.io.*;
import com.ibm.xml.parser.*;
import org.w3c.dom.*;

public class regExpOpNode extends regExp {
     
     private int operator;	// type of operator

     private regExp lchild;     // left operand (regExp itself)
     private regExp rchild;     // right operand if binary operator

     /**
      * Constructor
      *
      * @param the operator like *, +, etc.
      * @param left operand
      * @param right operand
      */

     public regExpOpNode(int operator, regExp lchild, regExp rchild) {
	this.operator = operator;
	this.lchild = lchild;
	this.rchild = rchild;
     }

     /**
      * Constructor for unary operator
      *
      * @param the operator
      * @param the operand
      */

     public regExpOpNode(int operator, regExp lchild) {
	 this.operator = operator;
	 this.lchild = lchild;
	 this.rchild = null;
     }

     /**
      * Constructor without operand
      *
      * @param the operator
      */

     public regExpOpNode(int operator) {
	this.operator = operator;
	lchild = rchild = null;
     }

     /**
      * @return the left operand or the only operand if unary operator
      */

     public regExp getLeftChild() {
	return lchild;
     }

     /**
      * @return the right operand
      */

     public regExp getRightChild() {
	return rchild;
     }

     /**
      * @return the operator
      */

     public int getOperator() {
	return operator;
     }

     /**
      * print to the screen
      *
      * @param number of tabs at the beginning of each line
      */

     public void dump(int depth) {

	 System.out.println();
	 genTab(depth);
	 
	 switch(operator){
	     
	 case opType.UNDEF: System.out.println("Undefined op type"); break;
	 case opType.STAR: System.out.println("*"); break;
	 case opType.PLUS: System.out.println("+"); break;
	 case opType.QMARK: System.out.println("?"); break;
	 case opType.DOT: System.out.println("."); break;
	 case opType.BAR:System.out.println("|"); break;
	 case opType.DOLLAR:System.out.println("$"); break;	    
	 case opType.LT:System.out.println("<"); break;
	 case opType.GT:System.out.println(">"); break;
	 case opType.LEQ:System.out.println("<="); break;
	 case opType.GEQ:System.out.println(">="); break;
	 case opType.NEQ:System.out.println("!="); break;
	 case opType.EQ:System.out.println("=="); break;
	 case opType.OR:System.out.println("OR"); break;
	 case opType.AND:System.out.println("AND"); break;
	 case opType.NOT:System.out.println("NOT"); break;
	 case opType.ONCE:System.out.println("ONCE"); break;
	 case opType.MULTIPLE:System.out.println("MULTIPLE"); break;
	 case opType.CREATE_TRIG:System.out.println("CREATE_TRIG"); break;
	 case opType.DELETE_TRIG:System.out.println("DELETE_TRIG"); break;
	 case opType.XMLQL:System.out.println("XMLQL"); break;
	 default: System.out.println("Invalid op type"); break;
	 }
	 
	 if(operator == opType.DOLLAR)
	     return;
 
	 genTab(depth);
	 if(rchild == null)
	     System.out.print("[unary] ");
	 else
	     System.out.print("[left ] ");

	 if(lchild instanceof regExpOpNode)
	     ((regExpOpNode)lchild).dump(depth+1);
	 else
	     ((regExpDataNode)lchild).dump(depth+1);

	 if(rchild == null)
	     return;
	 
	 genTab(depth);
	 System.out.print("[right] ");
	 if(rchild instanceof regExpOpNode)
	     ((regExpOpNode)rchild).dump(depth+1);
	 else
	     ((regExpDataNode)rchild).dump(depth+1);
	 
     }
	
     //trigger --Jianjun
     public String toString() {
	System.err.println("trigger--not supported yet");
	return null;
     }
}

