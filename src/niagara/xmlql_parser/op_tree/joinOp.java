
/**********************************************************************
  $Id: joinOp.java,v 1.4 2001/07/17 06:52:23 vpapad Exp $


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
 * This class is used to represent the join operator.
 *
 */
package niagara.xmlql_parser.op_tree;

import java.util.*;
import niagara.xmlql_parser.syntax_tree.*;

public class joinOp extends binOp {

	private predicate pred;// join predicate

	// for equi-join represents the attributes of the left relation
	// that will join with those of the right relation
	private Vector equiJoinAttr_Lrel;    // not used at present
	private Vector equiJoinAttr_Rrel;    // in the query engine

   /**
    * Constructor
    *
    * @param list of algorithm to implement this operator
    */
   public joinOp(Class[] al) {
	super(new String("Join"),al);
   }
  
   /**
    * @return the join predicate
    */
   public predicate getPredicate() {
	return pred;
   }

   /**
    * @return the attributes of the left relation that equi-joins with those of 
    *         right relation
    */
   public Vector getLeftEqJoinAttr() {
	return equiJoinAttr_Lrel;
   }

   /**
    * @return the attributes of the right relation in the equi-join
    */
   public Vector getRightEqJoinAttr() {
	return equiJoinAttr_Rrel;
   }

   /**
    * sets the parameter for the join operator. It constructs the predicate
    * for the equi-join part and then ANDs it to the given join predicate.
    *
    * @param join predicate
    * @param left attributes of the equi-join
    * @param right attributes of the equi-join
    */
   public void setJoin(predicate p, Vector lre, Vector rre) {
	equiJoinAttr_Lrel = lre;
	equiJoinAttr_Rrel = rre;

	// creates a predicate for the equi-join
	predicate equiJoinPred = Util.makePredicate(lre,rre);

	if(equiJoinPred != null)
		if(p!=null)
			// AND it to the join predicate if present
			pred = new predLogOpNode(opType.AND,p,equiJoinPred);
  		else
			pred = equiJoinPred;
	else
		pred = p;
   }

   /**
    * sets the predicate for a generic join operator. 
    * @param pred predicate
    */
   public void setJoin(predicate pred) {
       this.pred = pred;
   }

   /**
    * print the operator to the standard output
    */
   public void dump() {
      System.out.println("Join : ");
      if(pred != null)
         pred.dump(1);
   }

   /**
    * dummy toString method
    *
    * @return the String representation of the operator
    */
   public String toString() {
      StringBuffer strBuf = new StringBuffer();
      strBuf.append("Join");

      return strBuf.toString();
   }

    public String dumpAttributesInXML() {
        return " physical='" + getSelectedAlgo().getName() +"' ";
    }

    public String dumpChildrenInXML() {
        return pred.toXML();
    }
}
