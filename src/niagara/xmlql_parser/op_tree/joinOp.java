/**********************************************************************
  $Id: joinOp.java,v 1.6 2002/10/27 01:20:21 vpapad Exp $


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

import niagara.logical.And;
import niagara.logical.EquiJoinPredicateList;
import niagara.logical.Predicate;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.syntax_tree.*;

public class joinOp extends binOp {
	private Predicate pred;// non-equijoin part of the predicate

	// for equi-join represents the attributes of the left relation
	// that will join with those of the right relation
	private EquiJoinPredicateList equiJoinPredicates;
  
  
    public joinOp() {}
    
    public joinOp(Predicate pred, EquiJoinPredicateList equiJoinPredicates) {
        this.pred = pred;
        this.equiJoinPredicates = equiJoinPredicates;
    }
    
   public EquiJoinPredicateList getEquiJoinPredicates() {
        return equiJoinPredicates;
   }
   
   public Predicate getNonEquiJoinPredicate() {
        return pred;
   }
   
   /**
    * @return the attributes of the left relation that equi-joins with those of 
    *         right relation
    */
   public Attrs getLeftEqJoinAttr() {
	return equiJoinPredicates.getLeft();
   }

   /**
    * @return the attributes of the right relation in the equi-join
    */
   public Attrs getRightEqJoinAttr() {
    return equiJoinPredicates.getRight();
   }


   /**
    * @param p non-equijoin part of the predicate
    * @param left attributes of the equi-join
    * @param right attributes of the equi-join
    */
   public void setJoin(Predicate p, ArrayList left, ArrayList right) {
        equiJoinPredicates = new EquiJoinPredicateList(left, right);
        pred = p;    
   }

   public void setCartesian(Predicate p) {
       equiJoinPredicates = new EquiJoinPredicateList();
       pred = p;
   }
   
   /**
    * sets the predicate for a generic join operator. 
    * @param pred predicate
    */
   public void setJoin(Predicate pred) {
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

    public void dumpAttributesInXML(StringBuffer sb) {
        equiJoinPredicates.toXML(sb);
    }
    public void dumpChildrenInXML(StringBuffer sb) {
        sb.append(">");
        pred.toXML(sb);
        sb.append("</join>");
    }
    
    /**
     * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog, LogicalProperty[])
     */
    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        LogicalProperty left = input[0];
        LogicalProperty right = input[1];

        // check the joined predicates(attributes) are in the schema
        assert(left.GetAttrs().Contains(equiJoinPredicates.getLeft()));
        assert(right.GetAttrs().Contains(equiJoinPredicates.getRight()));

        LogicalProperty result = left.copy();

        Predicate allPredicates = And.conjunction(equiJoinPredicates.toPredicate(), pred);
        result.setCardinality(left.getCardinality() * right.getCardinality() 
                              * allPredicates.selectivity());
        
        result.setHasLocal(left.hasLocal() || right.hasLocal());
        result.setHasRemote(left.hasRemote() || right.hasRemote());

        // Derive the schema
        result.GetAttrs().Merge(right.GetAttrs());

        return result;
    }
    
    public boolean isCartesian() {
        return equiJoinPredicates.size() == 0;
    }

    public boolean isEquiJoin() {
        return equiJoinPredicates.size() > 0;
    }
    
    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op copy() {
        return new joinOp(pred.copy(), equiJoinPredicates.copy());
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof joinOp)) return false;
        if (obj.getClass() != joinOp.class) return obj.equals(this);
        joinOp other = (joinOp) obj;
        return pred.equals(other.pred) && equiJoinPredicates.equals(other.equiJoinPredicates);
    }
    
    public int hashCode() {
        return equiJoinPredicates.hashCode() ^ pred.hashCode();
    }
}
