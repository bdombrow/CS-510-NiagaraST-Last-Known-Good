/**********************************************************************
  $Id: selectOp.java,v 1.6 2002/12/10 00:51:53 vpapad Exp $


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
 * This class is used to represent the select operator
 *
 */
package niagara.xmlql_parser.op_tree;

import java.util.*;

import niagara.logical.*;
import niagara.optimizer.colombia.*;

public class selectOp extends unryOp {
    
   private Predicate pred;  // predicate for the selection

    public selectOp() {}
    
    public selectOp(Predicate pred) {
        this.pred = pred;
    }
    
   /**
    * @return the selection predicate
    */
   public Predicate getPredicate() {
	return pred;
   }

   /**
    * used to set the predicate of Select operator. A list of predicate are
    * ANDed to produce single predicate for this operator
    *
    * @param list of predicates
    */
   public void setSelect(Vector _preds) {
	pred = niagara.xmlql_parser.syntax_tree.Util.andPredicates(_preds);
   }

   /**
    * print the operator to the standard output
    */
   public void dump() {
      System.out.println("Select :");
      pred.dump(1);
   }

   /**
    * dummy toString method
    *
    * @return String representation of this operator
    */
   public String toString() {
      StringBuffer strBuf = new StringBuffer();
      strBuf.append("Select");

      return strBuf.toString();
   }


    public void dumpAttributesInXML(StringBuffer sb) {
        sb.append(" ");
    }
    public void dumpChildrenInXML(StringBuffer sb) {
        sb.append(">");
        pred.toXML(sb);
        sb.append("</select>");
    }
    
    
    public LogicalProperty findLogProp(
        ICatalog catalog,
        LogicalProperty[] input) {
        LogicalProperty result = input[0].copy();
        result.setCardinality(result.getCardinality() * pred.selectivity());
        return result;
    }
    
    public Op copy() {
        return new selectOp(pred);
    }
    
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof selectOp)) return false;
        if (obj.getClass() != selectOp.class) return obj.equals(this);
        selectOp other = (selectOp) obj;
        return pred.equals(other.pred);
    }
    
    public int hashCode() {
        return pred.hashCode();
    }

    public Attrs requiredInputAttributes(Attrs inputAttrs) {
        ArrayList al = new ArrayList();
        pred.getReferencedVariables(al);
        Attrs reqd = new Attrs(al);
        assert inputAttrs.contains(reqd);
        return reqd;
    }
 
    public boolean isEmpty() {
        return pred.equals(True.getTrue());
    }
    
    /** Can we combine the selection with this unnesting? */   
    public boolean isPushableInto(Unnest unnest) {
        ArrayList al = new ArrayList();
        pred.getReferencedVariables(al);
        return (al.size() == 1 && ((Variable) al.get(0)).equals(unnest.getVariable()));
    }
}

