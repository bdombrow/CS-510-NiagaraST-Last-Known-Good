
/**********************************************************************
  $Id: UnionOp.java,v 1.5 2002/10/31 04:17:06 vpapad Exp $


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


package niagara.xmlql_parser.op_tree;

import java.util.*;

import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.syntax_tree.*;

/**
 * This class is used to represent the Union operator.
 */
public class UnionOp extends op {

    private int arity;
    
    public void setArity(int arity) {
        this.arity = arity;
    }
     
    public int getArity() {
        return arity;
    }
       
   /**
    * print the operator to the standard output
    */
   public void dump() {
      System.out.println(this);
   }

   /**
    *
    * @return the String representation of the operator
    */
   public String toString() {
      return "Union";
   }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op copy() {
        UnionOp op = new UnionOp();
        op.setArity(arity);
        return op;
    }
    
    /**
     * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog, ArrayList)
     */
    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        // We only propagate the variables from the first input
        // XXX vpapad: We should be very careful when pushing
        // project through union: before projecting out a variable from
        // the first input we should make sure to project out the
        // respective variables from all the other inputs
        return input[0].copy();
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof UnionOp)) return false;
        if (obj.getClass() != UnionOp.class) return obj.equals(this);
        UnionOp other = (UnionOp) obj;
        return arity == other.arity;
    }

    public int hashCode() {
        return arity;
    }
    
   
    /**
     * @see niagara.optimizer.colombia.Op#matches(Op)
     */
    public boolean matches(Op other) {
        if (arity == 0) // Special case arity = 0 => match any Union
            return (other instanceof UnionOp);
        return super.matches(other);
    }
}
