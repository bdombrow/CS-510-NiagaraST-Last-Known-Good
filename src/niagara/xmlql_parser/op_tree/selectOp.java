
/**********************************************************************
  $Id: selectOp.java,v 1.2 2001/07/17 06:52:23 vpapad Exp $


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
import niagara.xmlql_parser.syntax_tree.*;

public class selectOp extends unryOp {

   private predicate pred;  // predicate for the selection

   /**
    * Constructor
    *
    * @param list of algorithm to implement this operator
    */
   public selectOp(Class[] al) {
	super(new String("Select"),al);
   }

   /**
    * @return the selection predicate
    */
   public predicate getPredicate() {
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

    // XXX hack 
    boolean[] clear;

    String clearAttr;

    public void setClearAttr(String clearAttr) {
        this.clearAttr = clearAttr;
    }

    public void setClear(boolean[] clear) {
        this.clear = clear;
    }
    public boolean[] getClear() {
        return clear;
    }

    public String dumpAttributesInXML() {
        return "clear='" + clearAttr + "'";
    }
    public String dumpChildrenInXML() {
        return pred.toXML();
    }
}

