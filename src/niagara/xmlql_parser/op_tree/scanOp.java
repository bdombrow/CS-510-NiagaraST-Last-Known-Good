
/**********************************************************************
  $Id: scanOp.java,v 1.5 2002/10/27 01:20:21 vpapad Exp $


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
 * This operator is used to read a regular expression from the root of a given
 * subtree.
 *
 */
package niagara.xmlql_parser.op_tree;

import java.util.*;
import niagara.xmlql_parser.syntax_tree.*;

public class scanOp extends UnoptimizableLogicalOperator {

    private schemaAttribute attrId;  // represents the root of the subtree
				     // or one can call it the ancestor of the
				     // element that is to be scanned
    private regExp regExpToScan;     // paths to the elements to scan

    /**
     * get the schemaAttribute that represent the root of the subtree at which
     * the regularExpression representing the path starts.
     *
     * @return the ancestor of the elements to scan
     */
    public schemaAttribute getParent() {
	return attrId;
    }

    /**
     * @return the path of the elements to scan
     */
    public regExp getRegExpToScan() {
	return regExpToScan;
    }

    /**
     * print the operator to the standard output
     */
    public void dump() {
       System.out.println("Scan:");
       attrId.dump(1);
       regExpToScan.dump(1);
    }

    /**
     * dummy toString method
     *
     * @return String representation of the operator
     */
    public String toString() {
       StringBuffer strBuf = new StringBuffer();
       strBuf.append("Scan");

       return strBuf.toString();
    }

    /**
     * set the parameters for this operator
     *
     * @param the root of the subtree at which the regular expression starts
     * @param the path of the elements to scan
     */
    public void setScan(schemaAttribute parent, regExp toScan) {
	attrId = parent;
	regExpToScan = toScan;
    }
}


