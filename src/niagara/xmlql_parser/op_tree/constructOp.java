
/**********************************************************************
  $Id: constructOp.java,v 1.5 2002/05/23 06:32:03 vpapad Exp $


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
 * This operator is used to construct XML results. This is analogous to SELECT
 * of SQL.
 *
 */
package niagara.xmlql_parser.op_tree;

import java.util.*;
import niagara.xmlql_parser.syntax_tree.*;

public class constructOp extends unryOp {

	constructBaseNode resultTemplate; // internal node or leaf node
					  // if it is the internal node, then
					  // all its children are leaf node that
					  // represents the schemaAttributes

    /**
     * If <code>clear</code> is set to true, the operator will
     * not pass attributes from incoming tuples to the outgoing stream.
     */
    boolean clear;

	/**
	 * @return the constructNode that has information about the tag names
	 *         and children
	 */
	public constructBaseNode getResTemp() {
		return resultTemplate;
	}

	/**
	 * used to set parameter for the construct operator
	 *
	 * @param the construct part (tag names and children if any)
	 */
	public void setConstruct(constructBaseNode temp, boolean clear) {
		resultTemplate = temp;
		this.clear = clear;
	}

    public void setConstruct(constructBaseNode temp) {
	setConstruct(temp, false);
    }

    public boolean isClear() {
	return clear;
    }

    /**
     * print the operator to the standard output
     */
    public void dump() {
	System.out.println("Construct : ");
	resultTemplate.dump(1);
    }

	/**
	 * a dummy toString method
	 *
	 * @return String representation of this operator
	 */
	public String toString() {
	   return "ConstructOp";
        }
}
