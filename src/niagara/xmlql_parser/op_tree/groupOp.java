/**********************************************************************
  $Id: groupOp.java,v 1.3 2002/10/27 01:20:21 vpapad Exp $


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
 * This is the class for the logical group operator. This is an abstract
 * class from which various notions of grouping can be derived. The
 * core part of this class is the skolem function attributes that are
 * used for grouping and are common to all the sub-classes
 *
 */

package niagara.xmlql_parser.op_tree;

import niagara.optimizer.colombia.*;
import niagara.xmlql_parser.syntax_tree.*;

public abstract class groupOp extends unryOp {
    // The skolem attributes associated with the group operator
    protected skolem skolemAttributes;

    /**
     * This function sets the skolem attributes of the group operator
     *
     * @param skolemAttributes The skolem attributes associated with the
     *                         group operator
     */

    protected void setSkolemAttributes(skolem skolemAttributes) {
        this.skolemAttributes = skolemAttributes;
    }

    /**
     * This function returns the skolem attributes associated with the group
     * operator
     *
     * @return The skolem attributes associated with the operator
     */
    public skolem getSkolemAttributes() {
        return skolemAttributes;
    }
}
