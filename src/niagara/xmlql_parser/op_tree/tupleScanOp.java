
/**********************************************************************
  $Id: tupleScanOp.java,v 1.2 2000/08/09 23:54:20 tufte Exp $


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
 * The class <code>tupleScanOp</code> is used to read the tmp files written
 * out in tuple format.  It is very similar to dtdScanOp.
 * 
 * @version 1.0
 *
 * @see dtdScanOp
 */
package niagara.xmlql_parser.op_tree;

import java.util.*;
import com.ibm.xml.parser.*;
import org.w3c.dom.*;

public class tupleScanOp extends unryOp {

   private Vector docs;

   public tupleScanOp(Class[] al) {
	super(new String("TupleScan"), al);
   }

   public Vector getDocs() {
	return docs;
   }

    /**
     * This function sets the vector of documents associated with the
     * dtd scan operator
     *
     * @param docVector The set of documents associated with the operator
     */

    public void setDocs (Vector docVector) {

	docs = docVector;
    }

    public void dump() {
	System.out.println("TupleScanOp");
    }

   
}
