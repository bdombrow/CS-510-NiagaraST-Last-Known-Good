
/**********************************************************************
  $Id: GroupOpAllocTbl.java,v 1.1 2000/05/30 21:03:29 tufte Exp $


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


package niagara.trigger_engine;

/**
 * The class <code>GroupOpAllocTbl</code> is the class for storing the
 * mapping of logical group operators including Duplicate and Split
 * to corresponding physical operators. It is used to avoid allocating
 * physical operators for those group logical operators again if they
 * are already allocated. 
 * 
 * @version 1.0
 *
 *
 */

import java.io.*;
import java.lang.*;
import java.util.*;
import niagara.query_engine.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;

public class GroupOpAllocTbl {

    private static Hashtable allocTbl;
    
    /**
     * constructor
     * @param 
     **/
    GroupOpAllocTbl() {
	allocTbl=new Hashtable();
    };

    /**
     * add a new logical group operator into the allocation table.
     * @param logical operator and physical operator 
     * 
     **/
     public void addPhyOp(op logOp, PhysicalOperator phyOp) {
	allocTbl.put(logOp,phyOp);
     };

    /**
     * find whether a possible logical operator existed in the alloc table.
     * @param logical operator 
     * @return the physical operator in the signature table
     **/
     public PhysicalOperator findPhyOp(op logOp) {

        return ((PhysicalOperator)(allocTbl.get(logOp)));
       
     };
    
    
    /**
     * remove a physical op from the alloc table.
     * @param the logical op 
     * @return 1 succeed, otherwise 0
     **/
     public int rmvPhyOp(op logOp) {
	 return 1;
     };
}

