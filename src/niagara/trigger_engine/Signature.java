
/**********************************************************************
  $Id: Signature.java,v 1.1 2000/05/30 21:03:29 tufte Exp $


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
 * The class <code>Signature</code> is the abstract class for signature. 
 * 
 * @version 1.0
 *
 * @see logNode 
 */
import niagara.xmlql_parser.op_tree.*;
          
abstract class Signature {
    protected String plan; //the ASCII plan
    protected logNode root; //root of the plan
    protected int groupId;  //group id

    //public abstract String addMember(logNode node, GroupQueryOptimizer gOpt);

    Signature() {};
   
    /**
     * This function returns the plan root 
     *
     * @return the root of the signature 
     */
    public logNode getRootNode() {
        return root; 
    }

    /**
     * This function return the group id
     *
     */
    public int getGroupId() {
        return groupId;
    }

    /**
     * @return  the signature string
     *
     */
    public String getPlanString() {
        return plan;
    } 

}





