
/**********************************************************************
  $Id: JoinSig.java,v 1.2 2002/05/23 06:31:59 vpapad Exp $


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
 * The class <code>JoinSig</code> is the class for represting 
 * Join signature. It derives from Signature class.
 * 
 * @version 1.0
 *
 *
 * @see Signature 
 */

import java.io.*;
import java.lang.*;
import java.util.*;
import niagara.xmlql_parser.op_tree.*;

public class JoinSig extends Signature {

    String destFileName;

    JoinSig(logNode node, String plan, TriggerManager tm, GroupQueryOptimizer gOpt) {
	//set the plan
	this.plan = plan;

	//get an uniqe id for group
        groupId = tm.getNextId();

        //create the signature logical plan
        //create a split node, note this split node equls a store node.
        splitOp topOp = new splitOp();
        destFileName = gOpt.getTrigFileMgr().getTmpFileName(groupId,"Join");
        topOp.setDestFileName(destFileName);
        root = new logNode(topOp,node);
    }

    /**
     * add a member to this Join group
     * return the destination file name for current simple join group
     * @return String tmp file name
     **/
    public String addMember(logNode node, GroupQueryOptimizer gOpt) {
	return destFileName;
    }
}
