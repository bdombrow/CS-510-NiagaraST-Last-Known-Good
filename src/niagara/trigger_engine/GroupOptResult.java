
/**********************************************************************
  $Id: GroupOptResult.java,v 1.1 2000/05/30 21:03:29 tufte Exp $


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
 * The class <code>GroupOptResult</code> is the class for storing the result
 * of group optimization for a new installed continuous query. 

 * @version 1.0
 *
 * @see GroupQueryOptimizer
 */
import java.util.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;

public class GroupOptResult {

    private Integer trigId; //trigger id
    private logNode trigRoot; //root node
    private Vector srcFileNames; //source file names

    /**
     * constructor
     * @param 
     **/
    GroupOptResult(Integer tid, logNode root, Vector fileNames) {
        trigId = tid;
	trigRoot = root;
	srcFileNames = fileNames;
    }

    /**
     * get trigger id
     * @return trigId
     **/
     public Integer getTrigId() {
	 return trigId;
     };

    /**
     * get trigger root
     * @return trigRoot
     **/
     public logNode getTrigRoot() {
	 return trigRoot;
     };

    /**
     * get source file names
     * @return source file Names
     **/
     public Vector getSrcFileNames() {
	 return srcFileNames;
     };
}

