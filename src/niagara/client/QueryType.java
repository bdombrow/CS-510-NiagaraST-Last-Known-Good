
/**********************************************************************
  $Id: QueryType.java,v 1.8 2007/05/17 21:13:22 tufte Exp $


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


package niagara.client;

/**
 * Query type constants
 */

class QueryType
{
	public static final int NOTYPE = -1;
	//public static final int XMLQL = 1; no longer supported
	public static final int QP = 4; // assoc with QPQuery-created in query factor
  public static final int SYNCHRONOUS_QP = 5; // Tracingclient ONLY
  public static final int EXPLAIN = 6; // was EXPLAIN_QP // ok
  public static final int MQP = 7; // MQP and LightMQP client only
  public static final int PREPARE = 8; // was PREPARE_QP - ok
  public static final int EXECUTE_PREPARED = 9; // ok
  public static final int SET_TUNABLE = 10; // ok

  // merged RequestType with QueryType
  // types from Request type were:
  //RUN, think -> QP
  //EXPLAIN,
  //PREPARE,
  //EXECUTE_PREPARED,
  //SET_TUNABLE

}
