
/**********************************************************************
  $Id: CtrlFlags.java,v 1.1 2002/04/29 19:54:57 tufte Exp $


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


package niagara.utils;


/** 
 * CtrlFlags is a class just to keep track of names and values
 * of control messages which are passed up and downstream during
 * query processing.
 *
 * @version 1.0
 */


public class CtrlFlags {

    // flag values - upstream means message of that type can
    // travel upstream (from bottom of tree up to top)
    // see notes on meanings of these flags after end of class
    public final static int NULLFLAG = 0; 
    public final static int SHUTDOWN = 1; // upstream & downstream
    public final static int GET_PARTIAL = 2; // downstream only
    public final static int SYNCH_PARTIAL = 3; // upstream only
    public final static int END_PARTIAL = 4; // upstream only
    public final static int EOS = 5;   // upstream only

    // flag names - order MUST match order of values -
    // value (from above) is used as an index into the name
    // array. i.e. CtrlFlags.name[NULLFLAG] should return "NullFlag"
    public final static String[] name = 
	{ "NullFlag",
	  "Shutdown",
	  "GetPartial",
	  "SynchPartial",
	  "EndPartial",
	  "EndOfStream" };
    
}

// NULLFLAG - just a value to indicate no flag 
// SHUTDOWN - shutdown is irregular shutdown - due to operator error
//            or client request. When any operator detects a shutdown
//            it sends shutdown messages to all of its sink and
//            source operators
// GET_PARTIAL - get partial is initiated by the top of the query tree
//               or by the client and travels down to the bottom
//               of the operator tree. At the bottom of the tree, when
//               a get partial is received, a SYNCH_PARTIAL message
//               is sent up the tree
// SYNCH_PARTIAL - synch partial messages are sent by non-blocking
//                 operators to indicate they have synchronized
//                 on the current partial result request - that is
//                 they have received either END_PARTIAL or SYNCH_PARTIAL
//                 messages from all of their source streams and have
//                 processed all tuples appearing in the stream before
//                 the SYNCH_PARTIAL and END_PARTIAL messages and
//                 output any corresponding results. SYNCH_PARTIAL
//                 is like an END_PARTIAL for non-blocking operators
// END_PARTIAL - END_PARTIAL messages are output by blocking operators
//               to indicate that they have output all tuples in the
//               current partial result request.
// EOS - EOS is used for normal shutdown due to end of stream. When
//       an operator completes processing, it puts an EOS into the
//       stream to indicate it is done.
