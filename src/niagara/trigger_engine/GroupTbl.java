
/**********************************************************************
  $Id: GroupTbl.java,v 1.2 2001/08/08 21:29:02 tufte Exp $


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
 * The class <code>GroupTbl</code> is the class for storing signatures. 
 * A signature is a part of plan which can be shared and reused by lots
 * of similar plans. 
 * 
 * @version 1.0
 *
 * @see Signature
 */
import org.w3c.dom.*;
import java.io.*;
import java.lang.*;
import java.util.*;

public class GroupTbl {

    private Hashtable groupTbl;

    /**
     * constructor
     * @param 
     **/
    GroupTbl() {
        groupTbl = new Hashtable();
    }


    /**
     * add a new signature into the group table.
     * @param Signature
     * 
     **/
     public void addSignature(Signature newSig) {
	 
	 groupTbl.put(newSig.getPlanString(), newSig);
	 // debug.mesg(""+groupTbl.size());
	 // debug.mesg(""+groupTbl.toString());
     };

    /**
     * find whether a possible signature existed in the group table.
     * @param signature logical plan
     * @return the signature in the signature table
     **/
     public Signature findSignature(String planString) {
         return (Signature)groupTbl.get(planString);
     };
    
        
    /**
     * remove a signature from the group table. It does nothing currently.
     * @param the signature index
     * @return 1 succeed, otherwise 0
     * 
     **/
     public int rmvSignature(int sigIndex) {
	 return 1;
     };
}

