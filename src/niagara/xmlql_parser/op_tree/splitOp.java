
/**********************************************************************
  $Id: splitOp.java,v 1.1 2000/05/30 21:03:29 tufte Exp $


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
 * The class <code>splitOp</code> is the class for operator Split.
 * 
 * @version 1.0
 *
 * @see op 
 */
package niagara.xmlql_parser.op_tree;

import java.util.*;
import com.ibm.xml.parser.*;
import org.w3c.dom.*;
import niagara.xmlql_parser.syntax_tree.*;

public class splitOp extends unryOp {

    //We have two kinds of splitOp, one is for multiple destinations(select
    // group), another is for fixed destination (join group). The name is
    // used to store the fiexd destination. 
    String destFileName=null;  

    private int tmpid;
    private Hashtable mappingTbl;
            
    public splitOp(Class [] al) {
	super(new String("Split"), al);
        // tmpid = -1;
    }

    //set the destination 
    public void setDestFileName(String fileName) {
        destFileName = fileName;
    }

    //get the destination 
    public String getDestFileName() {
        return destFileName;
    }

    public void initMappingTbl() {
        mappingTbl = new Hashtable();
    }

    public int allocCh(logNode parent){
	Integer tmpId=new Integer(++tmpid);
        mappingTbl.put(new Integer(parent.getId()),tmpId);
	return tmpid;
     };

    public int getCh(logNode parent){
        // Object o = mappingTbl.get(parent);
        // if(o==null) {
            Integer tmpint = new Integer(parent.getId());
            Object o = mappingTbl.get(tmpint);
        // }
        return ((Integer)o).intValue();
     };

    public void removeCh(logNode parent){
        if(mappingTbl.remove(parent)==null) {
            Integer tmpint = new Integer(parent.getId());
            mappingTbl.remove(tmpint);
        }
     };

    public int getNumDestinationStreams(){
        return mappingTbl.size();
     };
}

