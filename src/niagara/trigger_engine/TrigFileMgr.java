
/**********************************************************************
  $Id: TrigFileMgr.java,v 1.1 2000/05/30 21:03:29 tufte Exp $


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
 * The class <code>TrigFileMgr</code> is the class for managing 
 * the const table files. 
 * 
 * @version 1.0
 *
 *
 * @see selectSig 
 */

import java.io.*;
import java.util.*;
import com.ibm.xml.parser.*;
import org.w3c.dom.*;
import niagara.data_manager.*;

class TrigFileMgr {

    DataManager DM;
    
    TrigFileMgr(DataManager DM) {
	this.DM=DM;
    }

    /**
     * allocate a system unique file name 
     * 
     * @param id trigger id
     * @param val string 
     * @return the tmp file name
    **/    
    public String getTmpFileName(int id, String val) {
	String s="CACHE/TRIG_"+id+"_"+val+".xml";
	return s;
    }   


    /* return the const table name */
    private String getConstTblName(int id) {
        return new String("SYS/constTbl"+id+".xml");
    }

    //traverse the const table dom tree to find whether a value is already
    //existed, if yes, return the tmpFileName, otherwise, return null
    private String findTmpFileName(Element node, String val) {
	// do what you want with this node here...
	//debug.mesg("&&&&TrigFileMgr: Finding String "+val);

	
	if (node.hasChildNodes()) {
	    NodeList nl = node.getChildNodes();
	    int size = nl.getLength();
	    for (int i = 0; i < size; i++) {
		System.out.println("&&&&"+((Text)nl.item(i).getFirstChild().getFirstChild()).getData());
		if (val.equals(((Text)nl.item(i).getFirstChild().getFirstChild()).getData())) {
		    return ((Text)nl.item(i).getLastChild().getFirstChild()).getData();
		}
	    }
	}

	return null;
    }

    /**
     * This function allocates a new entry in the constant table 
     * for val, if not existing. Otherwise, the tmp file name associated
     * with the value of val in the constant table will be returned.
     * 
     * @param id group trigger id
     * @param val constant value
     * @return the tmp file name
    **/    
    public String insertConst(int groupId, String val) throws IOException {
	
	String tmpFileName;

	//get the constant table name for the group trigger id
	String fname = getConstTblName(groupId);
        //System.err.println("Will use " + fname + " to store constTbl");

        InputStream is;
        TXDocument doc;
        doc =(TXDocument) DM.createDoc(fname);
        Element root = doc.getDocumentElement();
	if(root==null) {
	    root = doc.createElement("pairs");
            doc.appendChild(root);
        }
        
	//needs to check here if the pair with the "value" is
	//already exists, if yes, do nothing. else, insert this
	//pair

	tmpFileName = findTmpFileName(root, val);

	if (tmpFileName!=null)
	    return tmpFileName;

	//new value, we need to create an entry in the const table
        TXElement tmp = new TXElement("pair");
        TXElement tmpv = new TXElement("value");
        TXElement tmpc = new TXElement("tmpFileName");

        tmpv.appendChild(new TXText(val));

	tmpFileName = getTmpFileName(groupId,val);
        tmpc.appendChild(new TXText(""+tmpFileName));

        tmp.appendChild(tmpv);
        tmp.appendChild(tmpc);

        root.appendChild(tmp);

	DM.modifyDoc(fname, doc);
	DM.flushDoc(fname); //debug purpose
	return tmpFileName;
    }

}
