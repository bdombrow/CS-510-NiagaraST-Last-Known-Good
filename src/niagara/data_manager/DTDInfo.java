
/**********************************************************************
  $Id: DTDInfo.java,v 1.1 2000/05/30 21:03:26 tufte Exp $


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


package niagara.data_manager;

import java.util.Vector;

import niagara.trigger_engine.*;
import niagara.query_engine.*;
import niagara.data_manager.XMLDiff.*;
/**
 *  The DTDInfo class is used to associate stats and urls sets with a DTD.
 *  This is just a skelton implementation for now to define the interface.
 *  This should be much easier to use than parsing the XML doc in the 
 *  Exec. Engine.
 */

public class DTDInfo{
    
    // The dtdid: url where dtd was found
    //
    private String dtdid;
    
    // The ulr list associated with this dtd
    //
    private Vector urls;
    
    // The stats for this dtd
    //
    private DTDStats stats;
    

    /**
     *  DTDInfo constructor.  Initialize the dtdid, url vector, and stats.
     *
     */
    DTDInfo(String dtdid)
    {
	this.dtdid = dtdid;
	urls = new Vector();
	stats = null;
    }
    
    DTDInfo() {
	this.dtdid = null;
	urls = new Vector();
	stats = null;
    }

    public DTDStats getDTDStats()
    {
	return stats;
    }

    public Vector getURLs()
    {
	return urls;
    }
    
    public String getDTDId()
    {
	return dtdid;
    }

    public void addURL(String url)
    {
	urls.addElement(url);
    }

    public void addStats(DTDStats stats)
    {
	this.stats = stats;
    }
}


/**
 *  This class must be defined at some point, just a placeholder for now
 *
 *
 */
class DTDStats
{ }


