
/**********************************************************************
  $Id: UrlQueue.java,v 1.2 2001/08/08 21:25:48 tufte Exp $


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

import java.io.*;
import java.net.*;

import java.util.Vector;
import org.w3c.dom.*;

import niagara.trigger_engine.*;
import niagara.query_engine.*;
import niagara.utils.*;
import niagara.data_manager.XMLDiff.*;

class UrlQueue {
    private SynchronizedQueue urlq;

    UrlQueue(int capacity) {
	super();
	urlq = new SynchronizedQueue(capacity);
    }

    public void addUrl(UrlQueueElement url) {
	urlq.put(url, true);
    }

    public UrlQueueElement getUrl() {
	return ( (UrlQueueElement) urlq.get() );
    }
}
