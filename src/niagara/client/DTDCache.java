
/**********************************************************************
  $Id: DTDCache.java,v 1.1 2000/05/30 21:03:24 tufte Exp $


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

import java.util.*;

/**
 * this class caches dtds in a map [dtdUrl -> dtdText]
 */

class DTDCache
{
	// request ids for the dtds
	private int gid = 0;

	/**
	 * the dtd map [dtdUrl -> dtdText]
	 */
	private Map dtdMap = new HashMap(10);

	/**
	 * map of id's to dtds
	 */
	private Map idMap = new HashMap(10);

	public DTDCache()
		{}
	
	public int getNextID()
		{
			return gid++;
		}

	public synchronized String getDTD(String dtdURL)
		{
			String s = (String)(dtdMap.get(dtdURL));
			return s;
		}

	public synchronized int registerDTD(String dtdURL)
		{
			int id = getNextID();
			idMap.put(new Integer(id), dtdURL);
			return id;
		}

	public synchronized void putDTD(int id, String dtd)
		{
			String dtdURL = (String)(idMap.remove(new Integer(id)));
			dtdMap.put(dtdURL, dtd);
		}
	
	public synchronized boolean hasDTDArrived(int id)
		{
			return !(idMap.keySet().contains(new Integer(id)));
		}

	public String toString()
		{
			return "[id->url] " + idMap + "\n[url->dtd] " + dtdMap;
		}
}
