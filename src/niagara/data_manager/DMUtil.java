
/**********************************************************************
  $Id: DMUtil.java,v 1.3 2003/03/08 01:01:53 vpapad Exp $


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

import java.util.*;
import org.w3c.dom.*;

/**
 *  DMUtil provides utility functions for the data manager
 *  
 *  @see DataManager
 *  @version
 */
public class DMUtil  
{

    /**
     *  translate a http to a unique path name in local file system
     *  the purpose to to cache related xml and dtd in same dir 
     *  for future parsing
     * 
     *  the last component would be used as the file name, 
     *  everything before that would be encoded into a directory name
     *  under current directory. this directory name would
     *  help grouping the 
     *
     *  @param url the url to translate     
     *  @return the local path name for an url
     */
    final static String ITEM = "item";

    private static String httpToPathname(String url)
    {

        /**
         * an URL like
         * http://www.cs.wisc.edu/~rushan/xml/department.dtd
         * is transformed to a string
         * tmp+hashCode(http://www.cs.wisc.edu/~rushan/xml/)+"/"
         * +"department.dtd"
         *
         */

        int    lastSlash = url.lastIndexOf('/');
        
        String pathName = "tmp"+(url.substring(0, lastSlash)).hashCode();
        String fileName = url.substring(lastSlash);
        
        return pathName + fileName;

    }


    /**
     *  return the file name component of a non http URL
     *  for example, if url == file:/afs/cs.wisc.edu
     *  /afs/cs.wisc.edu would be returned
     * 
     *  @param url the non-http url
     *  @return a string of the file name component
     */
    private static String fileToPathname(String url)
    {
        //file:/afs/cs.wisc.edu

        int firstSlash = url.indexOf('/');
        return url.substring(firstSlash);

    }
    

    /**
     *  map a URL(http:// or file:/) to local file name
     *  (the local file name might contain a directory 
     *  component to encoding the http://... / that
     *  precedes the file name(last component)
     *
     *  @param url the url to be transformed
     *  @return the result local file name    
     */
    public static String URLToPathname(String url) 
    {
        String tmp;
        
        int  pos1 = url.indexOf(':');
        int  pos2 = url.indexOf("http:");
        int  pos3 = url.indexOf("file:");
        
        if (pos1 == -1) {
            //file name itself if not URL
            //here url serves as the full path name
            //while name is just the last component
            //
            tmp = url;
        }
        else {
            if (pos3 == 0) {
                //file: like
                //
                tmp = fileToPathname(url);
            }
            else {
                //http: like
                //
                tmp = httpToPathname(url);
            }            
        }
        
        return tmp;

    }
    



    /**
     * an utility function to parse the DTD info passed from YPServer 
     *
     * @param dtdInfoDoc the xml file that contains the result from YP
     * @return a Hashtable mapping DTDId to DTDInfo node
     * @see DTDInfo
     */

    public static DTDInfo parseDTDInfo(Document dtdInfoDoc) {
	if(dtdInfoDoc == null)
	    return null;
	
	NodeList urls = dtdInfoDoc.getElementsByTagName(ITEM);
	int numUrls = urls.getLength();

	// Init a new DTDInfo obj
	//
	DTDInfo curDTDInfo = new DTDInfo();
	    
	    
	// Get all urls and stats associated with this dtd
	//
	for(int j = 0; j < numUrls; j++){
	    String nodevalue = urls.item(j).getFirstChild().getNodeValue();

	    curDTDInfo.addURL(nodevalue);
	}
	    
	return curDTDInfo;
    }


    public static Vector parseDTDList(Document dtdListDoc) {
	if(dtdListDoc == null)
	    return null;
	
	NodeList dtds = dtdListDoc.getElementsByTagName(ITEM);
	int numDTDs = dtds.getLength();

	Vector v = new Vector();
	    
	for(int j = 0; j < numDTDs; j++){
	    String nodevalue = dtds.item(j).getFirstChild().getNodeValue();

	    v.addElement(nodevalue);
	}
	    
	return v;
    }
}
