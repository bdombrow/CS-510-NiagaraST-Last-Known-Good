
/**********************************************************************
  $Id: DTDDirEntry.java,v 1.2 2000/08/09 23:53:52 tufte Exp $


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

import com.ibm.xml.parser.*;
import org.w3c.dom.*;
import java.io.*;
import java.net.URL;
import java.util.Vector;
import java.util.Hashtable;

import niagara.trigger_engine.*;
import niagara.query_engine.*;
import niagara.utils.*;
import niagara.data_manager.XMLDiff.*;
/**
 *  The DTDEntry class implements an entry to store information about a DTD
 *  in the DTDDirectory.  Information stored includes the DTD name, the DTD Id
 *  issued by the directory, a vector of URLs conforming to that DTD, flags to 
 *  indicate if this dtd is local and/or cached.
 *  The tmpFile name for the cached
 *  file, the parsed DTD, and the size of the cached file.
 * 
 *  @version
 */

public class DTDDirEntry implements Serializable{

    // The URL/file name for the DTD
    //
    private String URLName;

    // The Id of a DTD, 1:1 with DTDName
    //
    private int DTDId;
    
    // Whether this DTD has any predicates
    // might later put real predicates here
    //
    private boolean hasPredicates;

    // Vector of all URLs for documents conforming this DTD
    // Note for XML, URL serves as key
    //
    private Vector XMLURLVector;    

    // Always cache a DTD if remote
    //
    private boolean cached;
    
    // Whether DTD is in local file system
    //
    private boolean local;
    
    // If cached from URL, the temp file name for the cached copy
    //
    private String cachedFileName;

    // The local cached file 
    //
    private File cachedFile;

    // The size of the local cached file
    //
    private long fileSize;
    
    // The parsed DTD
    //
    private DTD dtd;
    

    /**
     * constructs a DTDEntry object
     *
     * @param url   URL/file name of the DTD 
     * @param id    the id of the DTD, 1:1 on name
     */
    public DTDDirEntry(String dtddir, String url, int id)
    {
        
	// Convert a URL to a ??local pathname
	//
        String tmp = DMUtil.URLToPathname(url);

        cached = false;

	// Mark if name is a file in local file system or remote
	//
        if ((url.indexOf(':') == -1)||(url.indexOf("file:/")==0)) {
            local = true;
        }
        else {
            local = false;
        }
        
        URLName = url;
	
        cachedFileName = dtddir+"/"+tmp;
        cachedFile = new File(cachedFileName);
        DTDId = id;
        dtd = null;
        
        hasPredicates = false;

        XMLURLVector = new Vector();

	// If not local, need to create a local temp file and read from URL
	//
        if (!local) {
	    int lastInd = cachedFileName.lastIndexOf('/');
	    File dir    = new File(cachedFileName.substring(0, lastInd));

            try {
                if (!dir.mkdirs()) {
                    //System.out.println("second time from same dir");
                }
                if (!cachedFile.createNewFile()) {
                    //persist directory
                    //if persist, won't see this line anymore
                    //
                    System.err.println("file of name: "+tmp+" exist");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }  

        
    }// of XMLEntry
    

    /**
     *  Read the DTDcache and parse DTD
     *  not tested, since parseDTD is not right yet
     *
     * @return a the parsed DTD object for this DTD entry, null if error
     */
    public DTD getDTD() 
	throws ParseException, java.net.MalformedURLException, 
	       java.io.FileNotFoundException, java.io.IOException {
        // return a clone if the DTD is already parsed
        //
        if( dtd != null ) {
            return (DTD)dtd.clone();
        }
        
        // If the dtd is local, parse and return
        //
        if ( local ) {
            dtd = CUtil.parseDTD(URLName);
	    if (dtd != null) {
               return (DTD)dtd.clone();
            }
	    return null;
        }
        
	// If ! local and not cached, 
        // read the DTD from remote site and cache
	//
        if (!cached) {
            readDTD();
        }
        
        // Should be cached after read if read succeeds
        // and then parse from local copy
        //
        if (cached) {
            dtd = CUtil.parseDTD(cachedFileName);
	    if (dtd != null) {
		return (DTD)dtd.clone();
            }
	    return null;
        }
        
        System.err.println("readDTD() failed");
        return null;
    }
    
           
    /**
     * check to see if a DTD is cached 
     *
     * @param urlName the URL of the DTD
     * @return a boolean indicating result
     */
    public boolean lookUpDTD(String urlName)
    {
        if (URLName.compareTo(urlName) == 0) {
            return true;
        }
        else{
	    return false;
	}
    }


    /**
     *  add the URLs for a conforming XML document
     *  tested
     * 
     *  @param URLList the list URL of XML document
     *            to be associated with this DTD
     *
     */
    public void addXMLList(String[] URLList) 
    {
        for (int i = 0; i < URLList.length; i++) {
            XMLURLVector.addElement(URLList[i]);
        }
    }


    /**
     *  Add the URL for a conforming XML document
     *  tested
     * 
     *  @param URL the URL of XML document
     *            to be associated with this DTD
     */
    public void addXML(String URL) 
    {
        XMLURLVector.addElement(URL);
    }
    

    /**
     * remove the URLs for a conforming XML document
     * tested
     *
     * @param URLList the list of URL of XML document 
     *                to be associated with this DTD
     */
    public void removeXMLList(String[] URLList)
    {
        for (int i = 0; i < URLList.length; i++) {
            XMLURLVector.removeElement(URLList[i]);
        }
    }


    /**
     * remove the URL for a conforming XML document
     * tested
     *
     * @param URL the URL of XML document to be associated with this DTD
     */
    public void removeXML(String URL)
    {
        XMLURLVector.removeElement(URL);
    }
    

    /**
     *  Fetch the DTD from a remote site if it is not local.
     *  Cache the DTD as File cachedFileName.  Update DTD entry fields 
     *  accordingly
     *  tested
     *
     *  @return a boolean indicating whether the file is now local
     */
    public boolean readDTD() 
    {
	
        if (local)
	    return true;
	    
	try {
	    
	    // Set up output file and the URL input stream
	    //
	    FileWriter outputFile = new FileWriter(cachedFileName);
	    URL tmpURL = new URL(URLName);
	    String line;
	    
	    BufferedReader inputStream
		= new BufferedReader(
			     new InputStreamReader(tmpURL.openStream()));
	    
	    // Read the URL stream and write to output file line at a time
	    //
	    while(inputStream.ready()) {
		line = inputStream.readLine();
		outputFile.write(line+"\n");
	    }
	    
	    // Close URL stream and output file
	    //
	    inputStream.close();
	    outputFile.close();
	    
	    // The URL is now cached locally in 'cachedFileName'
	    //
	    cached = true;
	    
	}catch  (IOException e) {
	    e.printStackTrace();
	    return false;
	}

	// Set DTD size using the File cachedFile, a wrapper for cachedFileName
	//
	fileSize = cachedFile.length();
	//System.out.println("filesize after read "+fileSize);
	
	return true;
    }
    

    /**
     *  Return the dtd name
     *  @return String the dtd name
     */
    public String getDtdName()
    {
	return URLName;
    }

    /**
     *  Return the dtd id
     *  @return int the dtd name
     */
    public int getDtdId()
    {
	return DTDId;
    }
            
        
    /**
     * string representation of the DTDEntry
     * tested
     *
     * @return a string containing the info
     */
    public String toString() 
    {
        String entryInfo = 
	    "\n  -------------------------------------------\n" +
	    "                DTD Entry\n" +
	    "  -------------------------------------------\n" +
            "  URLName        " + URLName       + "\n" +
            "  DTDId          " + DTDId         + "\n" +
            "  cached         " + cached        + "\n" +
            "  local          " + local         + "\n" +
            "  cachedFileName " + cachedFileName   + "\n" +
            "  fileSize       " + fileSize      + "\n" +
            "  XMLURLVector   " + "\n    "  +
            XMLURLVector.toString() +
            "\n  ------------------------------------------\n";
        
        return entryInfo;
    }

    private void writeObject(ObjectOutputStream out)
        throws IOException
    {
        try {
            out.writeObject (URLName);
            out.writeInt    (DTDId);
            out.writeBoolean(hasPredicates);
            out.writeObject (XMLURLVector);
            out.writeBoolean(cached);
            out.writeBoolean(local);
            out.writeObject (cachedFileName);
            out.writeObject (cachedFile);
            out.writeLong   (fileSize);
            dtd    = null;
            //dtd not serializable

        }
        catch (IOException ioe) {
            throw ioe;
        }
    }


    private void readObject(ObjectInputStream in)
	throws IOException, ClassNotFoundException
    {
	try {
	    URLName       = (String)in.readObject();
	    DTDId         =         in.readInt();
	    hasPredicates =         in.readBoolean();
	    XMLURLVector  = (Vector)in.readObject();
	    cached        =         in.readBoolean();
	    local         =         in.readBoolean();
            cachedFileName= (String)in.readObject();
	    cachedFile    =  (File) in.readObject();
	    fileSize      =         in.readLong();
	    dtd           = null;
	}
	catch (IOException ioe) {
	    throw ioe;
	}
	catch (ClassNotFoundException cnfe){
	    throw cnfe;
	}
    }
}



