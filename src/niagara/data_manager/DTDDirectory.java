
/**********************************************************************
  $Id: DTDDirectory.java,v 1.4 2003/03/08 01:01:53 vpapad Exp $


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

/**
 * DTDDirectory.java
 *
 * Created:        Fri Mar 29 13:15:56 1999
 * @version
 */

import org.w3c.dom.*;
import java.io.*;
import java.net.*;
import java.util.*;


/**
 * Directory for DTDs assume caching DTD parsing of DTD needs to be fixed
 *
 */
public class DTDDirectory implements Serializable
{

    // The last issued dtdid  (how do we deal with wrapping?)
    //
    private int maxDTDId;

    // The directory where all cached DTDs and documents will be stored
    //
    private String DTDDir;
    
    // Hashtable mapping dtd to dtdid
    //
    private Hashtable  DTDTable;
   
    // Vector of DTD entries indexed by dtdid
    //
    private Vector     DTDVector;
   

    /**
     * Construct an DTDDirectory 
     *
     * @param path directory to store temp files
     */
    public DTDDirectory(String path) 
    {
	// Initialize data members of the DTDDirectory and allocate table/vector
	//
        maxDTDId   = 0;
        DTDDir     = path;                     //ends with / for directory
        DTDTable   = new Hashtable();
        DTDVector  = new Vector();

	// If persistent versions of DTDTable and DTDVector exist, read them.
	//
	
    }


    /**
     *  Look up the id of a DTD.  If the dtd doesn't exist in directory, return -1
     *  tested
     *
     *  @param DTDUrl the url of the dtd we are looking up
     *  @return int the id of dtd or -1 if no such dtd exists
     */
    public int lookUp(String DTDUrl) 
    {
	// Lookup
	//
        Integer index = (Integer)DTDTable.get(DTDUrl);
        
        if (index != null) {
            return index.intValue();
        }
        
        return -1;
    }

    
    /**
     *  Add an entry for a DTD.  Return the newly issued DTDid.
     *  If an Entry exists for the dtd we are trying to add, return the 
     *  dtdid for that entry
     *  [tested]
     *
     * @param dtdURL the URL of the DTD, can be file name
     * @return the id of the DTD
     */
    public int addDTD(String DTDUrl) 
    {

	// If DTD exsisits, return the id of the existing dtd
	// Or should we return an error ???
	//
        int dtdid = lookUp(DTDUrl);
	if(dtdid != -1){
	    return dtdid;
	}

        // assume , DTD doesn't exist in DTD Dir
	// Not now, we need to deal with cases when an add is 
	// attempted for existing dtd

        int DTDId  = maxDTDId;
        DTDDirEntry entry = new DTDDirEntry(DTDDir, DTDUrl, DTDId);
   
	// Increment maxDTDId ( should this be a synchronized operation?? )
	//
        maxDTDId ++;

	// Read the DTD, if read fails, return error
	//
        if(!entry.readDTD() ){
	    System.err.println("unable to read dtd: "+entry.getDtdName());
	    return -1;
	}

        //may want to cache and parse DTD
        //entry.getDTD();

        DTDVector.insertElementAt(entry, DTDId);
        DTDTable.put(DTDUrl, new Integer(DTDId));

        return DTDId;    
    }
 

    /**
     * get the parsed dtd
     *
     * @param DTDURL the url of the DTD
     * @return the parsed DTD
     */
    public DocumentType getDTD(String DTDURL) 
    throws niagara.utils.ParseException, IOException, MalformedURLException {
        int dtdid = addDTD(DTDURL);
        DTDDirEntry entry = (DTDDirEntry)DTDVector.elementAt(dtdid);

        return entry.getDTD();
    }
    
        
    /**
     * remove a DTD from the directory
     *
     * @param DTDURL the url of the DTD to be removed
     * @return a boolean indicating status
     */
    public boolean removeDTD(String DTDURL) 
    {
        System.out.println("DTDDirectory.removeDTD: not implemented yet");
        
        return true;
    }
    
       
    /**
     *  Add an XML document to be associated with a DTD
     *
     *  @param dtdId the id of DTD as returned by addDTD
     *  @param xmlURL URL of XML to add
     *  @return a boolean indicating status
     *  @see addDTD
     */
    public boolean addXML(int DTDId, String XMLURL)
    {
        if (DTDId > DTDVector.size() ) {
            System.out.println("no such DTD stored ");
            return false;
        }
        
        ((DTDDirEntry)DTDVector.elementAt(DTDId)).addXML(XMLURL);
        return true;
        
    }


    /**
     *  Add a list of XML documents to be associated with a DTD
     *
     *  @param DTDId the id of DTD as returned by addDTD
     *  @param XMLURLList list of URL of XML to add
     *  @return a boolean indicating status
     *  @see addDTD
     */
    public boolean addXMLList(int DTDId, String[] XMLURLList)
    {
        if (DTDId > DTDVector.size() ) {
            System.out.println("no such DTD stored ");
            return false;
        }
        
        ((DTDDirEntry)DTDVector.elementAt(DTDId)).addXMLList(XMLURLList);
        
        return true;
        
    }



    /**
     *  Remove a list of XML documents associated with a DTD
     *
     *  @param DTDId the id of DTD as returned by addDTD
     *  @param XMLURLList list of URL of XML to remove
     *  @return a boolean indicating status
     *  @see removeDTD
     */
    public boolean removeXMLList(int DTDId, String[] XMLURLList)
    {
        if (DTDId > DTDVector.size() ) {
            System.out.println("no such DTD stored ");
            return false;
        }
        
        ((DTDDirEntry)DTDVector.elementAt(DTDId)).removeXMLList(XMLURLList);
        return true;
        
    }


    /**
     *  Remove an XML document associated with a DTD
     *
     *  @param DTDId the id of DTD as returned by addDTD
     *  @param XMLURL URL of XML to remove
     *  @return a boolean indicating status
     *  @see removeDTD
     */
    public boolean removeXML(int DTDId, String XMLURL)
    {
        if (DTDId > DTDVector.size() ) {
            System.out.println("no such DTD stored ");
            return false;
        }
        
        ((DTDDirEntry)DTDVector.elementAt(DTDId)).removeXML(XMLURL);
        return true;
        
    }

   
    /**
     *  Return a string representation of the DTDDirectory
     *
     *  @return the string representation
     */
    public String toString() 
    {
        String tmpStr =
	    "+------------------------------------------------------------+\n" +
	    "|                 DTD directory contents:                    |\n" +
	    "+------------------------------------------------------------+\n" +
            "| maxDTDId         " + maxDTDId  + "\n" +
            "| DTDDir           " + DTDDir    + "\n" +
            "| DTDTable:        " + "\n|   " +
              DTDTable.toString() + "\n" +
            "| DTDVector:       " + "\n|   " +
              DTDVector.toString()+ "\n" +
            "+------------------------------------------------------------+\n";
        
        return tmpStr;
    }
         

    private void writeObject(ObjectOutputStream out)
        throws IOException
    {
        try {
            out.writeInt(maxDTDId);
            out.writeObject(DTDDir);
            out.writeObject(DTDTable);
            out.writeObject(DTDVector);
        }
        catch (IOException ioe) {
            throw ioe;
        }
    }


    private void readObject(ObjectInputStream in)
        throws IOException, ClassNotFoundException
    {
        try {
            maxDTDId        =            in.readInt();
            DTDDir          =    (String)in.readObject();
            DTDTable        = (Hashtable)in.readObject();
            DTDVector       =    (Vector)in.readObject();
        }
        catch (IOException ioe) {
            throw ioe;
        }
        catch (ClassNotFoundException cnfe){
            throw cnfe;
        }
    }
}
