/**********************************************************************
  $Id: CUtil.java,v 1.9 2003/09/26 18:12:38 vpapad Exp $


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

import org.w3c.dom.*;
import java.io.*;
import java.net.*;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import niagara.ndom.*;

/**
 *  The CUtil class has some static methods for doing utility stuff
 *  to trees, such as pruning the empty node that are present in 
 *  trees produced by the old parser.  Call the functions here like:
 *  <pre>
 *       CUtil.pruneEmptyNodes(Element);
 *  
 *  </pre>
 *
 *  To remove the empty "\n" nodes.
 */
public class CUtil {
    /**
     *  Parse and return an XML document given a url
     *
     *  @param url the document to parse
     *  @return the parsed XML document or null
     */
    public static Document parseXML(String url) 
	throws ParseException {

        niagara.ndom.DOMParser parser;
        if (niagara.connection_server.NiagraServer.usingSAXDOM())
            parser = DOMFactory.newParser("saxdom");
        else
            parser = DOMFactory.newParser();

        boolean exceptionCaught = false;

        try {
            // Parse from a file stream
            // 
            if (url.indexOf(":") < 0) {    
                FileInputStream inStream = new FileInputStream(url);
                parser.parse(new InputSource(inStream));
            }
            
            // Parse from a URL stream
            //
            else {
                URL aurl = new URL(url);
                InputStream inStream = aurl.openStream();
                parser.parse(new InputSource(inStream));
            }
        } catch (SAXException se) {
            System.err.println("A SAXException occured during parsing of " + url + " Message was" + se.getMessage());
            exceptionCaught = true;
        } catch (FileNotFoundException fnfe) {
	    System.err.println("File not found exception: " + url + "Message: " + fnfe.getMessage());
	    exceptionCaught = true;
	} catch (MalformedURLException mue) {
	    System.err.println("MalformedURLException: " + url + "Message: " + mue.getMessage());
	    exceptionCaught = true;
	} catch (IOException ioe) {
	    System.err.println("IOException: " + url + "Message: " + ioe.getMessage());
	    exceptionCaught = true;
	} 

	// Throw exception if parse failed
	//
	if (parser.hasErrors() || parser.hasWarnings() || exceptionCaught) {
	    throw new ParseException("Error parsing xml file: " + url);
	}

	Document d = parser.getDocument();
	if(d ==  null) {
          System.err.println("Got null doc in CUtil:parseXML");
	}
        return d;
    }

    
    public static void genTab(int depth) {
		for(int i= 0; i<depth; i++)
			System.out.print("\t");
	}
}






