
/**********************************************************************
  $Id: CUtil.java,v 1.3 2000/08/21 00:50:37 vpapad Exp $


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

import com.ibm.xml.parser.*;
import org.w3c.dom.*;
import java.lang.*;
import java.io.*;
import java.net.*;
import java.util.*;
import com.ibm.xml.parser.util.*;

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
    
    private final static String PCDATA = "#PCDATA";
    private final static String ENCODING = "";
    private static int lastQueryId = 0;

    /**
     *  getNextQueryId() - Increments and returns the next unique queryId
     *  @return int - the next query id
     */
    public synchronized static int getNextQueryId()
    { 
	return (CUtil.lastQueryId++); 
    }

    /**
     *  Parse and return a DTD given a filename
     *
     */
    public static DTD parseDTD(String dtdURL) 
	throws ParseException, FileNotFoundException, IOException, MalformedURLException {
	
	// Set up a parser
	// 
        Parser p  = new Parser(dtdURL);
	DTD dtd = null;

	// Parse the DTD
	//

	// Parse from a file stream
	// 
	if (dtdURL.indexOf(":") < 0) {    
	    FileInputStream inStream = new FileInputStream(dtdURL);
	    dtd = p.readDTDStream( inStream );
	}
	
	// Parse from a URL stream
	//
	else {
	    URL aurl = new URL(dtdURL);
	    InputStream inStream = aurl.openStream();
	    dtd = p.readDTDStream( inStream );
	}
	
	// Throw exception if parse failed
	//
	if (p.getNumberOfWarnings()+p.getNumberOfErrors()>0){
	    System.out.println("Parse Error: "+dtdURL+" is not valid DTD file");
	    throw new ParseException("Error parsing DTD File: " +dtdURL);
	}
	return dtd;
    }
    


    /**
     *  Recursive print tree function to dump detailed DOM tree info
     *
     *  @param node the tree to print
     *  @param path path to reach this node (concats on each recursive call)
     */
    public static void printTree(Node node,String Path) {
        
	// Do stuff with this node here  ( Pre-order traversal )
	//
	Path+=node.getNodeName();
	String Value = node.getNodeValue();
	int NodeType =+ node.getNodeType();
	System.out.print(Path+": "+Value+" (");
	
	// Switch on the DOM node type
	//
	switch(NodeType) {
	case Node.ATTRIBUTE_NODE: System.out.println("ATTRIBUTE_NODE)"); break;
	case Node.CDATA_SECTION_NODE : System.out.println("CDATA_SECTION_NODE)"); break;
	case Node.COMMENT_NODE: System.out.println("COMMENT_NODE)"); break;
	case Node.DOCUMENT_FRAGMENT_NODE : System.out.println("DOCUMENT_FRAGMENT_NODE)"); break;
	case Node.DOCUMENT_NODE: System.out.println("DOCUMENT_NODE)"); break;
	case Node.DOCUMENT_TYPE_NODE : System.out.println("DOCUMENT_TYPE_NODE)"); break;
	case Node.ELEMENT_NODE : System.out.println("ELEMENT_NODE)");  break;
	case Node.ENTITY_NODE : System.out.println("ENTITY_NODE)"); break;
	case Node.ENTITY_REFERENCE_NODE : System.out.println("ENTITY_REFERENCE_NODE)"); break;
	case Node.NOTATION_NODE : System.out.println("NOTATION_NODE)"); break;
	case Node.PROCESSING_INSTRUCTION_NODE : System.out.println("PROCESSING_INSTRUCTION_NODE)"); break;
	case Node.TEXT_NODE : System.out.println("TEXT_NODE)"); break;
	default: System.out.println("unknown)"); break;
	}
	
	// Make recursive calls on children
	//
	if (node.hasChildNodes()) {
	    NodeList nl = node.getChildNodes();
	    int size = nl.getLength();
	    for (int i = 0; i < size; i++) {
		Node child=nl.item(i);
		printTree(child,Path+".");
	    }
	}
    }
    
    
    /**
     *  NOT TESTED or working??  
     *
     *  Result doc creation.  Takes an element, constructs a DTD, generates
     *  new doc with DTD and element node and returns this new doc.
     *
     *  @param root the tree use as the element node for the doc
     *  @return a newly constructed doc with root as that data element, and a DTD
     *          for root as the DTD element
     */
    public static TXDocument CreateDTD(Node root)
    {

	// Create a new doc and 
	//
	TXDocument doc = new TXDocument();
	DTD dtd = doc.createDTD(root.getNodeName(),null);
	doc.appendChild(dtd);
	doc.appendChild(root);
	
	// Collect all children names for each element
	//
	Hashtable ht = new Hashtable();
	traverseTree(root,ht);
	
	// Create new tag for this node
	//
	Enumeration names = ht.keys();
	
	// For each unique element name in the tree, create a content model
	//
	while(names.hasMoreElements()) {
	    String name = (String)names.nextElement();
	    Hashtable children = (Hashtable)ht.get(name);
	    if (children.size()==0) continue;   // don't create an entry for empty elements
	    
	    // Create content model for element
	    //
	    CMNode cmnode = null;
	    Enumeration e = children.keys();
	    while(e.hasMoreElements()) {
		String childName = (String)e.nextElement();
		if (cmnode==null)
		    cmnode = new CMLeaf(childName);
		else
		    cmnode = new CM2op('|',cmnode,new CMLeaf(childName));
	    }
	    
	    if (children.get(PCDATA) == null || children.size() != 1)
		cmnode=new CM1op('*',cmnode);
	    ContentModel cm = null;
	    if (cmnode != null)
		cm = new ContentModel(cmnode);
	    
	    // Create an element decl and add it to the dtd
	    //
	    ElementDecl ed = new ElementDecl(name,cm);
	    if (cm == null) 
		ed.setContentType(ElementDecl.EMPTY);
	    dtd.appendChild(ed);
	}
	
	return doc;
    }
    
    
    //--------------------------------------------------------------------
    // traverse tree
    //--------------------------------------------------------------------
    private static String traverseTree(Node n, Hashtable ht) 
    {
	
	int nodeType = n.getNodeType();
	if (nodeType == Node.TEXT_NODE) return PCDATA;
	if (nodeType != Node.ELEMENT_NODE) return null;

	//TXElement node=(TXElement)n;
	Node node = n;

	String name = node.getNodeName();
	Hashtable children = (Hashtable) ht.get(name);
	if (children == null) {
	    children = new Hashtable();
	    ht.put(name,children);
	}
	
	// Recursive calls on children
	//
	NodeList nl = node.getChildNodes();
	int childCount = nl.getLength();
	for (int i = 0; i < childCount; i++) {
	    Node child = nl.item(i);
	    String childName = traverseTree(child,ht);
	    if (childName != null) {
		children.put(childName,"");
	    }
	}
	
	return name;
    }
    


    public static boolean pruneEmptyNodes(Node node) 
    {
        
	boolean empty=false;
	
	if( node.getNodeType() == Node.TEXT_NODE) {
	    String Value=node.getNodeValue();
	    if (Value==null || TXText.trim(Value).length()==0) {
		empty=true;
	    }
	}
        
	// Make recursive calls on children
	//
	if (node.hasChildNodes()) {
	    Stack st = new Stack();
	    NodeList nl = node.getChildNodes();
	    int size = nl.getLength();
	    for (int i = 0; i < size; i++) {
		Node child = nl.item(i);
		if (pruneEmptyNodes(child)) st.push(child);
	    }
	    while(!st.empty()) {
		Node child=(Node) st.pop();
		node.removeChild(child);
	    }
	}	
	return empty;
    }
    
    
    public static void printAttributes(Node node) 
    {
	NamedNodeMap x = null;
	x = node.getAttributes();
	int count = -1;
	if(x!=null){
	    count = x.getLength();
	    System.out.println("Number of attributes is "+count);
	    if (count == 0) return;
	    for (int j=0;j<count;j++) {
		Node n=x.item(j);
		System.out.println("\tattr "+n.getNodeName()+":"+n.getNodeValue());
	    }
	}
    }   


    /**
     *  Parse and return an XML document given a url
     *
     *  @param url the document to parse
     *  @return the parsed XML document or null
     */
    public static TXDocument parseXML(String url)
    throws FileNotFoundException, MalformedURLException, IOException, ParseException{
	
	// Set up a parser
	// 
        Parser p   = new Parser(url);
        p.setWarningNoDoctypeDecl(true);
        p.setWarningNoXMLDecl(true);
        p.setKeepComment(false);
        TXDocument doc = new TXDocument();
        p.setElementFactory(doc);

	// Parse
	//
	// Parse from a file stream
	// 
	if (url.indexOf(":") < 0) {    
	    FileInputStream inStream = new FileInputStream(url);
	    p.readStream( inStream );
	}
	
	// Parse from a URL stream
	//
	else {
	    URL aurl = new URL(url);
	    InputStream inStream = aurl.openStream();
	    p.readStream( inStream );
	}
	
	// Throw exception if parse failed
	//
	if (p.getNumberOfWarnings()+p.getNumberOfErrors()>0) {
		System.out.println("WARNING: Error parsing " + url);
	    throw new ParseException("Error parsing xml file: " + url);
	}
	return doc;
    }

    
    private static InputStream getInputStream(String fileName) 
	throws FileNotFoundException { 
        FileInputStream fileInStream;
	fileInStream = new FileInputStream(fileName);
        return fileInStream;
    }


    private static OutputStream getOutputStream(String fileName) 
	throws FileNotFoundException { 
        FileOutputStream fileOutStream;
            fileOutStream = new FileOutputStream(fileName);
        return fileOutStream;
    }


    private static InputStream getInputStream(Socket sock) 
	throws IOException { 
        InputStream inStream;
	inStream = sock.getInputStream();
        return inStream;
    }
   
 
    private static OutputStream getOutputStream(Socket sock) 
    throws IOException { 
        OutputStream outStream;
	outStream = sock.getOutputStream();
        return outStream;
    }
   
                    
    /**
     *  persist an object to a file
     *
     *  @param obj the object to be persisted to a file
     *  @param fileName the persistent storage of an object
     *  @return a boolean indicating status
     */
    public static boolean memory2persistent(Object obj, String fileName) {
	try {
	    // Create a file output stream, declare ref for object output stream
	    //
	    OutputStream outStream = getOutputStream(fileName);
	    ObjectOutputStream objOutStream = null;    
	    
	    // Write the Object to an object output stream
	    //
	    objOutStream = new ObjectOutputStream(outStream);
	    
	    objOutStream.writeObject(obj);
	    outStream.close();
	    return true;
	}
	catch (NotSerializableException e) {
	    System.err.println("CUtil:memory2persistent-object needs "+
			       "to implement Serializable interface\n");
	    return false;
	} catch (FileNotFoundException fnfe) {
	    System.err.println("Unable to make object persistent. File: " +
			       fileName + " not found.");
	    return false;
	}
	catch(IOException ioe){
	    System.err.println("IO exception: "+ioe);
	    return false;
	}
    }


    /**
     *  Recover a persistent object from a file
     *
     *  @param fileName the persistent storage of an object
     *  @return the object that was read from persisten object source
     */
    public static Object persistent2memory(String fileName)
    {
	try {
	    // Open a stream on the persistent object source
	    //
	    InputStream inStream = getInputStream(fileName);
	    ObjectInputStream objInStream = null;
	    Object retObject = null;

	    // Create an object input stream, read the object
	    // close stream and return object, if error ret null
	    //
	    objInStream = new ObjectInputStream(inStream);
	    retObject = objInStream.readObject();  // not pass by ref
	    objInStream.close();
	    return retObject;
	} catch (NotSerializableException e) {
	    System.err.println("CUtil:persistent2memory-object needs "+
			       "to implement Serializable interface\n");
	    return null;
	}
	catch (IOException ioe){
	    System.err.println("IOException in persistent2memory: "+ioe);
	    return null;
	} catch (ClassNotFoundException cnfe) {
	    System.err.println("Unexplained class not found exception: "+cnfe);
	    return null;
	}
    }


    /**
     *  persist an object to a socket
     *
     *  @param obj the object to be persisted to a socket
     *  @param sock the socket where persisted obj is to be written to
     *  @return an boolean indicating status
     */
    public static boolean memory2socket(Object obj, Socket sock) 
    {
	try {
	    // Open an output stream connected to the socket
	    //
	    OutputStream outStream = getOutputStream(sock);
	    ObjectOutputStream objOutStream = null;
            
	    // Write the object to the socket stream, return true
	    //
	    objOutStream = new ObjectOutputStream(outStream);
	    objOutStream.writeObject(obj);		    
	    objOutStream.close();
	    return true;
	}
	catch (NotSerializableException e) {
	    System.err.println("CUtil:objectToPersist-object needs "+
			       "to implement Serializable interface\n");
	    return false;
	}
	catch (IOException ioe){
	    System.err.println("IOException: "+ioe);
	    return false;
	}
    }


    /**
     * recover a persisted object from a socket
     *
     * @param obj the object to be persisted to a file
     * @param sock the socket where the persisted object is read from
     * @return an boolean indicating status
     */
    public static Object socket2memory(Socket sock) 
    {
	try {
	    // Get the output stream
	    //
	    InputStream inStream = getInputStream(sock);
	    ObjectInputStream objInStream = null;
	    Object retObj = null;
            
	    // Read the object from the socket if possible
	    //
	    objInStream = new ObjectInputStream(inStream);		
	    retObj = objInStream.readObject();
	    objInStream.close();
	    return retObj;	    
	}
	catch (NotSerializableException e) {
	    System.err.println("DMUtil:objectToPersist-object needs "+
			       "to implement Serializable interface\n");
	    return null;
	}
	catch(IOException ioe){
	    System.err.println("IOException: "+ ioe);
	    return null;
	} catch (ClassNotFoundException cnfe) {
	    System.err.println("Unexplained class not found exception: "+cnfe);
	    return null;
	}
    }


    // Send the content of a file to an output stream
    //
    public static void sendFromFile(String fname, BufferedWriter sout)
    {
	try {
	    
	    // Creat an input stream from the file
	    //
	    BufferedReader reader = new BufferedReader(new FileReader
						      (fname));
	    String line;
		
	    // Read the file and send each line without line terminators.
	    //
	    for (;;) {
		
		line = reader.readLine();
		
		if (line == null) {
		    
		    // End of the file is reached
		    //
		    reader.close();
		    sout.write("\n");
		    sout.flush();		    
		    break;
		}
		
		// Otherwise send it (without line terminators)
		//
     		sout.write(line);
		sout.write(' ');		
 
	    } //end of for (;;) sending
	}
	catch (IOException e) {
	    System.err.println("Error in sending a file " + e);
	}

    } // end of sendFromFile()
}






