
/**********************************************************************
  $Id: ResponseHandler.java,v 1.4 2003/07/08 02:10:37 tufte Exp $


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

import com.microstar.xml.HandlerBase;
import java.util.*;
import javax.swing.tree.*;


/**
 * This class handles responses from the server and demultiplexes the resutls for the 
 * various queries that will be run on the client and sent to the server.
 *
 */
class ResponseHandler extends HandlerBase
{
    // constants
    //
    // Those should be negative to distinguish between result nesting counts
    private static final int INI = -1; // expecting messages. Initial State
    private static final int MSG = -2;  // decoding the message.
    private static final int RES = -3; // building up the result
	
    private static final String RESPONSE_MESSAGE = "responseMessage";
    private static final String RESPONSE_DATA = "responseData";
    private static final String RESPONSE = "response";

    private static final String LOCAL_ID = "localID";
    private static final String SERVER_ID = "serverID";
    private static final String RESPONSE_TYPE ="responseType";
	
	private static final String SERVER_QUERY_ID = "server_query_id";
	private static final String SE_QUERY_RESULT = "se_query_result";
	private static final String QUERY_RESULT = "query_result";
	private static final String END_RESULT = "end_result";
	private static final String DTD_LIST = "dtd_list";
	private static final String DTD = "dtd";
	private static final String PARSE_ERROR = "parse_error";
	private static final String ERROR = "error";

    // private clases
	
    /**
     * eStack object
     */
    private class Ell
    {
		// element name. This is null if this objects 
		// represents a part of the PCDATA character 
		// content
		public String name;
		// a linked list of attributes
		public Map attrs;
		// This carries subelements
		public DefaultMutableTreeNode data;
		// The character content of a result element
		public String charData;
		// The nesting level of the element
		// values are taken from the resultLevel
		public int nestingLevel;
		
		/**
		 * Ctor for control elements
		 */
		public Ell(String name, Map attrs)
			{
				// Nesting level irrelevant
				this(name,attrs,(String)null, -1);
			}

		/**
		 * Ctor for result elements
		 */
		public Ell(String name, Map attrs, int nestingLevel)
			{
				this(name,attrs,(String)null, nestingLevel);
			}

		/** 
		 * Ctor for character chunks
		 */
		public Ell(String charData)
			{
				// Nesting level is irrelevant
				this(null, null, charData,-1);
			}

		/**
		 * Private Ctor
		 */
		private Ell(String name, Map attrs, String charData, int nestingLevel)
			{
				this.name = name;
				this.attrs = attrs;
				this.data = null;
				this.charData = charData;
				this.nestingLevel = nestingLevel;
			}

		public String toString()
			{
				String s = null;
				if(name != null){
					s = "Element = " + name +"\n";
					if(attrs != null){
						s += attrs.toString() + "\n";
					}
					if(data != null){
						s += "tree node = " + data.toString() + "\n";
					}
				} else {
					s = "CharData = " + charData +"\n";
				}
				return s;
			}
    }

    // private variables
    //
	// The query registry
	private QueryRegistry reg;
	// The Runnable that is going to interact with th gui
	private UIDriverIF ui;
	// dtd cache for getting dtd strings from the server
	private DTDCache dtdCache;
    // State of parser
    private int parserState;
    // This shows the nesting of the result document
    // hierarchy so far. Initial value is 0
    private int resultLevel = 0; 
	// This is used to decide whether to process 
	// the entire children list or not
	private int prevResultLevel;
    /**
     * Attirbute accumulator
     */
    private HashMap attMap= new HashMap();
    /**
     * Stack for Element Processing
     */
    private Stack eStack = new Stack();
    /**
     * Children list of Ell's to construct the 
     * result tree for display
     */
    private List childrenList = new LinkedList();
	/**
	 * This is the list of tree nodes that will end up 
	 * in the registry
	 */
	private List nodeList = new LinkedList();
    
    
    /**
     * Constructor: All the structures manipulated by the handler callbacks
     * should be passed to the constructor
     * @param reg the registry to update
	 * @param obj the object to notify for changes
     */
    public ResponseHandler(QueryRegistry reg, UIDriverIF ui, DTDCache dtdCache)
		{
			this.reg = reg;
			this.ui = ui;
			this.dtdCache = dtdCache;
		}
	
	/**
     * Attribute handler: Accumulates attributes that are subsequently processed 
     * as soon as a startelement is called
     * @param aname the attribute name
     * @param value the attribute value
     * @param isSpecified false if the DTD default is used
     */
    public void attribute(String aname,
						  String value,
						  boolean isSpecified) throws Exception
		{
			// Accumulate the attributes regardless of parser state
			if(parserState == RES && !isSpecified){
			} else {
				attMap.put(aname, value);
			}
		}
	
    /**
     * Start Document. It marks the beginning of the section
     */
    public void startDocument() throws Exception
		{}
    /**
     * End Document. It marks the end of the Client Server session
     */
    public void endDocument() throws Exception
		{
			System.err.println("END OF DOCUMENT");
		}
	
    /**
     * Handle a start element event.
     * @param elname Element Name
     */
    public void startElement(String elname) throws Exception
		{
			// Check to see if the element name is in the dtd
			// If it is you must build up a MessageData object
			// otherwise you must build up an object to put 
			// into the message data responseData field
			
			if(parserState != RES){
				// Message Elements
				if(elname.equalsIgnoreCase(RESPONSE)){
					// Session opened put the element onto the stack
					//eStack.push(new Ell(elname, null));
					parserState = INI;// Expect a response message
				} else if(elname.equalsIgnoreCase(RESPONSE_MESSAGE)){
					// Build up a MessageData object
					eStack.push(new Ell(elname, attMap));
					attMap = new HashMap();
					parserState = MSG;
				} else if(elname.equalsIgnoreCase(RESPONSE_DATA)){
					// Build up a result if necessary
					eStack.push(new Ell(elname, null, resultLevel));
					parserState = RES;
					resultLevel++;// increase the nesting level
				} else {
					System.err.println(elname);
				}
			} else {
				// Result Elements
				eStack.push(new Ell(elname, attMap, resultLevel));

				attMap = new HashMap();
				if(parserState != RES){
					System.err.println(parserState);
				}
				resultLevel++;// increase the nesting level
			}
		}
	
    /**
     * Handle character data
     */
    public void charData (char ch[], int start, int length)
		{
			// Make a string out of the character data
			String cdata = new String(ch, start, length);
			String trimmed_cdata = cdata.trim();

			if(trimmed_cdata.length() == 0){
				// This is ignorable whitespace so don't do anything
			} else {
				// add an anonymous Ell object to the stack
				// and keep the resultLevel variable the same
				eStack.push(new Ell(cdata));
			}
		}
	
    /**
     * Handle an endElement event 
     * @param elname Element name
     */
    public void endElement(String elname) throws Exception
		{
			// pop the parser state and see what you have to do
			int ps = parserState;

			if(resultLevel>0) {
				resultLevel--;
			}
			
			if(ps == INI){
				// This is the end of the session
			}
			else if(ps == MSG){
                // This signals the end of processing for a message
                // (only one element should be in the children list)
                
                // Pop the responseMessage element from the stack
				Ell ell = (Ell)(eStack.pop());
				// Create a MessageData object
				
				int localId = Integer.parseInt((String)(ell.attrs.get(LOCAL_ID)));
				int serverId = Integer.parseInt((String)(ell.attrs.get(SERVER_ID)));
				String responseType = ((String)(ell.attrs.get(RESPONSE_TYPE)));

				
				// update the registry passing the children list
				this.updateRegistry(localId, serverId, responseType, nodeList);
				// clear the node list for later use
				nodeList.clear();
				
 				// set parserState back to original INI
				// that is going to signal on </response>
				// the end of session
				parserState = INI;
			} else {
				// Assertion
				if(ps != RES){
					System.err.println("parserState should be RES and it is not");
				}
                // Build up the partial result tree
				
				 // Add all children to the children list
				Ell ell = (Ell)(eStack.peek());
				while(ell.name != elname){
					childrenList.add(0,eStack.pop());
					ell = (Ell)(eStack.peek());
				}
                // Get the element Ell object
				ell = (Ell)(eStack.pop());
				
				// if this is not the end of a responseData node
				if(resultLevel !=0){
					
					DefaultMutableTreeNode 
						elRoot = new DefaultMutableTreeNode(elname, true);
					
					// Process the Attributes of an element if any
					if(ell.attrs != null){
						Set attrValueSet = ell.attrs.entrySet();
						Iterator attrValueSetIt = attrValueSet.iterator();
						
						while(attrValueSetIt.hasNext()){
							Map.Entry entry = (Map.Entry)attrValueSetIt.next();
							String attName = (String)(entry.getKey());
							String attValue = (String)(entry.getValue());
							String treeValue = attName + " = " + attValue;
							DefaultMutableTreeNode
								attChild = new DefaultMutableTreeNode(treeValue);
							elRoot.add(attChild);
						}
					}
					
					// Process the children stack
					Iterator listIt = childrenList.iterator();
					
					while(listIt.hasNext()){
						Ell e = (Ell)(listIt.next());
						if(e.name == null){
							// This is a text node
							DefaultMutableTreeNode
								cdata = new DefaultMutableTreeNode(e.charData);
							elRoot.add(cdata);
							listIt.remove();
						} else {
							// this is an element. Add it only
							// if it has the correct nesting level
							if(e.nestingLevel == ell.nestingLevel + 1){
								elRoot.add(e.data);
								listIt.remove();
							}
						}
					}
					// Update ell
					ell.data = elRoot;
					
					// and add ell on to it
					childrenList.add(ell);
				} else {
					parserState = MSG;
					// Create a list of mutable tree nodes the children list to add to the registry
					Iterator ci = childrenList.iterator();
					while(ci.hasNext()){
						Ell e = (Ell)(ci.next());
						if(e.data!=null) {
							// Tree node
							nodeList.add(e.data);
						} else {
							// Create a tree node
							nodeList.add(
								new DefaultMutableTreeNode(e.charData));
						}							
					}
					childrenList.clear();
				}
			}
		}
			
    /**
     * Error handler
     */
    public void error(String message,
					  String systemId,
					  int line,
					  int column) throws Exception
		{
			System.err.println("FATAL ERROR: " + message);
			System.err.println("  at " + systemId + ": line " + line
							   + " column " + column);
			throw new Error(message);
		}

	// PRIVATE FUNCTIONS
	/**
	 * update the registry.
	 * @param lid local query id
	 * @param sid server query id
	 * @param rtype the result type
	 * @param nList the list of result nodes
	 */
	public void updateRegistry(int lid, 
							   int sid, 
							   String rtype,
							   List nList)
		{
			// Check to see what to do with the result
			if(rtype.equals(QUERY_RESULT)){
				if(nList.size() < 1){
					System.err.println("Illegal list size=" + nList.size());
					System.err.println("The query result should not be zero");
					return;
				}
				// check to see if the query is killed
				if(!reg.isKilled(lid)){
					// if the id corresponds to an already fired trigger then clear the tree
					if(reg.wasAlreadyFired(lid) && !(reg.isTriggerPaused(lid))){
						reg.clearTree(lid);
					}
					reg.addResults(lid, sid, nList);
					// notify waiting thread of new result
					ui.notifyNew(lid);
				}
			} 
			else if(rtype.equals(SERVER_QUERY_ID)){
				if(nList.size() > 0){
					System.err.println("Illegal list size=" + nList.size());
					return;
				}
				
				reg.setServerId(lid, sid);
			}
			else if(rtype.equals(SE_QUERY_RESULT)){
				// Jaewoo gives
				// <!ELEMENT result (item*)>
				// <!ELEMENT item (#PCDATA)>
				if(nList.size() != 1){
					System.err.println("Illegal list size=" + nList.size());
					System.err.println("Search engine query should return one item only");
					return;
				}
				// get the node from the list
				DefaultMutableTreeNode n = (DefaultMutableTreeNode)(nList.get(0));
				
				Enumeration nChildren = n.children();
				List l = new LinkedList();

				while(nChildren.hasMoreElements()){
					DefaultMutableTreeNode nn = 
						(DefaultMutableTreeNode)(nChildren.nextElement());
					l.add(nn.getChildAt(0));
				}					

				reg.addResults(lid, sid, l);
				// notify waiting thread of new result
				if(l.size() > 0) {
					ui.notifyNew(lid);				
				}
			}
			else if(rtype.equals(END_RESULT)){
				// Mark the specific result as final and 
				// notify the listener
				QueryRegistry.Entry e = reg.getQueryInfo(lid);
				int queryType = reg.getQueryType(lid);
    			        reg.markFinal(lid);
				ui.notifyFinalResult(lid);
			}
			else if(rtype.equals(DTD_LIST)){
				// Construct a DTD_LIST object
				if(nList.size() != 1){
					System.err.println("Illegal list size. Should be one and is " + nList.size());
					System.err.println("Server sent no dtd's or server sent bad format");
					return;
				}
				DefaultMutableTreeNode n = (DefaultMutableTreeNode)(nList.get(0));
				reg.constructDTDList((String)(n.getUserObject()));
				// remove all nodes from the node list
				nList.clear();
			}
			else if(rtype.equals(DTD)){
				// Put the dtd you got in the cache
				// and notify any waiting threads
				if(nList.size() != 1){
					System.err.println("Illegal list size. Should be one and is " + nList.size());
					System.err.println("Server sent bad dtd format");
					return;
				}
				DefaultMutableTreeNode n = (DefaultMutableTreeNode)(nList.get(0));
				String s = (String)(n.getUserObject());
				dtdCache.putDTD(lid, s);
			}
			else if(rtype.equals(PARSE_ERROR)){
				// Put a message to be displayed and 
				// mark the query as final
				if(nList.size() < 1){
					System.err.println("Illegal list size=" + nList.size());
				}
				reg.addResults(lid, sid, nList);
				reg.markFinal(lid);
				// notify
				ui.errorMessage(lid, "RH: Parse Error");
			}
			else // General Error
			{
				// Spit out an error message
				DefaultMutableTreeNode n = 
					new DefaultMutableTreeNode("Error");
				List l = new LinkedList();
				l.add(n);
				reg.addResults(lid, sid, l);
				reg.markFinal(lid);
				// notify
				ui.errorMessage(lid, " Error. Received "+rtype);
			}
		}
	
}


