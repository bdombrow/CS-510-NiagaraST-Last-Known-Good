
/**********************************************************************
  $Id: IndexParser.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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


/**
 * The WordParser parses an XML documents into a bunch of WordEntries.
 *
 */

package niagara.search_engine.indexmgr;

import java.io.*;
import java.util.*;
import java.net.*;
import com.microstar.xml.*;

class Attribute {
	String name;
	String value;
	public Attribute (String name, String value) {
		this.name = name;
		this.value = value;
	}
}

public class IndexParser extends HandlerBase implements XmlHandler {

	public static String tokenDelimiters =
		"\t\n\r\f .,:'/\"!?;-()~`'@#$%^&*=\\+|";
	private int wordCount = 0;
	private int depth = 0;
	private String docUrlStr = null;
	private Vector positionsToPrint = null;
	private StringBuffer printBuffer = null;

	// An attribute vector is used to hold all attributes of an element.
	// This vector is necessary because the attribute events are
	// reported BEFORE the element event is reported.
	//
	private Vector attributeVector = null;

	// Vector of WordEntries as the result of parsing
	//
	private Vector wordEntryVector = null;

	// Stack of incomplete WordEntries. These are word entries of elements
	//
	private Stack wordEntryStack = null;


	/**
	 * Constructor
	 */
	public IndexParser (String urlstr, Vector posToPrint, StringBuffer printBuffer) {
		attributeVector = new Vector();
		wordEntryVector = new Vector();
		wordEntryStack = new Stack();
		docUrlStr = urlstr;
		positionsToPrint = posToPrint;
		this.printBuffer = printBuffer;
	}

	public void startElement (String elname) throws Exception {

		// handle tag

		if (printIt(wordCount)) {
			printBuffer.append ("<"+elname+">\n");
		}
		WordEntry we = new WordEntry (
			elname.toLowerCase(), WordEntry.WT_TAG, wordCount++, depth++);
		wordEntryStack.push (we);

		// now check for attributes held for this element

		for (int i = 0; i < attributeVector.size(); i++) {

			Attribute attr = (Attribute)attributeVector.elementAt(i);

			// create a word entry for the attr name
			if (printIt (wordCount)) {
				printBuffer.append ("<"+attr.name+">");
			}
			we = new WordEntry (attr.name.toLowerCase(), 
					WordEntry.WT_ATTR, wordCount++, depth++);
			wordEntryStack.push (we);

			// parse attribute value
			StringTokenizer strtok = new StringTokenizer(
					attr.value, tokenDelimiters);
			while (strtok.hasMoreTokens()) {

				String token = strtok.nextToken();

				if (printIt(wordCount)) {
					printBuffer.append (token+" ");
				}
				WordEntry attrValueWE = new WordEntry(token.toLowerCase(),
					WordEntry.WT_ATTRVALUE, wordCount++, depth);
				wordEntryVector.addElement (attrValueWE);
			}

			// now we have the end word number for the attr,
			// finish it up
			depth--;
			we = (WordEntry)wordEntryStack.pop();
			if (we == null || !we.getWord().equalsIgnoreCase(attr.name)) {
				throw new WordReaderException ();
			}
			if (printIt(wordCount)) {
				printBuffer.append ("</"+attr.name+">\n");
			}
			we.setPosition (we.getPosition().intValue(), wordCount++);
			wordEntryVector.addElement (we);
		}

		// clear the attribute vector for the next element
		attributeVector.clear();
	}

	public void endElement (String elname) throws Exception {
		depth--;

		// now we have the end word number for the element

		WordEntry we = (WordEntry)wordEntryStack.pop();
		if (we == null || !we.getWord().equalsIgnoreCase(elname)) {
			throw new WordReaderException();
		}
		if (printIt (wordCount)) {
			printBuffer.append ("</"+elname+">\n");
		}
		we.setPosition (we.getPosition().intValue(), wordCount++);
		wordEntryVector.addElement (we);
	}

	public void attribute (String aname, String avalue, boolean isSpecified)
		throws Exception {
		
		if (avalue == null) return;

		// hold on the attribute until the corresponding element event
		// is fired
		attributeVector.addElement (new Attribute(aname.toLowerCase(), avalue));
	}

	public void charData(char ch[], int start, int length)
		throws Exception {

		StringTokenizer strtok = new StringTokenizer (
			new String(ch, start, length), tokenDelimiters);

		while (strtok.hasMoreTokens()) {
			String token = strtok.nextToken();

			if (printIt(wordCount)) {
				printBuffer.append (token+" ");
			}
			WordEntry we = new WordEntry (token.toLowerCase(),
				WordEntry.WT_REGULAR, wordCount++, depth);
			wordEntryVector.addElement (we);
		}
	}

	public void doctypeDecl(String name, String publicId,
		String systemId) throws Exception {

		String dtdurl = systemId;
		if (dtdurl != null) {

			// expand the dtd into an absolute one if necessary
			try {
				URL url = new URL(dtdurl);
			}
			catch (MalformedURLException e) {
				// dtd is not an absolute path, construct one

			    //modified by Qiong Luo, Jan 2000
			    //for "../..." case
			    if (dtdurl.startsWith("../")) {
				String tail = new String(dtdurl);
				String head = new String
				    (docUrlStr.substring
				     (0,docUrlStr.lastIndexOf('/')));
	    
				do {
				    tail = tail.substring(tail.indexOf('/')+1);
				    
				    head = head.substring
					(0,head.lastIndexOf('/'));
				  
				} while (tail.startsWith("../"));
	    
				dtdurl = head.concat("/"+tail);	
			    }	
			    //end of modification, Qiong Luo
			    else {				
				dtdurl = docUrlStr.substring(0, docUrlStr.lastIndexOf('/')+1)
				    + dtdurl;
			    }
			    
			}

			// now create the word entry
			/*
			WordEntry we = new WordEntry (
				dtdurl.toLowerCase(), WordEntry.WT_DTD, wordCount++, 0);
			*/
			WordEntry we = new WordEntry (
				dtdurl, WordEntry.WT_DTD, wordCount++, 0);
			wordEntryVector.addElement (we);
		}
	}

	public void endDocument() {
		WordEntry we = new WordEntry (
			"@doc", WordEntry.WT_TAG, new Position(0, wordCount), 0);
		wordEntryVector.addElement (we);
	}

	public Vector getWordEntries() {
		return wordEntryVector;
	}

	/**
	 * Check to see if the word at 'wordCount' should be printed
	 */
	private boolean printIt (int wordCount) throws Exception {

		Position elPos = new Position (wordCount);
		if (printBuffer != null && positionsToPrint != null) {

			for (int i = 0; i < positionsToPrint.size(); i++) {
				Position pos = (Position)positionsToPrint.elementAt(i);
				if (elPos.isNestedIn (pos)) {
					return true;
				}
			}
		}
		return false;
	}

	public static void main (String[] args) {
		String url = null;
		try {
			url = args[0];
		}
		catch (ArrayIndexOutOfBoundsException e) {
			System.out.println ("Usage: java IndexParser <xmlfile>");
			System.exit(0);
		}

		Vector posToPrint = new Vector();
		posToPrint.addElement (new Position(0, 100));
		StringBuffer sb = new StringBuffer();
		IndexParser index_parser = new IndexParser(url, posToPrint, sb);

		XmlParser xml_parser = new XmlParser();
		xml_parser.setHandler(index_parser);

		try {
			xml_parser.parse(url, (String)null, (String)null);
		}
		catch (Exception e) {
			System.err.println (e);
			System.exit(1);
		}

		// word entries
		System.out.println ("word entries ============ ");
		Vector wordentries = index_parser.getWordEntries();
		for (int i = 0; i < wordentries.size(); i++) {
			WordEntry we = (WordEntry)wordentries.elementAt(i);
			System.out.println (we);
		}

		// the doc
		System.out.println ("the doc ============ ");
		System.out.println (sb);
	}
}
