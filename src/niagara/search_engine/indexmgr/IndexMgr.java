
/**********************************************************************
  $Id: IndexMgr.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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


package niagara.search_engine.indexmgr;

import java.util.*;
import java.net.*;
import java.io.*;
import com.microstar.xml.*;
import niagara.search_engine.operators.*;
import niagara.search_engine.seql.sym;

/**
 * Index Manager: Builds and manages indices
 *
 */

public class IndexMgr {
	//
	// Assuming that there will only be one index manager, this public
	// static member lets the index manager to be accessed anywhere as
	// IndexMgr.idxmgr
	//
	public static IndexMgr idxmgr=null;
	static {
		try {
			idxmgr = new IndexMgr("seindex.config");
		}
		catch (Exception e) {
			System.out.println ("Error creating IndexMgr. Exiting...");
			e.printStackTrace();
			System.exit(0);
		}
	}

 	//
	// Type of the index
	//
	public static final int INDEX_T_FULL = 0x00;
	public static final int INDEX_T_DOC = 0x01;
	public int indexType = 0x00;

	public static final boolean DEBUG = false;

	//
	// Below are data structures that are controlled by the index manager
	//
	private DocMap docMap = null;
	private TMLexicon textLexicon = null;
	private TMLexicon elemLexicon = null;
	private TMLexicon dtdLexicon = null;
	private NumberLexicon numLexicon = null;
	private IVLMgr textIVLMgr = null;
	private IVLMgr elemIVLMgr = null;
	private IVLMgr dtdIVLMgr = null;
	private IVLMgr numIVLMgr = null;

	//
	// These are for batch updating inverted lists. 
	// Hashtable maps from wordno --> IVLEntry
	//
	private Hashtable textIvlBatch = null;
	private Hashtable elemIvlBatch = null;
	private Hashtable numIvlBatch = null;
	private Hashtable dtdIvlBatch = null;
	private int batchLevel = 0;
	private int batchedDocCount = 0;

	/**
	 * Constructor. Open and load the indices.
	 */
	public IndexMgr (String configFile) throws IMException {
	
		// Load in configurations
		try {
			Properties props = new Properties(System.getProperties());
			props.load (new FileInputStream(configFile));
			System.setProperties(props);

			if (DEBUG) {
				System.out.println ("System Properties ----------");
				System.getProperties().list (System.out);
			}
		}
		catch (FileNotFoundException e) {
			System.out.println ("Problem reading configurations from "
				+ configFile);
			System.exit(1);
		}
		catch (IOException e) {
			System.out.println ("Problem reading configurations from "
				+ configFile);
			System.exit(1);
		}

		// type of index
		if (System.getProperty("se.index.type").equals("full")) {
			indexType = INDEX_T_FULL;
		}
		else if (System.getProperty("se.index.type").equals("doc")) {
			indexType = INDEX_T_DOC;
		}
		else {
			System.err.println ("Unknown index type!");
			System.exit(1);
		}

		// document map
		docMap = new DocMap(System.getProperty("se.index.docmap"));
	
		// lexicons
		textLexicon = new TMLexicon(
			System.getProperty("se.index.textlexicon"));

		elemLexicon = new TMLexicon(
			System.getProperty("se.index.elemlexicon")); 

		dtdLexicon = new TMLexicon(
			System.getProperty("se.index.dtdlexicon"));

		numLexicon = new NumberLexicon(
			System.getProperty("se.index.numLexicon"));

		// ivl mgrs
		try {

			//////// text ivlmgr
			int textbufsize = 0; 
				//Integer.parseInt(System.getProperty("se.textivl.bufsize"));
			if (textbufsize != -1) textbufsize *= 1000;

			textIVLMgr = new IVLMgr(IVLMgr.MGRT_TEXT,
				System.getProperty("se.index.textivldir"),
				System.getProperty("se.textivldir.structure").equals("flat")?
					InvertedFile.IVFT_FLAT : InvertedFile.IVFT_PAGED,
				Position.PT_SINGLE,
				textbufsize);

			//////// elem ivlmgr
			int elembufsize = 0;
				//Integer.parseInt(System.getProperty("se.elemivl.bufsize"));
			if (elembufsize != -1) elembufsize *= 1000;

			elemIVLMgr = new IVLMgr(IVLMgr.MGRT_ELEM,
				System.getProperty("se.index.elemivldir"),
				System.getProperty("se.elemivldir.structure").equals("flat")?
					InvertedFile.IVFT_FLAT : InvertedFile.IVFT_PAGED,
				Position.PT_PAIR,
				elembufsize);

			//////// dtd ivlmgr
			int dtdbufsize = 0;
				//Integer.parseInt(System.getProperty("se.dtdivl.bufsize"));
			if (dtdbufsize != -1) dtdbufsize *= 1000;

			dtdIVLMgr = new IVLMgr(IVLMgr.MGRT_DTD,
				System.getProperty("se.index.dtdivldir"),
				System.getProperty("se.dtdivldir.structure").equals("flat")?
					InvertedFile.IVFT_FLAT : InvertedFile.IVFT_PAGED,
				Position.PT_SINGLE,
				dtdbufsize);

			//////// num ivlmgr
			int numbufsize = 0;
				//Integer.parseInt(System.getProperty("se.numivl.bufsize"));
			if (numbufsize != -1) numbufsize *= 1000;

			numIVLMgr = new IVLMgr(IVLMgr.MGRT_NUM,
				System.getProperty("se.index.numivldir"),
				System.getProperty("se.numivldir.structure").equals("flat")?
					InvertedFile.IVFT_FLAT : InvertedFile.IVFT_PAGED,
				Position.PT_SINGLE,
				numbufsize);

		}
		catch (NumberFormatException e) {
			System.err.println(e);
			System.exit(1);
		}

		textIvlBatch = new Hashtable();
		elemIvlBatch = new Hashtable();
		numIvlBatch = new Hashtable();
		dtdIvlBatch = new Hashtable();
		try {
			batchLevel = Integer.parseInt(
					System.getProperty("se.index.batchlevel"));
		}
		catch (NumberFormatException e) {
			System.err.println (e);
			System.exit(1);
		}

		System.out.println ("\nIndexMgr: I am ready.\n");
	}

	/**
	 * Flush any in-memory stuff back to disk
	 */
	public void flush() throws IMException {

		writeBatch ();
		batchedDocCount = 0;

		// write doc map file
		docMap.flush();

		// write lexicons and inverted lists
		textLexicon.persist();
		elemLexicon.persist();
		dtdLexicon.persist();
		numLexicon.persist();

		textIVLMgr.persist();
		elemIVLMgr.persist();
		dtdIVLMgr.persist();
		numIVLMgr.persist();

		System.out.println ("\nIndexMgr: I am done.\n");
	}

	/**
	 * Getters
	 */
	public String getDocName(long docno) {
		return docMap.getDocName(docno); 
	}

	public long getDocNo(String fname) {
		URL u;
		try { u = createURL (fname); }
		catch (MalformedURLException e) {
			return -1;
		}
		return docMap.getDocNo(u);
	}


	/**
	 * Index a file or all files in a directory that matches certain 
	 * pattern
	 *
	 * @parameter pathname
	 *		the filename or a directory to index
	 * @parameter beginPattern
	 *		when not null, specify the begin pattern of the filenames to 
	 *		be indexed.
	 * @parameter endPattern
	 *		when not null, specify the end pattern of the filenames to 
	 *		be indexed.
	 *
	 * @return a message to the caller indicating the status of the
	 * indexing.  If the indexing succeeds, the msg will be null.
	 */
	public String index (
		String pathname, String beginPattern, String endPattern) {

		File fp = new File(pathname);
		if (!fp.exists()) {
			return ("IndexMgr Error: "+pathname+" does not exist.\n");
		}

		// if the pathname is a directory, we gather all files we can
		// find directly or indirectly in that directory

		Vector urls = new Vector();
		gatherFiles (fp, urls, beginPattern, endPattern);

		// now do the indexing

		String errstr = null;
		for (int i=0; i < urls.size(); i++) {
			URL u = (URL)urls.elementAt(i);
			String err = index(u);
			if (err != null) errstr += err;
		}
		return errstr;
	}

	/**
	 * Index a file specified by an URL
	 *
	 * @return whether the indexing succeeds or not
	 */
	public synchronized String index (URL url) {

		String errstr = null;

		System.out.print ("IndexMgr: indexing " +url.toString()+"...");

		// check if the file is already indexed, if not, add to doc map
		long docno = docMap.getDocNo(url);
		if (docno > 0) {
			System.out.println("already indexed.");
			return null;
		}

		// add this file to doc map
		docno = docMap.add (url);

		// add to word map, lexicon and inverted list
		WordEntry we = null;
		try {
			// parse the file

			IndexParser index_parser = new IndexParser(
				url.toString(), null, null);
			XmlParser xml_parser = new XmlParser();
			xml_parser.setHandler (index_parser);

			xml_parser.parse (url.toString(), (String)null, (String)null);

			// process word entries returned from parser

			Vector wordentries = index_parser.getWordEntries();
			for (int i = 0; i < wordentries.size(); i++) {

				we = (WordEntry)wordentries.elementAt(i);

                // add to text/num lexicon
				if ( (we.getType()==WordEntry.WT_REGULAR 
					  || we.getType()==WordEntry.WT_ATTRVALUE)
					 && !StopWord.isStopWord(we.getWord())) {

					// number lexicon
					try {
						Double dummy = new Double(we.getWord());
						int wordno = numLexicon.add(we, docno);
						addToBatch (numIvlBatch, wordno, docno,
									we.getPosition());
					}

					// text lexicon
					catch (NumberFormatException e) {
						int wordno = textLexicon.add(we, docno);
						addToBatch (textIvlBatch, wordno, docno,
									we.getPosition());

						//regionIndex.add (docno, wordno, we);
					}
				}
		    
				// add to element lexicon
				else if (we.getType() == WordEntry.WT_TAG
				|| we.getType() == WordEntry.WT_ATTR) {

					int wordno = elemLexicon.add(we, docno);
					addToBatch (elemIvlBatch, wordno, docno, we.getPosition());
				}

				// add to dtd lexicon
				else if (we.getType() == WordEntry.WT_DTD) {
					int wordno = dtdLexicon.add(we, docno);
					addToBatch (dtdIvlBatch, wordno, docno, we.getPosition());
				}
			} // end for

			// send batched-up inverted lists to IVLMgr
			batchedDocCount++;
			if (batchedDocCount >= batchLevel) {
				writeBatch ();
				batchedDocCount = 0;
			}
		}
		catch (Exception e) {
			errstr = "Problem indexing file: " + e;

			docMap.remove (docno);

			System.out.println (errstr);
			return errstr;
		}

		System.out.println("done.");
		return errstr;
	}

	/**
	 * Process containment operation
	 * @param parent the parent
	 * @param child the child
	 * @param isElement whether element or word containment
	 * @param print whether to print the result
	 */
	public void processContainment (String parent, String child,
				    boolean childIsElement, boolean print)
		throws IMException {

		if (parent==null || child==null) return;
		System.out.println("Finding element "+parent+" containing "+child+"...");
	
		Vector parameters = new Vector();

		parameters.addElement(IndexMgr.idxmgr);
		parameters.addElement(parent.toLowerCase());
		parameters.addElement(new Boolean(true));
	
		IVLOp pOp = new IVLOp(parameters);
	
		parameters.setElementAt(child.toLowerCase(),1);
		parameters.setElementAt(new Boolean(childIsElement),2);
		IVLOp cOp = new IVLOp(parameters);

		// get inverted lists
		try {
			pOp.evaluate();
			cOp.evaluate();
		}

		catch (IMException e) {
			System.err.println ("Error processing query.");
			return;
		}

		Vector parentIVL = pOp.getResult();
		if (parentIVL == null) {
			System.out.println("not found");
			return;
		}

		Vector childIVL = cOp.getResult();
		if (childIVL == null) {
			System.out.println("not found");
			return;
		}

		System.out.println("");

		// now processing containment
		Vector operands = new Vector();
		operands.addElement(parentIVL);
		operands.addElement(childIVL);
	
		ContainOp ctOp = new ContainOp(operands);
		try {
			ctOp.evaluate ();
		}
		catch (IMException e) {
			System.err.println ("Error processing query");
			return;
		}

		Vector results = ctOp.getResult();
		System.out.println("Found in " + results.size() + " docs");
		if (print) {
			for (int ri = 0; ri < results.size(); ri++) {

				IVLEntry ivlent = (IVLEntry)results.elementAt(ri);
				System.out.println("In "+ docMap.getDocName(ivlent.getDocNo())
						+"----------");

				long docno = ivlent.getDocNo();
				Vector poslist = ivlent.getPositionList();

				System.out.println (retrieve (docno, poslist));
			}
		}
	}


	/**
	 * Retrieve parts from a document.
	 * @param name the name of the document
	 * @param positions the positions to print
	 */
	public String retrieve (String name, Vector positions) {

		StringBuffer sb = new StringBuffer();

		try {
			URL url = createURL(name);

			IndexParser index_parser = new IndexParser (
				url.toString(), positions, sb);
			XmlParser xml_parser = new XmlParser();
			xml_parser.setHandler (index_parser);

			xml_parser.parse (url.toString(), (String)null, (String)null);
		}
		catch (Exception e) {
			System.out.println(e);
		}

		return sb.toString();
	}

	/**
	 * Retrieve parts from a document
	 * @param docNo the number of the document
	 * @param positions the positions to print
	 */
	public String retrieve (long docNo, Vector positions) {

		String filename = getDocName(docNo);
		return retrieve (filename, positions);
	}
    
	/**
	 * Get the inverted list of a text word or an element
	 */
	public Vector getInvertedList (String item, boolean itemIsElement) {
		Vector ivl = null;
		int wordno;

		// get the word number from the lexicon,
		// then get the inverted list from the ivlmgr using the word
		// number as key

		try {
			if (itemIsElement) {
				wordno = elemLexicon.getWordNum(item);
				if (wordno < 0) return null;

				// try to see if the list is in the batch first,
				// if not, get it from IVL mgr

				IVL ivlobj = (IVL)elemIvlBatch.get(new Integer(wordno));
				if (ivlobj == null) {
					System.out.println ("ELEM_IVLMGR: getting element "
											+item+"...");
					ivl = elemIVLMgr.getInvertedList (wordno);
				}
				else {
					ivl = ivlobj.getList();
				}
			}
			else {
				wordno = textLexicon.getWordNum(item);
				if (wordno < 0) return null;

				// try to see if the list is in the batch first,
				// if not, get it from IVL mgr

				IVL ivlobj = (IVL)textIvlBatch.get (new Integer(wordno));
				if (ivlobj == null) {
					System.out.println ("TEXT_IVLMGR: getting word "
												+item+"...");
					ivl = textIVLMgr.getInvertedList (wordno);
				}
				else {
					ivl = ivlobj.getList();
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return ivl;
	}

	/**
	 * Get the inverted list of a dtd name
	 */
	public Vector getDTDInvertedList (String item) {

		Vector ivl = null;
		int wordno;

		// get the word number from the lexicon,
		// then get the inverted list from the ivlmgr using the word
		// number as key

		try {
			wordno = dtdLexicon.getWordNum(item, true);
	 		if (wordno < 0) return null;

			// try to see if the list is in the batch first,
			// if not, get it from IVL mgr

			IVL ivlobj = (IVL)dtdIvlBatch.get (new Integer(wordno));
			if (ivlobj == null) {
				System.out.println ("DTD_IVLMGR: getting dtd "+item+"...");
				ivl = dtdIVLMgr.getInvertedList (wordno);
			}
			else {
				ivl = ivlobj.getList();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return ivl;
	}

	/**
	 * Get a vector of inverted lists which satisfies "op value"
	 */
	public Vector getIVLs (int op, double value) {

		Vector ivls = new Vector();	
		Vector ivl = null;
		int wordno;

		String opstr=null;
		switch (op) {
			case sym.EQ: opstr = "="; break;
			case sym.LT: opstr = "<"; break;
			case sym.GT: opstr = ">"; break;
			case sym.LEQ: opstr = "<="; break;
			case sym.GEQ: opstr = ">="; break;
			default: opstr="(unknow)"; break;
		}

		//first getWordNums
		int[] wordnos = numLexicon.getWordNums(op,value);
		if (wordnos == null || wordnos.length==0) return ivls;
	
		//Then get the inverted list from the ivlmgr using the word
		// number as key

		try {
			for (int i = 0; i< wordnos.length; i++) {


				IVL ivlobj = (IVL)numIvlBatch.get(new Integer(wordnos[i]));
				// not in the batch, get it from disk
				if (ivlobj == null) {
					System.out.println ("NUM_IVLMGR: getting numbers "
									+opstr+" "+value+"...");
					ivl = numIVLMgr.getInvertedList (wordnos[i]);
				}
				// in the batch
				else {
					ivl = ivlobj.getList();
				}
				ivls.addElement(ivl);		
			}
		}
	
		catch (Exception e) {
			e.printStackTrace();
		}

		return ivls;
	}

	/**
	 * Get all the dtds indexed.
	 * @return array of String's
	 */
	public Vector getDTDs() {
		Object[] dtds = dtdLexicon.getAllWords();
		Vector result = new Vector(dtds.length, 0);
		for (int i = 0; i < dtds.length; i++) {
			result.addElement (dtds[i]);
		}
		return result;
	}

	/**
	 * Get all the XML files conforming to the DTD specified by the URL
	 *
	 * @return null if no such file exists
	 */
	public Vector getXMLByDTD(String absoluteDTDURLPath) throws IMException {
		int wordno = dtdLexicon.getWordNum(absoluteDTDURLPath);
		if (wordno < 0) {
			return null;
		}

		Vector ivl = null;

		IVL ivlobj = (IVL)dtdIvlBatch.get (new Integer(wordno));
		if (ivlobj == null) {
			try {
				System.out.println ("DTD_IVLMGR: getting dtd "
									+absoluteDTDURLPath);
				ivl = dtdIVLMgr.getInvertedList(wordno);
			}
			catch (IOException e) {
				System.err.println(e);
				return null;
			}
		}
		else {
			ivl = ivlobj.getList();
		}
		if (ivl == null) return null;

		Vector xmls = new Vector();
		for (int i = 0; i < ivl.size(); i++) {
			IVLEntry ivlent = (IVLEntry)ivl.elementAt(i);
			long docno = ivlent.getDocNo();
			xmls.addElement (docMap.getDocName(docno));
		}
		return xmls;
	}

    /**
     * Print all the docs maintained
     */
    public void printDocs() {
		docMap.printDocs();
    }

	/**
	 * Print the lexicons
	 */
	public void printLexicons() {
		System.out.println ("\nELEMENT LEXICON-----");
		elemLexicon.print();

		System.out.println ("\nTEXT LEXICON-----");
		textLexicon.print();

		System.out.println ("\nNUMBER LEXICON-----");
		numLexicon.print();

		System.out.println ("\nDTD LEXICON-----");
		dtdLexicon.print();
	}

	/**
	  * Print everything, in the format of:
	  * <term>: <docno; pos, pos, pos><docno; pos ..>...
	  */
	public void printAll () {
		try {
			// element inverted lists

			FileOutputStream elem_fos = new FileOutputStream ("elem_ivl.dat");
			PrintStream elem_ps = new PrintStream (elem_fos);

			System.out.println ("\nELEMENT INVERTED LISTS-----");
			elemLexicon.printAll(elemIVLMgr, elem_ps);

			elem_ps.flush();
			elem_ps.close();

			// text/number inverted lists

			FileOutputStream text_fos = new FileOutputStream ("text_ivl.dat");
			PrintStream text_ps = new PrintStream (text_fos);

			System.out.println ("\nTEXT INVERTED LISTS-----");
			textLexicon.printAll(textIVLMgr, text_ps);

			System.out.println ("\nNUMBER INVERTED LISTS-----");
			numLexicon.printAll(numIVLMgr, text_ps);

			System.out.println ("\nDTD INVERTED LISTS-----");
			dtdLexicon.printAll(dtdIVLMgr, text_ps);

			text_ps.flush();
			text_ps.close();
		}
		catch (Exception e) {
			System.err.println (e);
		}
	}

	/**
	  * Print inverted lists as relational records, in the format of:
	  * elements: <term> <docno> <begin> <end>
	  * texts: <term> <docno> <wordno>
	  */
	public void printRelationRecords () {
		try {
			// element records

			FileOutputStream elem_fos = new FileOutputStream ("elem_records");
			PrintStream elem_ps = new PrintStream (elem_fos);

			Object[] all_elems = elemLexicon.getAllWords();
			for (int i = 0; i < all_elems.length; i++) {

				// an element and its inverted list
				String an_elem = (String)all_elems[i];

				int wordno = elemLexicon.getWordNum(an_elem);

				Vector ivl = elemIVLMgr.getInvertedList(wordno);

				for (int j = 0; ivl!=null&&j < ivl.size(); j++) {
				
					IVLEntry ivlent = (IVLEntry)ivl.elementAt(j);
					Vector pl = ivlent.getPositionList();

					for (int k = 0; k < pl.size(); k++) {
					
						Position pos = (Position)pl.elementAt(k);
						long posval = pos.longValue();

						elem_ps.println (wordno  // word
							+", "+ivlent.getDocNo()  // docno
							+", "+(int)posval  // begin
							+", "+(int)(posval>>32));
					}
				}
			}

			elem_ps.flush();
			elem_ps.close();

			// text/number inverted lists

			FileOutputStream text_fos = new FileOutputStream ("text_records");
			PrintStream text_ps = new PrintStream (text_fos);

			Object[] all_texts = textLexicon.getAllWords();
			for (int i = 0; i < all_texts.length; i++) {

				// a text and its inverted list
				String a_word = (String)all_texts[i];

				int wordno = textLexicon.getWordNum(a_word);

				Vector ivl = textIVLMgr.getInvertedList (wordno);

				for (int j = 0; ivl!=null&&j < ivl.size(); j++) {
				
					IVLEntry ivlent = (IVLEntry)ivl.elementAt(j);
					Vector pl = ivlent.getPositionList();

					for (int k = 0; k < pl.size(); k++) {
					
						Position pos = (Position)pl.elementAt(k);

						text_ps.println (wordno  // word
							+", "+ivlent.getDocNo()  // docno
							+", "+pos.intValue());  // wordno
					}
				}
			}

			text_ps.flush();
			text_ps.close();
		}
		catch (Exception e) {
			System.err.println (e);
		}
	}

    /**
     * Remove index information about a document
     * @return true if the index information is successfully removed
     *         false otherwise
     */
    public boolean remove (URL url) {
	//TODO
	// first, get the docno
	long docno = docMap.getDocNo(url);
	if (docno < 0) return false;

	docMap.remove(docno);
	return true;
    }

	public void dumpRegionIndex (byte dumpLevel) {
		//regionIndex.dump(dumpLevel);
	}

    /////////////////////////  private functions ///////////////////////
	private static URL createURL (String name) throws MalformedURLException {
		URL u;
		try {
			u = new URL (name);
			return u;
		}
		catch (MalformedURLException e) {}
	
		u = new URL ("file:"+ new File(name).getAbsolutePath());
		return u;
	}

	/**
	 * Batch up the inserts
	 */
	private void addToBatch (Hashtable ht, int wordno, long docno,Position pos) 
		throws IMException {

		IVLEntry newent = null;
		if (indexType == INDEX_T_FULL) {
			newent = new IVLEntry(docno, pos);
		}
		else {
			newent = new IVLEntry(docno);
		}

		IVL ivlobj = (IVL)ht.get (new Integer(wordno));

		// for a new word
		if (ivlobj==null) {

			ivlobj = new IVL(wordno);
			ivlobj.add (newent);

			ht.put (new Integer(wordno), ivlobj);
		}

		// for an old word.
		else {
			ivlobj.merge (newent);
		}
	}

	/**
	 * Send the batched inserts to IVLMgr
	 */
	private void writeBatch () throws IMException {

		Integer wordNo = null;
		IVL ivlobj=null;

		// write text batch
		Enumeration textWordNos = textIvlBatch.keys();
		while (textWordNos.hasMoreElements()) {

			wordNo = (Integer)textWordNos.nextElement();
			ivlobj = (IVL)textIvlBatch.get(wordNo);
			textIVLMgr.addIVL (ivlobj);
		}

		// write elem batch
		Enumeration elemWordNos = elemIvlBatch.keys();
		while (elemWordNos.hasMoreElements()) {

			wordNo = (Integer)elemWordNos.nextElement();
			ivlobj = (IVL)elemIvlBatch.get(wordNo);
			elemIVLMgr.addIVL (ivlobj);
		}

		// write num batch
		Object[] numWordNos = numIvlBatch.keySet().toArray();
		for (int i = 0; i < numWordNos.length; i++) {

			wordNo = (Integer)numWordNos[i];
			ivlobj = (IVL)numIvlBatch.get(wordNo);
			numIVLMgr.addIVL (ivlobj);
		}

		// write dtd batch
		Object[] dtdWordNos = dtdIvlBatch.keySet().toArray();
		for (int i = 0; i < dtdWordNos.length; i++) {

			wordNo = (Integer)dtdWordNos[i];
			ivlobj = (IVL)dtdIvlBatch.get(wordNo);
			dtdIVLMgr.addIVL (ivlobj);
		}


		// refresh 
		textIvlBatch.clear();
		elemIvlBatch.clear();
		numIvlBatch.clear();
		dtdIvlBatch.clear();
	}
    
    /**
     * Gather URLs of files whose names match certain pattern.
     *
     * @parameter fp 
     *		the root of the File directory
     * @parameter urls
     *		the resultant URLs after walking the directory tree
     * @parameter beginPattern
     *		if not null, the name of qualified file must match it
     * @parameter endPattern
     *		if not null, the name of qualified file must match it
     */
    private void gatherFiles(File fp, Vector urls,
	String beginPattern, String endPattern) {

	if (fp.isFile()) {
	    String filename = fp.getName();
	    boolean add = true;
	    if (beginPattern!=null && !filename.startsWith(beginPattern)){
		add = false;
	    }
	    if (endPattern!=null && !filename.endsWith(endPattern)) {
		add = false;
	    }
	    if (add) {
		try {
		    urls.addElement(fp.toURL());
		}
		catch (MalformedURLException e) {
		    System.out.print("IndexMgr: warning ");
		    System.out.println(e);
		}
	    }
	}
	else if (fp.isDirectory()) {
	    File[] subs = fp.listFiles();
	    for (int i = 0; i < subs.length; i++) {
		gatherFiles(subs[i], urls, beginPattern, endPattern);
	    }
	}
    }
}
