
/**********************************************************************
  $Id: TMLexicon.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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
import java.io.*;
import java.util.zip.*;
import niagara.search_engine.seql.sym;

/**
 */
public class TMLexicon extends Lexicon {
	private Hashtable lexicon = null; // word(String)-->LexiconEntry

	private boolean DEBUG=false;

	/**
	 * Constructor. load in lexicon and inverted lists.
	 */
	public TMLexicon (String lfname) throws IMException {
	
		super(lfname);

		System.out.print("Lexicon: loading from persisted data....");
		long beginTime = System.currentTimeMillis();

		// read in and parse the whole lexicon into memory
		DataInputStream dis = null;
		try {
			FileInputStream lexiconfi = new FileInputStream (lexiconFileName);
			GZIPInputStream gzis = new GZIPInputStream(lexiconfi);
			dis = new DataInputStream(gzis);

			lexicon = new Hashtable();

			String word = null;
			LexiconEntry lentry = null;
			while (true) {
				// word len, 2 bytes
				short wordlen = dis.readShort();

				// word, wordlen bytes
				byte[] buf = new byte[wordlen+1];
				dis.readFully(buf, 0, wordlen);
				word = new String(buf, 0, wordlen);

				lentry = new LexiconEntry();
				int bytes = lentry.parse(dis);

				lexicon.put (word, lentry);

				// maintain the word number set
				int wordno = lentry.getWordNum();
				wordNumSet.add (new Integer(wordno));
				nextWordNum = wordno > nextWordNum? (wordno+1):nextWordNum;
			}
		}
		catch (FileNotFoundException e) {
			// no persistent entries yet
			// system must be used the first time.
			if (lexicon == null) lexicon = new Hashtable();
		}
		catch (EOFException e) {
			try {
				dis.close();
			} catch (IOException e2) { e.printStackTrace(); }
		}
		catch (IOException e) {
			e.printStackTrace();
		}

		long endTime = System.currentTimeMillis();
		System.out.println("took "+(endTime-beginTime)+" millisecs");
	}

	/**
	 * Flush lexicon and inverted lists to disk
	 */
	public void persist() {
		System.out.print ("Lexicon: persisting data to disk....");
		long beginTime = System.currentTimeMillis();

		try {
			FileOutputStream lexiconfo = new FileOutputStream(lexiconFileName);
			GZIPOutputStream gzos = new GZIPOutputStream(lexiconfo);
			DataOutputStream dos = new DataOutputStream(gzos);

			Object[] allWords = lexicon.keySet().toArray();
			for (int i = 0; i < allWords.length; i++) {

				String word = (String)allWords[i];

				// word len, 2 bytes
				dos.writeShort((short)word.length());

				// word, wordlen bytes
				dos.writeBytes (word);

				LexiconEntry lentry = (LexiconEntry)lexicon.get(word);
				lentry.persist (dos);
			}
			dos.close();
		}
		catch (IOException e) {
			System.out.println(e);
		}

		long endTime = System.currentTimeMillis();
		System.out.println("took "+(endTime-beginTime)+" millisecs");
	}

	/**
	 * Add a word to lexicon.
	 * If the word does not yet exist in the lexicon, a new word number
	 * is assigned to it; if the word already exists, its word number is
	 * returned.
	 *
	 * @return the word number for the word
	 */
	public int add (WordEntry we, long docno) throws IMException {

		LexiconEntry lentry = (LexiconEntry)lexicon.get(we.getWord());

		// new word
		if (lentry==null) {
			// assign a word number
			int wordno = assignNextWordNum();
			if (wordno == -1) {
				String errstr = 
					"Lexicon Overflow. Failed to assign a word number.";
				System.err.println(errstr);
				throw new IMException(errstr);
			}
			lentry = new LexiconEntry(wordno, docno);
			lexicon.put (we.getWord(), lentry);
			/*
			if (we.getType()==WordEntry.WT_REGULAR
			|| we.getType()==WordEntry.WT_ATTRVALUE) { 
				System.out.println ("wordno of "+we.getWord()+" : "+wordno);
			}
			*/
			return wordno;
		}
		else {
			lentry.increFrequency();
			lentry.increDocFrequency(docno);
			return lentry.getWordNum();
		}
	}

	/**
	 * Get a word from lexicon
	 * @return the word's word number. -1 if not found
	 */
	public int getWordNum(String word) {
		LexiconEntry lentry = (LexiconEntry)lexicon.get(word.toLowerCase());
		if (lentry == null) return -1;
		return lentry.getWordNum();
	}
	public int getWordNum(String word, boolean respectWord) {
		LexiconEntry lentry = null;
		if (respectWord) {
			lentry = (LexiconEntry)lexicon.get(word);
		}
		else {
			lentry = (LexiconEntry)lexicon.get(word.toLowerCase());
		}
		if (lentry == null) return -1;
		return lentry.getWordNum();
	}

	/**
	 * Print all words in lexicon in decreasing order of frequency
	 *
	 * Sort words according to their frequencies first.
	 * then the entries are printed out in decreasing order of word
	 * frequency.
	 */
	public void print () {

		TreeMap lexes = new TreeMap(new LEFrequencyComparator());

		int totalWordCount = 0;
		int totalFrequency = 0;
		int totalDocFrequency = 0;

		//
		// Sort words according to frequencies
		//
		Object[] allWords = lexicon.keySet().toArray();
		for (int i = 0; i < allWords.length; i++) {

			String word = (String)allWords[i];
			LexiconEntry lentry = (LexiconEntry)lexicon.get(word);
			totalWordCount++;

			if (lentry == null) {
				System.out.println("WARNING: null LexiconEntry for " +word);
				continue;
			}

			totalFrequency += lentry.getFrequency();
			totalDocFrequency += lentry.getDocFrequency();

			// insert this entry in 'lexes'
			lexes.put (lentry, word);
		}

		//
		// Now print them out
		//
		Object[] allEntries = lexes.keySet().toArray();
		for (int i = 0; i < allEntries.length; i++) {
			LexiconEntry lentry = (LexiconEntry) allEntries[i];
			System.out.println((i+1)+"\t"+lexes.get(lentry)+": "+lentry);
		}
		System.out.println ("SUMMARY: total words: "+totalWordCount
			+", total frequency: "+totalFrequency
			+", total docfrequency: "+totalDocFrequency);

		lexes = null;
	}

	/**
	 * Print all inverted lists associated with this lexicon. 
	 */
	public void printAll (IVLMgr ivlmgr, PrintStream ps) {

		int totalWordCount = 0;
		int totalFrequency = 0;
		int totalDocFrequency = 0;

		Object[] allWords = lexicon.keySet().toArray();
		for (int i = 0; i < allWords.length; i++) {

			String word = (String)allWords[i];
			LexiconEntry lentry = (LexiconEntry)lexicon.get(word);
			totalWordCount++;

			if (lentry == null) {
				System.err.println("WARNING: null LexiconEntry for " +word);
				continue;
			}

			totalFrequency += lentry.getFrequency();
			totalDocFrequency += lentry.getDocFrequency();

			int wordno = lentry.getWordNum();
			try {
				Vector ivl = ivlmgr.getInvertedList (wordno);
				if (ivl == null) {
					System.out.println ("INVERTED LIST FOR "+word+" IS NULL!!");
					//System.exit(1);
				}

				// print
				try {
					ps.println (word+": "+ivl+"\n");
				}
				catch (NullPointerException e) {
					System.out.println("CAUGHT IT!!");
					System.exit(0);
				}
			}
			catch (Exception e) {
				System.err.println (e);
				/* ignore */ 
			}
		}
	}

	/**
	  * @return an array of String's
	 */
	public Object[] getAllWords() {
		return lexicon.keySet().toArray();
	}

}

