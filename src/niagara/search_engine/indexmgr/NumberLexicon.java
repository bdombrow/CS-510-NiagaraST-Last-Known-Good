
/**********************************************************************
  $Id: NumberLexicon.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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
public class NumberLexicon extends Lexicon {
	private TreeMap lexicon = null; // vector of NumberLexiconEntry's

	private boolean DEBUG=false;

	/**
	 * Constructor. load in lexicon and inverted lists.
	 */
	public NumberLexicon (String lfname) throws IMException {
	
		super(lfname);

		System.out.print("Lexicon: loading from persisted data....");
		long beginTime = System.currentTimeMillis();

		// read in and parse the whole lexicon into memory
		DataInputStream dis = null;
		try {
			FileInputStream lexiconfi = new FileInputStream (lexiconFileName);
			GZIPInputStream gzis = new GZIPInputStream(lexiconfi);
			dis = new DataInputStream(gzis);

			lexicon = new TreeMap();

			while (true) {
				// read number
				double number = dis.readDouble();

				LexiconEntry lentry = new LexiconEntry();
				lentry.parse(dis);

				lexicon.put (new Double(number), lentry);

				// maintain the word number set
				int wordno = lentry.getWordNum();
				wordNumSet.add (new Integer(wordno));
				nextWordNum = wordno > nextWordNum? (wordno+1):nextWordNum;
			}
		}
		catch (EOFException e) {
			try { dis.close(); }
			catch (IOException e2) { e2.printStackTrace(); }
		}
		catch (FileNotFoundException e) {
			// no persistent entries yet
			// system must be used the first time.
			if (lexicon == null) lexicon = new TreeMap();
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

			Object[] allNumbers = lexicon.keySet().toArray();
			for (int i = 0; i < allNumbers.length; i++) {

				Double numObj = (Double)allNumbers[i];
				LexiconEntry lentry = (LexiconEntry)lexicon.get(numObj);

				// write number
				dos.writeDouble (numObj.doubleValue());

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
	 * Add a number to lexicon.
	 * If the number does not yet exist in the lexicon, a new word number
	 * is assigned to it; if the number already exists, its word number is
	 * returned.
	 *
	 * @return the word number for the number
	 */
	public int add (WordEntry we, long docno) throws IMException {

		Double numObj;
		try {
			numObj = new Double(we.getWord());
		}
		catch (NumberFormatException e) {
			throw new IMException();
		}

		LexiconEntry lentry = (LexiconEntry)lexicon.get(numObj);

		// new number
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
			lexicon.put (numObj, lentry);
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
	public int getWordNum (double num) {
		LexiconEntry lentry = (LexiconEntry)lexicon.get(new Double(num));
		if (lentry == null) return -1;
		return lentry.getWordNum();
	}

	/**
	 * Get array of word numbers whose corresponding numbers satisfy 
	 * the numeric condition
	 */
	public int[] getWordNums(int op, double value) {

		Object[] allEntries = null;
		int[] wordNums = null;

		switch (op) {
		case sym.EQ: {
			LexiconEntry lentry = (LexiconEntry)lexicon.get(new Double(value));
			if (lentry == null) return null;
			
			wordNums = new int[1];
			wordNums[0] = lentry.getWordNum();
			return wordNums;
		}
		case sym.LT: {
			SortedMap sm = lexicon.headMap (new Double(value));
			if (sm == null) return null;

			allEntries = sm.values().toArray();
			wordNums = new int[allEntries.length];

			for (int i = 0; i < allEntries.length; i++) {
				wordNums[i] = ((LexiconEntry)allEntries[i]).getWordNum();
			}

			return wordNums;
		}
		case sym.GT: {
			SortedMap submap = lexicon.tailMap (new Double(value));
			if (submap == null) return null;

			// since the tailMap() returns all keys that are greater
			// than or equal to the specified value, we need to do more work
			Object[] keys = submap.keySet().toArray();
			int i = 0;
			for (; i < keys.length; i++) {
				Double akey = (Double)keys[i];
				if (akey.doubleValue() > value)
					break;
				else if (akey.doubleValue() < value) {
					System.err.println ("How can this value be less than"
										+value+"!!!");
					System.exit(1);
				}
			}

			if (i >= keys.length) {
				// did not find any number that is strictly greater than 
				// the specified value
				return null;
			}

			// get the results

			allEntries = null;
			if (i == 0) {
				allEntries = submap.values().toArray();
			}
			else {
				// need to narrow down further
				SortedMap submap2 = submap.tailMap (keys[i]);
				allEntries = submap2.values().toArray();
			}

			wordNums = new int[allEntries.length];
			for (int j = 0; j < allEntries.length; j++) {
				wordNums[j] = ((LexiconEntry)allEntries[j]).getWordNum();
			}

			return wordNums;
		}
		case sym.LEQ: {
			// numbers that satisfy this condition are union of those
			// numbers that satisfy condition EQ and LT, thus we can
			// combine the results of these two cases

			// EQ case
			LexiconEntry eqEnt = (LexiconEntry)lexicon.get(new Double(value));

			// LT case
			SortedMap submap = lexicon.headMap (new Double(value));

			if (eqEnt == null && submap == null)
				return null;

			Object[] submapEntries = submap.values().toArray();
			wordNums = new int
				[eqEnt==null ? submapEntries.length : submapEntries.length+1];

			int i = 0;
			for (; i < submapEntries.length; i++) {
				wordNums[i] = ((LexiconEntry)allEntries[i]).getWordNum();
			}
			if (eqEnt != null) {
				wordNums[i] = eqEnt.getWordNum();
			}

			return wordNums;
		}
		case sym.GEQ: {
			SortedMap submap = lexicon.tailMap (new Double(value));
			if (submap == null) return null;

			allEntries = submap.values().toArray();
			wordNums = new int[allEntries.length];
			for (int i = 0; i < allEntries.length; i++) {
				wordNums[i] = ((LexiconEntry)allEntries[i]).getWordNum();
			}

			return wordNums;
		}
		default:
			return null;

		} // switch
	}
  
	/**
	 * Print all numbers in lexicon in decreasing order of frequency
	 *
	 * Sort the numbers according to their frequencies first.
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
		Object[] allNumbers = lexicon.keySet().toArray();
		for (int i = 0; i < allNumbers.length; i++) {

			Double numobj = (Double)allNumbers[i];
			LexiconEntry lentry = (LexiconEntry)lexicon.get(numobj);
			totalWordCount++;

			if (lentry == null) {
				System.out.println("WARNING: null LexiconEntry for "
								   +numobj);
				continue;
			}

			totalFrequency += lentry.getFrequency();
			totalDocFrequency += lentry.getDocFrequency();

			// insert this entry in 'lexes'
			lexes.put (lentry, numobj);
		}

		//
		// Now print them out
		//
		Object[] allEntries = lexes.keySet().toArray();
		for (int i = 0; i < allEntries.length; i++) {
			LexiconEntry lentry = (LexiconEntry) allEntries[i];
			System.out.println((i+1)+"\t"+lexes.get(lentry)+": "+lentry);
		}
		System.out.println ("SUMMARY: total numbers: "+totalWordCount
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

		Object[] allNumbers = lexicon.keySet().toArray();
		for (int i = 0; i < allNumbers.length; i++) {

			Double numObj = (Double)allNumbers[i];
			LexiconEntry lentry = (LexiconEntry)lexicon.get(numObj);
			totalWordCount++;

			if (lentry == null) {
				System.err.println("WARNING: null LexiconEntry for " +numObj);
				continue;
			}

			totalFrequency += lentry.getFrequency();
			totalDocFrequency += lentry.getDocFrequency();

			int wordno = lentry.getWordNum();
			try {
				Vector ivl = ivlmgr.getInvertedList (wordno);

				// print
				ps.println (numObj+": "+ivl);
			}
			catch (Exception e) {
				System.err.println (e);
			}
		}
	}

	/**
	  * @return an array of Double's
	 */
	public Object[] getAllNumbers () {
		return lexicon.keySet().toArray();
	}

}

