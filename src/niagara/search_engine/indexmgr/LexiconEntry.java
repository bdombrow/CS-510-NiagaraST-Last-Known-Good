
/**********************************************************************
  $Id: LexiconEntry.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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

import java.io.*;
import java.util.zip.*;

/**
 */
public class LexiconEntry {
	private int wordno;
	private int frequency;
	private int docFrequency;
	private long lastDocNo;  // we assume that the inverted list is
							// sorted according to docno, and thus the
							// lastDocNo is the largest docno
	/**
	 * Constructor
	 */
	public LexiconEntry() {
		wordno = -1;
		frequency = 0;
		docFrequency = 0;
		lastDocNo = -1;
	}

	/**
	 * Constructor
	 */
	public LexiconEntry (int wno, long docno) {
		wordno = wno;
		frequency = 1;
		docFrequency = 1;
		lastDocNo = docno;
	}

	/**
	 * Parse a LexiconEntry from persisted file
	 *
	 * @param dis the persisted file to parse from.
	 * @return number of bytes parsed, 0 if end of stream reached
	 */
	public int parse(DataInputStream dis) throws IOException {

		try {
			// word number, 4 bytes
			wordno = dis.readInt();

			// word frequency, 4 bytes
			frequency = dis.readInt();

			// doc frequency, 4 bytes
			docFrequency = dis.readInt();
		}
		catch (EOFException e) {
			return 0;
		}

		return 12;
	}

	/**
	 * Persist a LexiconEntry to a file
	 *
	 * @param fo the file to persist, must be open already
	 */
	public void persist(DataOutputStream dos) throws IOException {

		// word number, 4 bytes
		dos.writeInt (wordno);

		// word frequency, 4 bytes
		dos.writeInt (frequency);

		// doc frequency, 4 bytes
		dos.writeInt (docFrequency);
	}

	/**
	 */
	public int getWordNum() { return wordno; }
	/**
	 */
	public int getFrequency() { return frequency; }
	/**
	 */
	public int getDocFrequency() { return docFrequency; }
	/**
	 */
	public void increFrequency() { frequency++; }
	/**
	 */
	public void increDocFrequency (long docno) {
		if (docno!=lastDocNo) {
			docFrequency++;
			lastDocNo = docno;
		}
	}
	/**
	 */
	public String toString() {
		return new String("wordno="+wordno
						  +", frequency="+frequency
						  +", docfrequency="+docFrequency);
	}
}

