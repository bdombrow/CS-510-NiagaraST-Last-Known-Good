
/**********************************************************************
  $Id: IVLMgr.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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
import java.util.*;
import java.util.zip.*;

/**
 * The class IVLMgr manages a set of InvertedFiles, and provides an 
 * interface to get all the inverted lists
 *
 * The persisted IVLMap contains (number in bytes):
 *     positionType(1), numInvertedFiles(4),
 *     [wordno(4), fileno(4),..., wordno, fileno(4)]
 * The wordno-fileno map is only recorded for 'flat' files.
 * It is not recorded for 'paged' files.
 *
 * An IVLMgr only manages inverted lists of the same type, that is,
 * it manages text/word inverted lists, or element inverted
 * lists, or dtd inverted lists, but not any combination.
 *
 */

public class IVLMgr {
	public static final byte MGRT_TEXT = 0x00;
	public static final byte MGRT_ELEM = 0x01;
	public static final byte MGRT_DTD = 0x02;
	public static final byte MGRT_NUM = 0x03;

	private byte ivlmgrType = MGRT_TEXT;
	private String ivlDirName;
	private byte invertedFileType = InvertedFile.IVFT_FLAT;
	private byte positionType;

	private int pagedIVLFileSize, pagedIVLFilePageSize;
	private PagedInvertedFile pagedIVLFile = null;

	private int ivlBufSize = 0;
			// This size is in number of positions. Thus, at most 'ivlBufSize'
			// number of positions can be in memory at a time. A size of -1
			// means infinite buffering--everything is kept in memory.

	private Hashtable ivlBuf = null; // wordno --> IVL
			// Hashtable of IVLBufEntries. The total number of positions in
	        // the in-memory inverted lists should not exceed 'ivlBufSize'.

	private int totalPosInBuf = 0; // total number of positions in buf
	private int totalListInBuf = 0; // total number of words/lists in buf
	private Vector sortedBufIVLs = null;
    //private Vector ageIVLVector = null;
			// vector of wordnos sorted on decreasing age,
			// or increasing order of IVL.lastAccessed

	private long totalUpdateTime = 0;
	private long totalOpenCloseTime = 0;
	private long totalFileWriteTime = 0;
	private static boolean PERSIST_DEBUG=false;
	private static boolean BUF_DEBUG=false;


	/**
	 * Constructor.
	 *
	 * @param mgrtype		the type of the manager
	 * @param ivldir		the directory to put the inverted lists
	 * @param ivftype		the structure of the inverted file.
	 *						"flat" or "paged"
	 * @param postype     the type of positions in the InvertedFile
	 * @param bufsize     the size of buffer, in BYTES
	 */
	public IVLMgr(byte mgrtype, String ivldir, byte ivftype,
				  byte postype, int bufsize) throws IMException {

		ivlmgrType = mgrtype;
		ivlDirName = ivldir;
		invertedFileType = ivftype;
		positionType = postype;

		// buffering
		ivlBufSize = bufsize;
		if (ivlBufSize != 0) {
			ivlBuf = new Hashtable();
			sortedBufIVLs = new Vector();
		}

		/////// create directory structure
		System.out.println(ivlmgrName(ivlmgrType)
			+": creating directory structure....");
		File fp = new File(ivldir);
		if (fp==null) throw new IMException("Can't find directory "+ivldir);
		createDirectory (fp);

		/////// create paged file if necessary
		if (invertedFileType == InvertedFile.IVFT_PAGED) {

			pagedIVLFilePageSize = Integer.parseInt(
									System.getProperty("se.index.pagesize"));
			switch (ivlmgrType) {
			case MGRT_TEXT:
				pagedIVLFileSize = Integer.parseInt(System.getProperty(
					"se.textivl.pagedfilesize")) * 1024*1024;
				break;
			case MGRT_ELEM:
				pagedIVLFileSize = Integer.parseInt(System.getProperty(
					"se.elemivl.pagedfilesize")) * 1024*1024;
				break;
			case MGRT_DTD:
				pagedIVLFileSize = Integer.parseInt(System.getProperty(
					"se.dtdivl.pagedfilesize")) * 1024*1024;
				break;
			case MGRT_NUM:
				pagedIVLFileSize = Integer.parseInt(System.getProperty(
					"se.numivl.pagedfilesize")) * 1024*1024;
				break;
			default:
				pagedIVLFileSize = -1;
			}
		
			pagedIVLFile = new PagedInvertedFile(
				getAbsolutePathName(ivldir, 0),
				0, positionType, pagedIVLFileSize, pagedIVLFilePageSize);
		}
	}

	/**
	 * Persist the ivl map info to disk
	 */
	public void persist() {
		if (PERSIST_DEBUG) System.out.println ("\nPERSISTING IVL MAP...");
		System.out.print (ivlmgrName(ivlmgrType)
			+": persisting data to disk...\n");

		////// flush in-memory inverted lists 
		try {
			if (ivlBuf != null) {
				long beforeTime = System.currentTimeMillis();

				Enumeration wordnos = ivlBuf.keys();
				while (wordnos.hasMoreElements()) {
					Integer wordnoObj = (Integer)wordnos.nextElement();
					writeIVLToFile ((IVL)ivlBuf.get(wordnoObj));
				}

				long afterTime = System.currentTimeMillis();
				totalUpdateTime += afterTime - beforeTime;
			}
		}
		catch (IMException e) {
			System.out.print("Warning from IVLMgr: ");
			System.out.println("problem persisting in-memory ivl: ");
			System.out.println(e);
		}

		////// persist inverted file meta info
		if (invertedFileType == InvertedFile.IVFT_PAGED) {
			((PagedInvertedFile)getInvertedFile(0)).persist();
		}
	}

	/**
	 * Add new entry to the inverted list of a word.
	 */
	public void addIVL (IVL ivlobj) throws IMException {

		long beginT = System.currentTimeMillis();

		// there is buffering
		if (ivlBuf != null) {
			Integer wordnoObj = new Integer(ivlobj.getWordNo());

			IVL bufivl = (IVL)ivlBuf.get(wordnoObj);

			// the word is not in the buffer
			if (bufivl == null) {
				ivlBuf.put (wordnoObj, ivlobj);
				totalListInBuf++;
			}
			else {
				bufivl.merge (ivlobj);
				sortedBufIVLs.remove (bufivl);
			}
			totalPosInBuf += ivlobj.getNumPosInIVL();
			addSortedBufIVL (bufivl);

			//updateAgeVector(bufivl);
			resizeBuf ();
		}
		else {
			// there is no buffering
			writeIVLToFile (ivlobj);
		}

		long endT = System.currentTimeMillis();
		totalUpdateTime += endT - beginT;
	}

	/**
	 * Get the inverted list of a word.
	 *
	 * TODO:
	 * Currently, this method always goes to the file to retrieve
	 * the inverted list, even if the whole list is buffered and
	 * can be returned directly from memory.
	 * Some optimization could be used to determine that the list can
	 * be found in its entirety in the memory and therefore the file
	 * I/O could be avoided.
	 *
	 * @return null if the word does not exist in index
	 */
	public Vector getInvertedList(int wordno) 
		throws IMException, IOException {

		//System.out.print(ivlmgrName(ivlmgrType)+": getting list of wordno "
		//	+wordno+"...");
		long beginT = System.currentTimeMillis();

		Vector resultv = null;
		Integer wordnoObj = new Integer(wordno);

		// get from buffer
		IVL bufivlobj = null;
		if (ivlBuf != null) {
			bufivlobj = (IVL)ivlBuf.get(wordnoObj);
		}

		// get from file
		IVL fileivlobj = null;
		InvertedFile ivlfile = getInvertedFile (wordno); // fileno is wordno
		if (ivlfile == null) {
			return bufivlobj==null? null : bufivlobj.getList();
		}

		fileivlobj = new IVL(wordno);
		ivlfile.retrieveList (fileivlobj);

		if (bufivlobj != null) {
			fileivlobj.merge (bufivlobj);
		}
		// do not put it in buf, just return directly
		resultv = fileivlobj.getList();

			/*
			// there is no buffering
			if (ivlBuf == null) {
				resultv = fileivlobj.ivl;
			}
			else {
				// there is buffering: two cases:
				// 1. this list is not buffered, add it in buffer
				// 2. some part of the list is already buffered, merge
				if (bufivlobj == null) { // case 1
					ivlBuf.put (wordnoObj, fileivlobj);
					bufivlobj = fileivlobj;
					totalListInBuf++;
				}
				else { // case 2
					bufivlobj.merge (fileivlobj);
				}
				totalPosInBuf += fileivlobj.getNumPosInIVL();
				updateAgeVector(bufivlobj);
				resizeBuf (bufivlobj);

				if (BUF_DEBUG) {
					//System.out.println(ivlBuf);
					//System.out.println(ageIVLVector);
					System.out.println(ivlmgrName(ivlmgrType)
						+": put list of wordno "+wordno+" in buf");
				}
				fileivlobj = null;
				wordnoObj = null;
				resultv = bufivlobj.ivl;
			}
			*/

		long endT = System.currentTimeMillis();
		System.out.println ("...took " + (endT-beginT) + " millisec");
		return resultv;
	}

	/**
	 */
	public long getTotalUpdateTime() {
		return totalUpdateTime;
	}

    /////////////////////  private member functions /////////////////

	/**
	 * Given a file number, get the path name for the file
	 */
	private static final String getAbsolutePathName (
		String dirName, int fileno) {

		String pathname = null;
		pathname = dirName + "/ivf" + fileno;
		return pathname;
	}

	/**
	 * Get the InvertedFile given its number
	 */
    private InvertedFile getInvertedFile (int fileno) {
		InvertedFile ivlfile = null;
		if (invertedFileType == InvertedFile.IVFT_FLAT) {
			try {
				ivlfile = new FlatInvertedFile (
						getAbsolutePathName(ivlDirName, fileno),
						fileno, positionType);
			}
			catch (Exception e) {
				ivlfile = null;
			}
		}
		else {
			// this must be a paged file.
			ivlfile = pagedIVLFile;
		}
		return ivlfile;
	}

	/**
	 * Create a directory if it is not yet there
	 * If the path represents a file, the file is removed and a
	 * directory is created at its place.
	 */
	private void createDirectory (File fp) throws IMException {

		if (fp.exists() && !fp.isDirectory()) {
			if (!fp.delete() || !fp.mkdir()) {
				throw new IMException("Can't create directory "
					+fp.getAbsolutePath());
			}
		}
		if (!fp.exists()) {
			if (!fp.mkdir()) 
				throw new IMException("Can't create directory "
					+fp.getAbsolutePath());
		}
	}

	/**
	 * Write an inverted list to file. Only the dirty entries are
	 * written to file. Clean ones are not.
	 *
	 * @param ivlobj the inverted list to add
	 */
	private void writeIVLToFile(IVL ivlobj) throws IMException {

		if (ivlobj == null) return;

		InvertedFile ivlfile = getInvertedFile (ivlobj.getWordNo());
		if (ivlfile == null) {
			System.err.println ("Cannot writeIVLToFile: ivlfile==null!");
			throw new IMException();
		}
		ivlfile.insertList (ivlobj);
	}

	/**
	 * Resize buffer such that the total number of positions in memory
	 * does not exceed ivlBufSize.
	 */
	private void resizeBuf() throws IMException {
		if (ivlBufSize == 0) {
			System.out.println("bug: resizeBuf() called when no buffer");
			System.exit(1);
		}
		if (ivlBufSize == -1) return; // infinite memory
		if (totalPosInBuf <= ivlBufSize) return; // nothing to worry about

		if (BUF_DEBUG && ivlmgrType==MGRT_ELEM) {
			System.out.println (ivlmgrName(ivlmgrType)+": buf overflown by "
				+(totalPosInBuf-ivlBufSize)+" positions");
		}

		//
		// Method 1: flush all
		//
		Enumeration wordNos = ivlBuf.keys();
		while (wordNos.hasMoreElements()) {
			Integer wordNo = (Integer)wordNos.nextElement();
			IVL bufivl = (IVL)ivlBuf.get (wordNo);
			writeIVLToFile (bufivl);
			totalPosInBuf -= bufivl.getNumPosInIVL();
			bufivl.clear();
		}
		if (totalPosInBuf != 0) {
			System.err.println ("wrong totalPosInBuf!");
			System.exit(1);
		}
		//
		// Method 2: flush longest one
		//
/*
		// just a check
		if (sortedBufIVLs.size() != totalListInBuf) {
			System.err.println("inconsistent sorted vector");
			System.exit(1);
		}
		if (BUF_DEBUG && ivlmgrType==MGRT_ELEM) {
			System.out.print ("sortedBufIVLs: ");
			for (int i = 0; i < sortedBufIVLs.size(); i++) {
				IVL ivlelem = (IVL)sortedBufIVLs.elementAt(i);
				System.out.println ("    wordno: "+ivlelem.wordno
					+ ", numPosInIVL: "+ivlelem.getNumPosInIVL());
			}
			System.out.println("");
		}

		while (totalPosInBuf > ivlBufSize) {
			//Integer wordnoObj = (Integer)ageIVLVector.remove(0);
			IVL oldivl = (IVL)sortedBufIVLs.elementAt(
				sortedBufIVLs.size()-1);
			if (oldivl == null) {
				System.err.println("bug: inconsistent ivlbuf and sorted vector");
				System.exit(1);
			}

			if (BUF_DEBUG && ivlmgrType==MGRT_ELEM) {
				System.out.println(ivlmgrName(ivlmgrType)
					+": Clearing list of wordno "
					+oldivl.wordno + " of length "+oldivl.getNumPosInIVL()
					+" positions");
			}
			writeIVLToFile (oldivl);
			totalPosInBuf -= oldivl.getNumPosInIVL();
			oldivl.clear();
			sortedBufIVLs.removeElement(oldivl);
			sortedBufIVLs.insertElementAt(oldivl, 0);
		}

		if (BUF_DEBUG && ivlmgrType==MGRT_ELEM) {
			System.out.print ("sortedBufIVLs: ");
			for (int i = 0; i < sortedBufIVLs.size(); i++) {
				IVL ivlelem = (IVL)sortedBufIVLs.elementAt(i);
				System.out.println ("    wordno: "+ivlelem.wordno
					+ ", numPosInIVL: "+ivlelem.getNumPosInIVL());
			}
			System.out.println("");
		}
*/
	}

	/**
	 * sort ivls in the buf in the increasing order of list size
	 */
	private void addSortedBufIVL (IVL ivlobj) {
		if (sortedBufIVLs.size() == 0) {
			sortedBufIVLs.addElement (ivlobj);
			return;
		}
		// an optimization
		IVL biggest = (IVL)sortedBufIVLs.elementAt (
			sortedBufIVLs.size()-1);
		if (ivlobj.getNumPosInIVL() >= biggest.getNumPosInIVL()) {
			sortedBufIVLs.addElement (ivlobj);
			return;
		}

		int low = 0, high = sortedBufIVLs.size()-1;
		while (low <= high) {
			int mid = (low+high)/2;
			IVL ivlelem = (IVL)sortedBufIVLs.elementAt(mid);
			if (ivlobj.getNumPosInIVL() >= ivlelem.getNumPosInIVL()) {
				low = mid+1;
			}
			else if (ivlobj.getNumPosInIVL() < ivlelem.getNumPosInIVL()) {
				high = mid-1;
			}
		}
		sortedBufIVLs.insertElementAt (ivlobj, low);
	}

	private final String ivlmgrName(byte type) {
		switch (type) {
		case MGRT_TEXT: return "TEXT_IVLMGR";
		case MGRT_ELEM: return "ELEM_IVLMGR";
		case MGRT_DTD: return "DTD_IVLMGR";
		case MGRT_NUM: return "NUM_IVLMGR";
		default: return "UNKNOWN_IVLMGR";
		}
	}

}
