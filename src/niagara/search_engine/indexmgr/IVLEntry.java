
/**********************************************************************
  $Id: IVLEntry.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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

import java.util.Vector;
import java.io.*;

/**
 * Inverted list entry. The in-memory form of an inverted list entry
 * depends on the type of index.
 *
 * For full-text index, an inverted list entry is
 * <docno, position-list>. The on-disk form of an entry is 
 * <docno, numpos, position-list>.
 *
 * For doc-type index, an inverted list entry is just <docno>.
 *
 * Each Position is either a) a word no, or b) a pair of word no's.
 * a) is used for regular text words, b) is used for elements, and
 * the pair represents the begin and end word number of the element.
 *
 */

public class IVLEntry {

	private long docNo;
	private Vector positionList = null;

	private static final boolean DEBUG=false;

	/**
	 * The default constructor. All data members are set to illegal
	 * values.  This constructor must be followed by setter functions 
	 * later to set the data members.
	 *
	 * This is Mainly used when parsing from the persistant index.
	 */
	public IVLEntry () {
		docNo = -1;
		positionList = null;
	}

	/**
	 * Constructor. A position list is created.
	 */
	public IVLEntry(long docno, Position pos) {
		docNo = docno;
		positionList = new Vector();
		positionList.addElement (pos);
	}

	/**
	 * Constructor
	 */
	public IVLEntry(long docno, Vector pl) {
		docNo = docno;
		positionList = pl;
	}

	/**
	  * Constructor. This should be used for doc-type index.
	  */
	public IVLEntry (long docno) {
		docNo = docno;
		positionList = null;
	}

	/**
	 * Copy constructor
	 */
	public IVLEntry (IVLEntry other) throws IMPositionException {
		docNo = other.getDocNo();

		if (other.getPositionList() != null) {
			positionList = new Vector();

			Vector pl = other.getPositionList();

			for (int i = 0; i < pl.size(); i++) {
				positionList.addElement(
					new Position((Position)pl.elementAt(i)));
			}
		}
    }

    /**
     */
    public long getDocNo() { return docNo; }

    /**
     */
    public Vector getPositionList() { return positionList; }

    /**
     */
    public void setDocNo(long docno) { docNo = docno; }

    /**
     */
    public void setPositionList(Vector plist) { positionList = plist; }

	/**
	 * Read in and parse an inverted list entry from a file
	 *
	 * @param fi      the open stream to read the inverted list from.
	 *                it could either be a RandomAccessFile or DataInputStream
	 * @param ptype   position type--so we know how many bytes to read
	 *                for a position
	 * @return -1 if end of stream reached
	 */
	public int parse (Object ins, byte ptype) throws IOException {

		int numPos = 0;

		try {
			if (ins instanceof RandomAccessFile) {
				// docno
				docNo = ((RandomAccessFile)ins).readLong();
				if (DEBUG) System.out.println ("parse: read docno="+docNo);

				if (IndexMgr.idxmgr.indexType == IndexMgr.INDEX_T_FULL) {
					// number of positions
					numPos = ((RandomAccessFile)ins).readInt();
					if (DEBUG) System.out.println ("parse: read npos="+numPos);
				}
			}
			else if (ins instanceof DataInputStream) {
				// docno
				docNo = ((DataInputStream)ins).readLong();
				if (DEBUG) System.out.println ("parse: read docno="+docNo);

				if (IndexMgr.idxmgr.indexType == IndexMgr.INDEX_T_FULL) {
					// number of positions
					numPos = ((DataInputStream)ins).readInt();
					if (DEBUG) System.out.println ("parse: read npos="+numPos);
				}
			}
			else {
				throw new IOException ("wrong input stream");
			}

			// position list
			if (IndexMgr.idxmgr.indexType == IndexMgr.INDEX_T_FULL) {
				positionList = new Vector();
				for (int i = 0; i < numPos; i++) {
					Position pos = new Position();
					pos.parse (ins, ptype);
					positionList.addElement(pos);
					if (DEBUG) System.out.println ("parse: read pos="+pos);
				}
			}
		}
		catch (EOFException e) {
			return -1;
		}

		return 0;
	}

	/**
	 * Parse an IVLEntry from a byte array
	 * @return number of bytes parsed
	 */
	public int parse (byte[] data, int offset, byte postype) {

		int off = offset;

		docNo = Util.toLong(data, off);
		off += 8;

		if (IndexMgr.idxmgr.indexType == IndexMgr.INDEX_T_FULL) {
			int numpos = Util.toInt(data, off);
			off += 4;

			positionList = null;
			positionList = new Vector();
			for (int i = 0; i < numpos; i++) {
				Position pos = new Position();
				off += pos.parse (data, off, postype);
				positionList.addElement(pos);
			}
		}
		return (off-offset);
	}

	/**
	 * Write this inverted list entry to a file
	 *
	 * @param fo  the open stream to write. it should already be open.
	 *            the stream could either be RandomAccessFile or
	 *            DataOutputStream.
	 * @return number of bytes written
	 */
	public int writeToFile (Object out) throws IOException {

		//if (positionList.size()==0) return 0;

		int bytesWritten = 0;

		if (out instanceof RandomAccessFile) {

			((RandomAccessFile)out).writeLong (docNo);
			bytesWritten += 8;
			if (DEBUG) System.out.println ("writeToFile: wrote docno="+docNo);

			if (positionList != null) {
				((RandomAccessFile)out).writeInt (positionList.size());
				bytesWritten += 4;
				if (DEBUG) System.out.println (
					"writeToFile: wrote npos="+positionList.size());
			}
		}
		else if (out instanceof DataOutputStream) {

			((DataOutputStream)out).writeLong(docNo);
			bytesWritten += 8;
			if (DEBUG) System.out.println ("writeToFile: wrote docno="+docNo);

			if (positionList != null) {
				((DataOutputStream)out).writeInt (positionList.size());
				bytesWritten += 4;
				if (DEBUG) System.out.println (
					"writeToFile: wrote npos="+positionList.size());
			}
		}
		else {
			throw new IOException ("wrong output stream");
		}

		// position list
		if (positionList != null) {
			for (int i = 0; i < positionList.size(); i++) {
				Position pos = (Position)positionList.elementAt(i);
				bytesWritten += pos.writeToFile(out);
				if (DEBUG) System.out.println ("writeToFile: wrote pos="+pos);
			}
		}

		return bytesWritten;
	}

	/**
	 * Write to a byte array
	 * @return number of bytes written
	 */
	public int write (byte[] buf, int offset) {

		int off = offset;

		// docno, number of positions
		Util.writeLong (docNo, buf, off);
		off += 8;

		// position list
		if (positionList != null) {

			Util.writeInt(positionList.size(), buf, off);
			off += 4;

			for (int i = 0; i < positionList.size(); i++) {
				Position pos = (Position)positionList.elementAt(i);
				off += pos.write (buf, off);
			}
		}
		return (off-offset);
	}

	/**
	 * Convert this entry to printable form
	 */
	public String toString() {
		String ret = "<"+docNo;

		if (positionList != null) {
			ret += ";";

			for (int i = 0; i < positionList.size(); i++) {
				ret += " "+(Position)positionList.elementAt(i);
			}
		}

		ret += ">";
		return ret;
	}

	/**
	 * Combine with another entry of the same doc. Merge the positons
	 * together.
	 */
	public void merge (IVLEntry other) {

		if (other==null || other.getPositionList()==null) return;
		if (getDocNo() != other.getDocNo()) return;

		if (positionList == null) {
			positionList = other.getPositionList();
			return;
		}

		try {
			Vector resultv = new Vector();
			Vector poslist1 = getPositionList();
			Vector poslist2 = other.getPositionList();
			int i = 0;
			int j = 0;
			while (i < poslist1.size() && j < poslist2.size()) {
				Position pos1 = (Position)poslist1.elementAt(i);
				Position pos2 = (Position)poslist2.elementAt(j);

				if (pos1.getType() != pos2.getType()) return;
				int cp = pos1.compareTo(pos2);
				if (cp==0) {
					resultv.addElement(pos1);
					i++;
					j++;
				}
				else if (cp < 0) {
					resultv.addElement(pos1);
					i++;
				}
				else {
					resultv.addElement(pos2);
					j++;
				}
			}
			while (i < poslist1.size()) {
				Position pos1 = (Position)poslist1.elementAt(i);
				resultv.addElement(pos1);
				i++;
			}
			while (j < poslist2.size()) {
				Position pos2 = (Position)poslist2.elementAt(j);
				resultv.addElement(pos2);
				j++;
			}
			positionList = null;
			positionList = resultv;
		}
		catch (IMPositionException e) {
			System.out.println ("Warning: " + e);
		}
	}
}
