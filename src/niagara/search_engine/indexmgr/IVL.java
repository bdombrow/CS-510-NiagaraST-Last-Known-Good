
/**********************************************************************
  $Id: IVL.java,v 1.1 2000/05/30 21:03:27 tufte Exp $


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

/**
 * Encapsulation of an inverted list and related info.
 *
 */
public class IVL {
	private int wordno;
	private Vector ivl;      // the inverted list. vector of IVLEntry's
	private int numPosInIVL; // number of positions in IVL

	public IVL (int wordno) {
		this.wordno = wordno;
		ivl = null;
		numPosInIVL = 0;
	}

	public int getNumPosInIVL() {
		return numPosInIVL;
	}
	public int getWordNo() {
		return wordno;
	}
	public Vector getList() {
		return ivl;
	}
	public void add (IVLEntry ivlent) {

		if (ivl == null) ivl = new Vector();
		ivl.addElement (ivlent);

		Vector pl = ivlent.getPositionList();
		if (pl != null) {
			numPosInIVL += pl.size();
		}
	}

	/**
	 * Flatten into a byte array
	 */
	public byte[] flatten (byte postype) throws IMPositionException {

		// first calculate space needed

		int size;
		if (IndexMgr.idxmgr.indexType == IndexMgr.INDEX_T_FULL) {
			size = (8+4)*ivl.size(); // space for doc numbers and
									// number of positions in a doc
		}
		else {
			size = 8*ivl.size();  // space for doc numbers
		}

		if (numPosInIVL != 0) {
			size += numPosInIVL*Position.sizeof(postype); //space for positions
		}

		// now flatten

		byte[] data = new byte[size];
		int offset = 0;

		try {
			for (int i = 0; i < ivl.size(); i++) {
				IVLEntry entry = (IVLEntry)ivl.elementAt(i);
				offset += entry.write (data, offset);
				Util.assert (offset <= size);
			}
			Util.assert (offset == size);
		}
		catch (ArrayIndexOutOfBoundsException e) {
			//System.err.println ("allocated size="+size);
			//System.err.println ("trying to write "+this);
			//System.err.println ("numPosInIVL="+numPosInIVL);
			//System.err.println ("ivl.size()="+ivl.size());
			e.printStackTrace();
			System.exit (1);
		}
		
		return data;
	}

	/**
	 * Parse from a byte array
	 */
	public void parse(byte[] data, byte postype) {

		if (ivl == null) {
			ivl = new Vector();
			numPosInIVL = 0;
		}

		int offset = 0;
		while (offset < data.length) {
			IVLEntry entry = new IVLEntry();
			offset += entry.parse (data, offset, postype);

			ivl.addElement (entry);

			Vector pl = entry.getPositionList();
			if (pl != null) {
				numPosInIVL += entry.getPositionList().size();
			}
		}
	}

	/**
	 */
	public void clear() {
		ivl = null;
		ivl = new Vector();
		numPosInIVL = 0;
	}

	/**
	  * Delete the ivlentry associated with a document
	  */
	public void delete (long docno) {

		for (int i = 0; i < ivl.size(); i++) {
			IVLEntry ivlent = (IVLEntry)ivl.elementAt(i);
			if (ivlent.getDocNo() == docno) {
				ivl.remove(i);
				break;
			}
		}
	}

	/**
	 * Format to string for printout.
	 */
	public String toString() {
		String ret = new Integer(wordno).toString();
		if (ivl != null) {
			for (int i = 0; i < ivl.size(); i++) {
				IVLEntry ivlent = (IVLEntry)ivl.elementAt(i);
				ret += ivlent;
			}
		}
		return ret;
	}

	/**
	 * Merge with another inverted list of the same word.
	 * Merge position lists.
	 */
	public void merge (IVL other) {
		if (other==null || other.ivl==null || other.ivl.size()==0) return;
		if (wordno != other.wordno) {
			System.err.println("Merge different lists!!");
			System.exit(1);
		}
		if (ivl == null || ivl.size()==0) {
			ivl = other.ivl;
			numPosInIVL = other.numPosInIVL;
			return;
		}

		// Optimization: if the other list is ordered after this list,
		// we concatenate them together
		// We are able to do so because the two lists are themselves
		// sorted.

		IVLEntry thisLast = (IVLEntry)ivl.elementAt(ivl.size()-1);
		IVLEntry otherFirst = (IVLEntry)other.ivl.elementAt(0);

		if (thisLast.getDocNo() < otherFirst.getDocNo()) {
			for (int i = 0; i < other.ivl.size(); i++) {
				IVLEntry ent = (IVLEntry)other.ivl.elementAt(i);
				ivl.addElement (ent);

				Vector pl = ent.getPositionList();
				if (pl != null) {
					numPosInIVL += pl.size();
				}
			}
			return;
		}

		Vector resultv = new Vector();
		int resultPosInIVL = 0;
		int i = 0;
		int j = 0;

		while (i < ivl.size() && j < other.ivl.size()) {

			IVLEntry ivlent1 = (IVLEntry)ivl.elementAt(i);
			IVLEntry ivlent2 = (IVLEntry)other.ivl.elementAt(j);

			if (ivlent1.getDocNo() == ivlent2.getDocNo()) {

				ivlent1.merge(ivlent2);
				resultv.addElement(ivlent1);

				resultPosInIVL += ivlent1.getPositionList() == null?
							0 : ivlent1.getPositionList().size();
				i++;
				j++;
			}
			else if (ivlent1.getDocNo() < ivlent2.getDocNo()) {

				resultv.addElement(ivlent1);
				resultPosInIVL += ivlent1.getPositionList() == null?
							0 : ivlent1.getPositionList().size();
				i++;
			}
			else if (ivlent2.getDocNo() < ivlent1.getDocNo()) {

				resultv.addElement(ivlent2);
				resultPosInIVL += ivlent2.getPositionList() == null?
							0: ivlent2.getPositionList().size();
				j++;
			}
		}
		while (i < ivl.size()) {
			IVLEntry ivlent1 = (IVLEntry)ivl.elementAt(i);
			resultv.addElement(ivlent1);
			resultPosInIVL += ivlent1.getPositionList() == null?
						0 : ivlent1.getPositionList().size();
			i++;
		}
		while (j < other.ivl.size()) {
			IVLEntry ivlent2 = (IVLEntry)other.ivl.elementAt(j);
			resultv.addElement(ivlent2);
			resultPosInIVL += ivlent2.getPositionList() == null?
						0 : ivlent2.getPositionList().size();
			j++;
		}
		ivl = null; // delete old one
		ivl = resultv;
		numPosInIVL = resultPosInIVL;
	}

	/**
	 * Merge with another IVLEntry of the same word
	 */
	public void merge (IVLEntry other) {

		if (other==null) return;

		// do a binary search on the docno's of all IVLEntries
		int low = 0, high = ivl.size()-1;
		while (low <= high) {

			int mid = (low+high)/2;
			IVLEntry tmpent = (IVLEntry)ivl.elementAt(mid);

			if (other.getDocNo() == tmpent.getDocNo()) {
				tmpent.merge(other);
				numPosInIVL += other.getPositionList() == null?
					0 : other.getPositionList().size();
				return;
			}
			else if (other.getDocNo() > tmpent.getDocNo()) {
				low = mid + 1;
			}
			else { // other.getDocNo() < tmpent.getDocNo()
				high = mid-1;
			}
		}
		ivl.insertElementAt (other, low);
		numPosInIVL += other.getPositionList() == null?
					0 : other.getPositionList().size();
	}

    /**
     * Get the position type of the inverted list.
     * If the list is empty. We cannot really tell. In that case, a '-1'
     * is returned.
     */
    public byte getPositionType() {
		if (ivl==null) return (byte)-1;

		// note that the positions in the list must all be the same
		// type, therefore we only need to grab one position and see 
		// what it is.

		IVLEntry ivlent = (IVLEntry)ivl.elementAt(0);
		Vector poslist = ivlent.getPositionList();

		if (poslist == null) return -1;
		return ((Position)poslist.elementAt(0)).getType();
    }
}

