
/**********************************************************************
  $Id: WordEntry.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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

/**
 * A word entry describes info about a word in an xmlfile
 * It consists of: <word, wordno>
 *
 */

public class WordEntry {
	public static final byte WT_NONE = 0x00;
	public static final byte WT_REGULAR = 0x01;
	public static final byte WT_TAG = 0x02;
	public static final byte WT_ATTR = 0x03;
	public static final byte WT_ATTRVALUE = 0x04;
	public static final byte WT_DTD = 0x05;

	private String word;
	private byte type;
	private Position position;
	private int depth;

	/**
	 * Constructor. Word entry for regular text word.
	 */
	public WordEntry(String w, byte tt, int wordno, int depth) {
		word = w;
		type = tt;
		position = new Position(wordno);
		this.depth = depth;
	}

	/**
	 * Constructor.
	 */
	public WordEntry (String w, byte tt, Position pos, int depth) {
		word = w;
		type = tt;
		position = pos;
		this.depth = depth;
	}

	/**
	 * Getters and Setters
	 */
	public String getWord() {
		return word;
	}
	public Position getPosition() {
		return position;
	}
	public byte getType() {
		return type;
	}
	public void setPosition(int wordno1, int wordno2) {
		position = null; position = new Position(wordno1, wordno2);
	}
	public int getDepth() {
		return depth;
	}

	/**
	 * String description of this word entry
	 */
	public String toString() {
		String ret = new String("<"+position+": "+word+"  ");
		if (type == WT_NONE) {
			ret += "WT_NONE  ";
		}
		else if (type == WT_REGULAR) {
			ret += "WT_REGULAR  ";
		}
		else if (type == WT_TAG) {
			ret += "WT_TAG  ";
		}
		else if (type == WT_ATTR) {
			ret += "WT_ATTR  ";
		}
		else if (type == WT_ATTRVALUE) {
			ret += "WT_ATTRVALUE  ";
		}
		else if (type == WT_DTD) {
			ret += "WT_DTD  ";
		}
		ret += depth + ">";
		return ret;
	}
}
