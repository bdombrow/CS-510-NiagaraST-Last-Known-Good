
/**********************************************************************
  $Id: Position.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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

/**
 * A position in an inverted list could either be a word number, or a
 * word number pair. The Position class extracts this out so that other
 * parts of the system, such as the operators do not have to know 
 * exactly what kind of position it is.
 *
 */

public class Position {
    // the position types
    public static final byte PT_SINGLE = 0x00;  // word number
    public static final byte PT_PAIR = 0x01;    // word number pair

    private Object position;

    /**
     * Get the size of a position in bytes.
     */
    public static int sizeof(byte postype) throws
    IMPositionException {

	if (postype == PT_SINGLE) return 4;
	else if (postype == PT_PAIR) return 8;
	throw new IMPositionException();
    }

    /**
     * Constructor. Create a position given a word number.
     */
    public Position (int pos) {
	position = new Integer(pos);
    }

    /**
     * Constructor. Create a position given a word number pair
     */
    public Position (int from, int to) {
	position = new Long(Util.makeLong(to, from));
    }

    /**
     * Constructor. Create an empty position.
     * This one should be used when reading and parsing a position 
     * from file.
     */
    public Position() {
	position = null;
    }
    
    /**
     * Copy constructor
     */
    public Position (Position other) throws IMPositionException {
	if (other.getType() == PT_SINGLE) {
	    position = new Integer(other.intValue());
	}
	else if (other.getType() == PT_PAIR) {
	    position = new Long(other.longValue());
	}
	else {
	    throw new IMPositionException();
	}
    }

    /**
     * Get the type of the position
     */
    public byte getType() {
	if (position instanceof Long) return PT_PAIR;
	else return PT_SINGLE;
    }

    /**
     * Get the word number represented by the position
     *
     * If the position represents a pair of word number, the begin word
     * number is returned.  If it is necessary to make sure this is
     * called on the text word position, the type of position should be
     * checked before this member function is called.
     */
    public int intValue() throws IMPositionException {
	if (position instanceof Integer) {
	    return ((Integer)position).intValue();
	}
	else if (position instanceof Long) {
	    return (int)((Long)position).longValue();
	}
	else {
	    throw new IMPositionException();
	}
    }

    /**
     * Get the word number pair represented by the position
     */
    public long longValue() throws IMPositionException {
	if (!(position instanceof Long)) {
	    throw new IMPositionException();
	}
	return ((Long)position).longValue();
    }
    
    /**
     * Test if two positions are nested.
     *
     * @param pos1 the first position
     * @param pos2 the second position
     * @return true if pos1 is nested inside pos2, false otherwise
     */
    public static boolean isNestedIn (Position pos1, Position pos2) 
	throws IMPositionException {
	return pos1.isNestedIn(pos2);
    }

    /**
     * Test if this position is nested in another one.
     *
     * @return true if yes
     */
    public boolean isNestedIn (Position other) throws IMPositionException {
	if (other.getType() == PT_SINGLE) {
	    return false;
	}
	else if (other.getType() == PT_PAIR) {
	    long pos2 = other.longValue();
	    
	    if (this.getType() == PT_SINGLE) {
		int pos1 = intValue();
		if (pos1>=(int)pos2 && pos1<=(int)(pos2 >> 32)) {
		    return true;
		}
	    }
	    else if (this.getType() == PT_PAIR) {
		long pos1 = longValue();
		if ((int)pos1>=(int)pos2 && (int)(pos1>>32)<=(int)(pos2>>32)) {
		    return true;
		}
	    }
	}
	else {
	    throw new IMPositionException();
	}
	return false;
    }

    /**
     * Test if two positions are properly nested.
     *
     * A position x is properly nested inside another position y
     * if y not only nests x but also other things.
     */
    public boolean isProperNestedIn (Position other) throws
    IMPositionException {

	if (other.getType() == PT_SINGLE) {
	    return false;
	}
	else if (other.getType() == PT_PAIR) {
	    long pos2 = other.longValue();
	    if (position instanceof Integer) {
		int pos1 = ((Integer)position).intValue();
		return ((pos1-(int)pos2>0) && ((int)(pos2>>32)-pos1>0)
		  && ((pos1-(int)pos2 > 1) || ((int)(pos2>>32)-pos1 > 1)));
	    }
	    else if (position instanceof Long) {
		long pos1 = ((Long)position).longValue();
		return ( ((int)pos1-(int)pos2>0)
		  && ((int)(pos2>>32)-(int)(pos1>>32) > 0)
		  && ((int)pos1-(int)pos2>1 || (int)(pos2>>32)-(int)(pos1>>32)>1) );
	    }
	    else throw new IMPositionException();
	}
	else throw new IMPositionException();
    }

    /**
     * Test if two positions are properly nested.
     *
     * A position x is properly nested inside another position y
     * if y not only nests x but also other things.
     */
    public static boolean isProperNestedIn (Position p1, Position p2)
    throws IMPositionException {
	return p1.isProperNestedIn(p2);
    }

    /**
     * Read in and create a position from a file.
     */
    public void parse (Object ins, byte ptype)
	throws IOException {

	if (ptype == PT_SINGLE) {
	    position = null;
	    if (ins instanceof RandomAccessFile) {
		position = new Integer(((RandomAccessFile)ins).readInt());
	    }
	    else if (ins instanceof DataInputStream) {
		position = new Integer(((DataInputStream)ins).readInt());
	    }
	    else {
		throw new IOException ("wrong input stream");
	    }
	}
	else if (ptype == PT_PAIR) {
	    position = null;
	    if (ins instanceof RandomAccessFile) {
		position = new Long(((RandomAccessFile)ins).readLong());
	    }
	    else if (ins instanceof DataInputStream) {
		position = new Long(((DataInputStream)ins).readLong());
	    }
	    else {
		throw new IOException ("wrong input stream");
	    }
	}
    }

    /**
     * Read in and create a position from a byte array
     * @return bytes parsed
     */
    public int parse (byte[] data, int offset, byte postype) {
	position = null;
	if (postype == PT_SINGLE) {
	    position = new Integer(Util.toInt(data, offset));
	    return 4;
	}
	else if (postype == PT_PAIR) {
	    position = new Long(Util.toLong(data,offset));
	    return 8;
	}
	return -1;
    }

    /**
     * Persist this position to a file.
     *
     * @param fo the file to write. should be already open.
     * @return number of bytes written.
     */
    public int writeToFile (Object out)
	throws IOException {

	int bytesWritten = 0;
	if (position instanceof Integer) {
	    if (out instanceof RandomAccessFile) {
		((RandomAccessFile)out).writeInt(
			((Integer)position).intValue());
		bytesWritten += 4;
	    }
	    else if (out instanceof DataOutputStream) {
		((DataOutputStream)out).writeInt(
			((Integer)position).intValue());
		bytesWritten += 4;
	    }
	    else {
		throw new IOException("wrong output stream");
	    }
	}
	else {
	    if (out instanceof RandomAccessFile) {
		((RandomAccessFile)out).writeLong(
			((Long)position).longValue());
		bytesWritten += 8;
	    }
	    else if (out instanceof DataOutputStream) {
		((DataOutputStream)out).writeLong(
			((Long)position).longValue());
		bytesWritten += 8;
	    }
	    else {
		throw new IOException("wrong output stream");
	    }
	}
	return bytesWritten;
    }
    
    /**
     * Write to a byte array
     */
    public int write (byte[] buf, int offset) {
	if (position instanceof Integer) {
	    Util.writeInt (((Integer)position).intValue(),buf,offset);
	    return 4;
	}
	else {
	    Util.writeLong (((Long)position).longValue(), buf,offset);
	    return 8;
	}
    }

    /**
     * Print the position out
     */
    public String toString() {
	if (position instanceof Integer)
	    return position.toString();
	else {
	    long posValue = ((Long)position).longValue();
	    return "("+(int)posValue+", "+(int)(posValue>>32)+")";
	}
    }

    /**
     * Test position (in)equality.
     * The idea is to ensure that the positions are sorted in the 
     * increasing order of the two word numbers.
     *
     * Positions of different types can also be compared.
     *
     * If both positions are of type PT_SINGLE, and pos1=a, pos2=b,
     * pos1 == pos2 iff a == b
     * pos1 < pos2 if a < b
     *
     * If both positions are of type PT_PAIR, and pos1=(a,b), pos2=(c,d)
     * pos1 == pos2 iff a==c and b==d
     * pos1 < pos2 if (a<c) or (a==c and b>d)
     * pos1 > pos2 if (a>c) or (a==c and b<d)
     *
     * If pos1=a, pos2=(b,c), then
     * pos1 == pos2 if (b <= a <= c)
     * pos1 < pos2 if a < b
     * pos1 > pos2 if a > c
     *
     * The above critera may look wierd.
     * If the result is undesired, it's up to the caller to make sure
     * that the two positions compared are of the same type.
     *
     * @return 0 if equal,
     *        -1 if this position is less than other
     *         1 if this position is greater than other
     */
    public int compareTo (Position other) throws IMPositionException {
	if (position instanceof Integer) {
	    if (other.getType() == PT_SINGLE) {
		return ((Integer)position).intValue() - other.intValue();
	    }
	    else if (other.getType() == PT_PAIR) {
		int this_pos = ((Integer)position).intValue();
		long other_pos = other.longValue();
		int other_pos1 = (int)other_pos;
		int other_pos2 = (int)(other_pos >> 32);

		if (this_pos < other_pos1) return -1;
		else if (this_pos > other_pos2) return 1;
		else return 0;
	    }
	    else {
		throw new IMPositionException();
	    }
	}
	else {
	    if (other.getType() == PT_SINGLE) {
		return -(other.compareTo (this));
	    }
	    else if (other.getType() == PT_PAIR) {
		long this_pos = this.longValue();
		int this_pos1 = (int)this_pos;
		int this_pos2 = (int)this_pos >> 32;

		long other_pos = other.longValue();
		int other_pos1 = (int)other_pos;
		int other_pos2 = (int)other_pos >> 32;

		if (this_pos1 == other_pos1) {
		    //it really doesn't matter which one of this go
		    // first, as the containment operation always
		    // does nested loop over positions inside a document.
		    //return ((int)(pos2>>32)-(int)(pos1>>32));

		    return this_pos2 - other_pos2;
		}
		else {
		    return (this_pos1 - other_pos1);
		}
	    }
	    else {
		throw new IMPositionException();
	    }
	}
    }

    /**
     * Test if two positions are next to each other
     * The two positions tested may be of different types.
     * For instance, a position of an element can be next to a position
     * of a word. This means that the word is right before the beginning
     * or after the ending word number of the element.
     *
     * @return true if two positions are next to each other,
     *         false otherwise
     */
    public boolean isNextTo(Position other) throws IMPositionException {
	if (position instanceof Integer) {
	    int thisPos = ((Integer)position).intValue();
	    if (other.getType() == PT_SINGLE) {
		int otherPos = other.intValue();
		return (thisPos-otherPos==1 || otherPos-thisPos==1);
	    }
	    else if (other.getType() == PT_PAIR) {
		long otherPos = other.longValue();
		return ((int)otherPos-thisPos==1 ||
			thisPos-(int)(otherPos>>32)==1);
	    }
	    else throw new IMPositionException();
	}
	else {
	    long thisPos = ((Long)position).longValue();
	    if (other.getType() == PT_SINGLE) {
		int otherPos = other.intValue();
		return ((int)thisPos-otherPos==1 ||
			otherPos-(int)(thisPos>>32)==1);
	    }
	    else if (other.getType() == PT_PAIR) {
		long otherPos = other.longValue();
		return ((int)otherPos-(int)(thisPos>>32)==1 ||
			(int)(thisPos>>32)-(int)otherPos==1);
	    }
	    else throw new IMPositionException();
	}
    }

    /**
     * Test if two positions are next to each other
     */
    public static boolean isNextTo (Position pos1, Position pos2) 
    throws IMPositionException {
	return pos1.isNextTo(pos2);
    }

    /**
     * Concatenate two positions to form a bigger one.
     * The concatenation can be done on two positions of different
     * types.  The resulting position is the minimum extent that 
     * covers both positions.
     *
     * No checking is done to see if the two positions are next to
     * each other.  This permits the concatenation of any two positions.
     */
    public Position concatenate (Position other) throws
    IMPositionException {
	if (position instanceof Integer) {
	    int thisPos = ((Integer)position).intValue();
	    if (other.getType() == PT_SINGLE) {
		int otherPos = other.intValue();
		return new Position(
			Util.min(thisPos, otherPos),
			Util.max(thisPos, otherPos));
	    }
	    else if (other.getType() == PT_PAIR) {
		long otherPos = other.longValue();
		return new Position(
			Util.min(thisPos, (int)otherPos),
			Util.max(thisPos, (int)(otherPos>>32)));
	    }
	}
	else {
	    long thisPos = ((Long)position).longValue();
	    if (other.getType() == PT_SINGLE) {
		int otherPos = other.intValue();
		return new Position(
			Util.min((int)thisPos, otherPos),
			Util.max((int)(thisPos>>32), otherPos));
	    }
	    else if (other.getType() == PT_PAIR) {
		long otherPos = other.longValue();
		return new Position(
			Util.min((int)thisPos, (int)otherPos),
			Util.max((int)(thisPos>>32), (int)(otherPos>>32)));
	    }
	}
	return null;
    }
}
