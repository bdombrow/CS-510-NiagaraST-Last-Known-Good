
/**********************************************************************
  $Id: FlatInvertedFile.java,v 1.1 2000/05/30 21:03:27 tufte Exp $


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
 * Format of Flat IVLFile:
 *     <ivl entry><ivl entry>......
 *
 */
public class FlatInvertedFile extends InvertedFile {
    public static final boolean DEBUG = false;

    /**
     * Constructor
     *
     * @param ivldir   the directory to hold the inverted lists
     * @param fileno   the number to assign to the file.
     * @param postype  the type of positions in the file
     * @param filesize the size of the file. ignored for flat file.
     */
    public FlatInvertedFile (String ivldir, int fileno, byte postype)
	throws IMIVLFileException, IMPositionException {

		super(ivldir, fileno, postype);
    }

    /**
     */
    public byte getFileType () { return IVFT_FLAT; }

    /**
     * The new inverted list is appended to the end of the file.
     */
    public synchronized void insertList (IVL newivl) {

		if (newivl == null) return;
    	if (DEBUG) {
            System.out.println ("putting inverted list to file "
                + fileName + ": "+newivl);
        }

		byte[] newbytes = null;
		try {
			newbytes = newivl.flatten (positionType);
		}
		catch (IMException e) {
			e.printStackTrace();
			System.exit(1);
		}

		// read it in
		byte[] content = null;
		try {
			FileInputStream fis = new FileInputStream (fileName);
			//GZIPInputStream gzis = new GZIPInputStream (fis);
			BufferedInputStream bis = new BufferedInputStream (fis);

			content = new byte[fileSize];
			bis.read (content, 0, fileSize);

			bis.close();
		}
		catch (FileNotFoundException e) {
			// ignore. must be for the first time
		}
		catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

		// write it out
		try {
			FileOutputStream fos = new FileOutputStream (fileName);
			//GZIPOutputStream gzos = new GZIPOutputStream (fos);
			BufferedOutputStream bos = new BufferedOutputStream (fos);

			if (content != null) {
				bos.write (content, 0, fileSize);
			}
			if (newbytes != null) {
				bos.write (newbytes, 0, newbytes.length);
				fileSize += newbytes.length;
			}
			bos.close();
		}
		catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

    /**
     * Read an inverted list from file.
     * (see comment in InvertedFile.java)
     */
    public synchronized void retrieveList (IVL ivlToFill) {

		try {
			// open the file
			FileInputStream fis = new FileInputStream (fileName);
			//GZIPInputStream gzis = new GZIPInputStream (fis);
			BufferedInputStream bis = new BufferedInputStream (fis);

			byte[] content = new byte[fileSize];
			bis.read (content, 0, fileSize);

			ivlToFill.parse (content, positionType);

			bis.close();
		}
		catch (FileNotFoundException e) {
			return;
		}
		catch (IOException e) {
			System.out.println (e);
			e.printStackTrace();
			System.exit(1);
		}
	}

    /**
     * Delete a whole list.
     */
    public void deleteList (int wordno) {
		File fp = new File (fileName);
		fp.delete();
		fp = null;
    }

    /**
     * Delete all positions of a document associated with a list
     */
    public void deleteList (int wordno, long docno) {

		// read it in
		byte[] content = null;
		try {
			FileInputStream fis = new FileInputStream (fileName);
			//GZIPInputStream gzis = new GZIPInputStream (fis);
			BufferedInputStream bis = new BufferedInputStream (fis);

			content = new byte[fileSize];
			bis.read (content, 0, fileSize);

			bis.close();
		}
		catch (FileNotFoundException e) {
			// ignore. no such list.
		}
		catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

		// convert it into an inverted list
		IVL ivlobj = new IVL(wordno);
		ivlobj.parse (content, positionType);

		// now do delete
		ivlobj.delete (docno);
		byte[] newcontent = null;
		try {
			newcontent = ivlobj.flatten(positionType);
		}
		catch (IMException e) {
			System.err.println (e);
			System.exit(1);
		}

		// write it out
		try {
			FileOutputStream fos = new FileOutputStream (fileName);
			//GZIPOutputStream gzos = new GZIPOutputStream (fos);
			BufferedOutputStream bos = new BufferedOutputStream (fos);

			if (newcontent != null) {
				bos.write (newcontent, 0, fileSize);
			}
			bos.close();
		}
		catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
    }
}
