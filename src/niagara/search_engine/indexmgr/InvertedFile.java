
/**********************************************************************
  $Id: InvertedFile.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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
import java.util.Vector;
import java.util.Hashtable;

/**
 * InvertedFile is a wrapper around an OS file that stores inverted lists.
 * The actual format of the OS file is determined by the subclass.
 *
 * An InvertedFile does batch updates of inverted lists. The updates are
 * batched on ivl entries.
 *
 */
public abstract class InvertedFile {
    public static final byte IVFT_FLAT = 0x00;
    public static final byte IVFT_PAGED = 0x01;

    /////////////////////////////////////////////////////////////////
    //
    //             Data Members
    //
    /////////////////////////////////////////////////////////////////

    private static boolean DEBUG=false;
    protected String fileName;
    protected int fileno;
    protected int fileSize;
    protected byte positionType;
    public long t_openclose = 0;
    public long t_write = 0;

    /////////////////////////////////////////////////////////////////
    //
    //             Member Functions
    //
    /////////////////////////////////////////////////////////////////
    /**
     * Constructor to wrap around an OS file that already exists.
     */
    public InvertedFile (String fname, int fileno, byte postype) 
	throws IMIVLFileException, IMPositionException {

	this.fileno = fileno;
	fileName = fname;
	positionType = postype;

	// file size
	File fp = new File(fileName);
	if (fp.exists()) {
	    fileSize = (int)fp.length();
	}
	else fileSize = 0;

	if (DEBUG) {
	    System.out.println ("InvertedFile "+fileno+" created.");
	}
    }

    /**
     */
    public int getFileNo() { return fileno; }
    /**
     */
    public int getFileSize() { return fileSize; }

    /**
     * Add data to an inverted list
     */
    public abstract void insertList (IVL newivl);

    /**
     */
    public abstract byte getFileType ();

    /**
     * Read an inverted list from file.
     *
     * @param ivlToFill the IVL to fill.
     *        The wordno in it MUST have already been provided.
     *        Two fields are expected to be filled at the return of this
     *	      call: 'ivl' and 'numPosInIVL'.
     */
    public abstract void retrieveList (IVL ivlToFill);

    /**
     */
    public abstract void deleteList (int wordno);

    /**
     */
    public abstract void deleteList (int wordno, long docno);
}


