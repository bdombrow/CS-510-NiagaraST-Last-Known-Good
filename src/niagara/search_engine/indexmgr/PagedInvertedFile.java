
/**********************************************************************
  $Id: PagedInvertedFile.java,v 1.2 2002/08/17 16:50:05 tufte Exp $


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

class WordMapEntry {
	int firstPageNo;
	int lastPageNo;
	int sizeInBytes;
	boolean hasUpdates;

	WordMapEntry(int fpn, int lpn, int sz, boolean u) {
		firstPageNo = fpn;
		lastPageNo = lpn;
		sizeInBytes = sz;
		hasUpdates = u;
	}
	public String toString() {
		String ret = "<"+firstPageNo+", "+lastPageNo+", "+sizeInBytes
			+", "+(hasUpdates?"true>":"false>");
		return ret;
	}
	public void parse(DataInputStream dis) throws IOException {
		firstPageNo = dis.readInt();
		lastPageNo = dis.readInt();
		sizeInBytes = dis.readInt();
		hasUpdates = dis.readByte()==0x01;
	}
	public void persist(DataOutputStream dos) throws IOException {
		dos.writeInt (firstPageNo);
		dos.writeInt (lastPageNo);
		dos.writeInt (sizeInBytes);
		dos.writeByte (hasUpdates? 0x01 : 0x00);
	}
}

/**
 * Meta file (number in bytes):
 *   <pagesize(4), page bitmap>
 *   <wordno(4), begin_page_no(4), end_page_no(4), has_updates(1)>
 *
 * First 8 bytes of each page: <next_page(4), free_offset_in_page>
 * The free space offset starts at the beginning of the page.
 *
 */

public class PagedInvertedFile extends InvertedFile {

	public static final boolean DEBUG = false;

	private RandomAccessFile randomFile = null;
	private byte[] pageBitMap = null; // bitmap indicating free pages
				      // '1' bits indicate free pages
	private int fileSize;
	private int pageSize;

	private Hashtable wordMap = null; // word-->(begin_blk,end_blk,has_update)

	/**
	 * Constructor
	 *
	 * @param ivldir   the directory to hold the inverted lists
	 * @param fileno   the number to assign to the file.
	 * @param postype  the type of positions in the file
	 * @param filesize the size of the file. ignored for flat file.
	 * @param pagesize the page size of the file
	 * @param newfile  whether to create a new file or to use an existing file
	 */
	public PagedInvertedFile (String ivldir, int fileno, byte postype,
		int filesize, int pagesize)
		throws IMIVLFileException, IMPositionException {

		super(ivldir, fileno, postype);
		fileSize = filesize;
		pageSize = pagesize;
		wordMap = new Hashtable();

		////// check all the sizes
		int totalPages = filesize / pagesize;
		try {
			if (filesize % pagesize != 0) {
				throw new IMException(
					"File size must be multiple of page size!");
			}
			if (pagesize <= 8) {
				throw new IMException("Page size too small!");
			}
			if (totalPages % 8 != 0) {
				throw new IMException("Number of pages must be multiple of 8!");
			}
		}
		catch (IMException e) {
			System.out.println (e);
			System.exit(1);
		}

		///// create page bit map
		int bitmaplen = totalPages/8;
		pageBitMap = new byte[bitmaplen];

		///// create data file and read in meta infor if necessary
		File fpmeta = null;
		DataInputStream metadis = null;
		try {
			File fp = new File(fileName);
			if (fp.exists()) {
				// create a RandomAccessFile for the old data file
				randomFile = new RandomAccessFile (fp, "rw");

				// check meta info

				fpmeta = new File (fileName+".meta");
				if (!fpmeta.exists()) {
					throw new IOException("Cannot find meta file");
				}

				metadis = new DataInputStream(
						new BufferedInputStream(new FileInputStream(fpmeta)));
				// pagesize
				if (metadis.readInt() != pagesize) {
					throw new IOException(
						"File is of wrong page size or meta file is corrupted");
				}
				// bitmap
				for (int i = 0; i < bitmaplen; i++) {
					pageBitMap[i] = metadis.readByte();
				}
				// wordmap
				while (true) {
					int wordno = metadis.readInt();
					WordMapEntry mapentry = new WordMapEntry(
						-1, -1, 0, false);
					mapentry.parse (metadis);
					wordMap.put (new Integer(wordno), mapentry);
				}
			}
			else {
	    		// create a RandomAccessFile for the new data file
				randomFile = new RandomAccessFile (fp, "rw");

				for (int i = 0; i < bitmaplen; i++) {
					pageBitMap[i] = (byte)0xff; // everything is free
				}

				// nothing to do for wordmap: it is empty
			}
		}
		catch (EOFException e) {
			fpmeta = null;
			try { metadis.close(); }
			catch (IOException e2) {
				System.err.println (e2);
			}
		}
		catch (IOException e) {
			System.err.println ("Error initlializing inverted file:");
			System.err.println (e);
			// can't do anything without file. must exit
			System.exit(1);
		}
	}

	/**
	 * Persist the meta info and close the file
	 */
	public void persist() {
		try {
			// write meta info
			DataOutputStream dos = new DataOutputStream(
				new BufferedOutputStream(
					new FileOutputStream(fileName+".meta")));

			// pagesize
			dos.writeInt (pageSize);

			// bitmap
			dos.write (pageBitMap, 0, pageBitMap.length);

			// wordmap
			Enumeration allwords = wordMap.keys();
			while (allwords.hasMoreElements()) {
				Integer wordnoObj = (Integer)allwords.nextElement();
				WordMapEntry mapentry = (WordMapEntry)wordMap.get(wordnoObj);

				dos.writeInt (wordnoObj.intValue());
				mapentry.persist (dos);
			}

			dos.flush();
			dos.close();

			// close inverted file
			randomFile.close();
			randomFile = null;
		}
		catch (IOException e) {
			System.err.println ("Warning: problem persisting file meta info");
		}
	}

	/**
	 */
	public byte getFileType () { return IVFT_PAGED; }

	/**
	 * The list to be inserted may or may not already exist.
	 * If the list does not yet exist, it is created.
	 * Otherwise, the new data is appended at the end of old list.
	 */
	public synchronized void insertList (IVL newivl) {

		if (newivl == null) return;
		/*
		if (DEBUG) {
    		System.out.println ("putting inverted list to file "
                + fileName + ": "+newivl);
        }
		*/

		long beginWriteTime = System.currentTimeMillis();

		try {
			byte[] data = newivl.flatten(positionType);
			assert (data != null && data.length > 0): "Invalid data";
			int offset = 0;

			boolean wordIsNew = false;

			// see if this list already exists
			WordMapEntry mapentry = (WordMapEntry)wordMap.get(
				new Integer(newivl.getWordNo()));

	 		// if list already exist, we fill the last page first
			if (mapentry != null) {
				// next page of the last page must be null
			    assert (get_next_page_no(mapentry.lastPageNo)==-1);

				offset += append_page (mapentry.lastPageNo, data, offset);
			}
			else {
				wordIsNew = true;
				mapentry = new WordMapEntry (-1, -1, 0, false);

				// create a new entry in wordmap
				wordMap.put (new Integer(newivl.getWordNo()), mapentry);
			}

			if (offset < data.length) {
				int newpageno = -1;
				while (offset < data.length) {
					// create a new page
					newpageno = new_page ();
					if (newpageno < 0) {
						System.err.println (
							"Error getting page from file " + fileName
							+ ". Running out of space");
						System.exit(1);
					}

					// adjust page pointers
					if (wordIsNew) {
						mapentry.firstPageNo = newpageno;
						wordIsNew = false;
					}
					else {
						set_next_page_no (mapentry.lastPageNo, newpageno);
					}
					mapentry.lastPageNo = newpageno;

					// now write the page
					int bytesWritten = -1;
					bytesWritten = write_page (newpageno, data, offset);
					offset += bytesWritten;
				}
				// null terminate the next pointer of the last page 
				set_next_page_no (newpageno, -1);
			}

	 		// adjust the list size recorded in map entry
			mapentry.sizeInBytes += data.length;
			data = null;
		}
		catch (IMException e) {
			e.printStackTrace();
			System.exit(1);
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

		long endWriteTime = System.currentTimeMillis();
		t_write = endWriteTime-beginWriteTime;
	}

	/**
	 * Read an inverted list from file.
	 * (see comment in InvertedFile.java)
	 */
	public synchronized void retrieveList (IVL ivlToFill) {

		boolean closeAfterRetrieve = false;

		if (randomFile == null) {
			try {
				randomFile = new RandomAccessFile (fileName, "r");
				closeAfterRetrieve = true;
			}
			catch (Exception e) {
				System.err.println (e);
				e.printStackTrace();
				return;
			}
		}

		IVL tmpivl = new IVL(ivlToFill.getWordNo());
		try {
			// see if the list exists
			WordMapEntry mapentry = (WordMapEntry)wordMap.get(
				new Integer(ivlToFill.getWordNo()));
			if (mapentry == null) {
				if (DEBUG) {
					System.out.println ("...no list found for wordno "
							+ivlToFill.getWordNo());
				}
				return;
			}

			// allocate enough space to hold the data
			byte[] data = new byte[mapentry.sizeInBytes];
			if (DEBUG) {
				System.out.println ("...allocating "
						+mapentry.sizeInBytes+" bytes to hold the list");
			}

			// read the list from pages
			int currPageNo = mapentry.firstPageNo;
			int offset = 0;
			while (currPageNo != -1) {
				if (DEBUG) {
					System.out.println ("...reading from page " +currPageNo);
				}
				offset += read_page(currPageNo, data, offset);
				currPageNo = get_next_page_no (currPageNo);
			}
			assert (offset == mapentry.sizeInBytes) : "Invalid offset";

			// construct IVL
			ivlToFill.parse (data, positionType);

			data = null;
		}
		catch (IOException e) {
			System.err.println ("Error reading list for wordno "
				+ivlToFill.getWordNo());
			e.printStackTrace();
		}

		if (closeAfterRetrieve) {
			try {
				randomFile.close();
			}
			catch (Exception e) {
				System.err.println (e);
				e.printStackTrace();
			}
		}
	}

	/**
	 * Delete a whole list
	 * Read in the file, and write it out to another file, ignoring the
	 * positions for 'wordno'
	 */
	public void deleteList (int wordno) {
		System.out.println ("Not Implemented");
	}

	/**
	 * Delete all positions of a document associated with a list
	 */
	public void deleteList (int wordno, long docno) {
		System.out.println ("Not Implemented");
	}

	///////////// private member functions for handling pages /////////
	/**
	 * Obtain a new free page.
	 * @return page number obtained
	 */
	private final int new_page() {
		int pageno = 0;
		boolean found = false;

		for (int i=0; (i < pageBitMap.length && !found); i++) {
			for (int j=0; (j<8 && !found); j++) {
				if ( ((pageBitMap[i]<<j) & (byte)0x80) != 0) {
					// page found. reset the j'th bit of the i'th map
					// note that i,j start at the beginning,not the end
					pageBitMap[i] &= ~((byte)0x80>>>j);

					found = true;
					break;
				}
				pageno++;
			}
		}
		if (!found) return -1;
		return pageno;
	}

	private final void free_page(int pageno) {
		// TODO
	}

	/**
	 * Read data into a byte array starting at an offset.
	 */
	private final int read_page(int pageno, byte[] buf, int offset) 
		throws IOException {

		randomFile.seek (pageno*pageSize + 4);
		int freeSpaceOffset = randomFile.readInt();
		int bytesToRead = freeSpaceOffset-8;
		assert (bytesToRead <= buf.length-offset);

		return randomFile.read(buf, offset, bytesToRead);
	}

	/**
	 * Overwrite a page.
	 * @return number of bytes written
	 */
	private final int write_page(int pageno, byte[] data, int offset)
		throws IOException {

		// seek to the spot of the page right after the heading
		randomFile.seek (pageno*pageSize+8);

		// do not write over the page boundary
		int bytesToWrite = Util.min(data.length-offset, pageSize-8);

		// now write
		randomFile.write (data, offset, bytesToWrite);

		// change the freeSpaceOffset
		randomFile.seek (pageno*pageSize + 4);
		randomFile.writeInt (bytesToWrite+8);

		return bytesToWrite;
	}

	/**
	 * Append data, starting at an offset to a page
	 * Try to append as much as possible to the page until it is full
	 * @return number of bytes written
	 */
	private final int append_page(int pageno, byte[] data, int offset)
		throws IOException {

		// seek to the beginning of the freeSpaceOffset of the page
		randomFile.seek (pageno*pageSize+4);

		// check to see how much space available
		int freeSpaceOffset = randomFile.readInt();

		// if page already full, return
		if (freeSpaceOffset >= pageSize) return 0;

		// otherwise append starting at freeSpaceOffset
		randomFile.seek (pageno*pageSize + freeSpaceOffset);

		// append not exceeding availabe space on page
		int bytesToWrite = 
			Util.min(data.length-offset, pageSize-freeSpaceOffset);
		randomFile.write (data, offset, bytesToWrite);

		// adjust freeSpaceOffset
		randomFile.seek (pageno*pageSize + 4);
		randomFile.writeInt(freeSpaceOffset+bytesToWrite);

		return bytesToWrite;
	}

	private final int get_next_page_no(int pageno) throws IOException {
		randomFile.seek (pageno*pageSize);
		return randomFile.readInt();
	}

	private final void set_next_page_no(int pageno, int nextpageno) 
		throws IOException {

		randomFile.seek(pageno*pageSize);
		randomFile.writeInt (nextpageno);
	}
}
