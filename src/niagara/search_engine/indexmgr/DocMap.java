
/**********************************************************************
  $Id: DocMap.java,v 1.1 2000/05/30 21:03:27 tufte Exp $


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
import java.net.*;

/**
 * Document map
 *
 * The doc map is implemented as a hash table. Although the document
 * numbers are implemented as "long"s, the total number of entries will 
 * never exceed 2^32-1, because the Hashtable cannot have more than 
 * that many entries. Making the document numbers "longs" is just 
 * for us to have room to keep set of removed document numbers, besides
 * those in active use.
 *
 * Format of persisted document map(number in bytes):
 * <number-of-doc-entries(4)>
 * <docno(8), url_len(4), url>
 *    ... ...
 * <removed-docno(8)>
 *    ... ...
 *
 */

public class DocMap {
	private Hashtable docMap = null; // docno(int)-->url(URL)
	private Hashtable revDocMap = null; // url(URL)-->docno(int)

	private Set docNumSet = null; // set of used doc numbers
    private Set removedDocNumSet = null; // set of removed doc numbers
					 // this is a subset of docNumSet
	private long nextDocNum = 1;

	private String docMapFile = null;
	private static boolean DEBUG=false;

	/**
	 * Constructor. Load in persisted data.
	 */
	public DocMap (String docmapfile) throws IMException {
		docMap = new Hashtable();
		revDocMap = new Hashtable();
		docMapFile = docmapfile;
		docNumSet = Collections.synchronizedSet(new HashSet());
		removedDocNumSet = Collections.synchronizedSet(new HashSet());

		FileInputStream fis = null;
		GZIPInputStream gzis = null;
		DataInputStream dis = null;
		try {
			fis = new FileInputStream(docmapfile);
			gzis = new GZIPInputStream(fis);
			dis = new DataInputStream(gzis);

			// number of doc entries
			int numDocEntries = dis.readInt();

			// doc entries
			for (int i = 0; i < numDocEntries; i++) {
				// doc no
				long doc_no = dis.readLong();
				docNumSet.add(new Long(doc_no));
				nextDocNum = doc_no > nextDocNum? doc_no+1 : nextDocNum;
				if (DEBUG) System.out.println("docmap: read docno="+doc_no);

				// url len
				int url_len = dis.readInt();
				if (DEBUG) System.out.println("docmap: read urllen="+url_len);

				// url
				byte[] url_bytes = new byte[url_len];
				dis.readFully (url_bytes);
				String url_str = new String(url_bytes);
				if (DEBUG) System.out.println("docmap: read url="+url_str);

				// maps
				docMap.put (new Long(doc_no), new URL(url_str));
				revDocMap.put (new URL(url_str), new Long(doc_no));
			}

			// removed doc numbers
			while (true) {
				long removed_doc_no = dis.readLong();
				removedDocNumSet.add (new Long(removed_doc_no));
			}
		}
		catch (EOFException e) {
			try { dis.close(); }
			catch (IOException e2) {
				System.out.println(e2);
			}
		}
		catch (FileNotFoundException e) { 
			// nothing
		}
		catch (IOException e) {
			throw new IMException("Corrupted doc map");
		}
	}

	/**
	 * Flush stuff to disk
	 */
	public void flush() throws IMException {
		try {
			FileOutputStream fos = new FileOutputStream(docMapFile);
			GZIPOutputStream gzos = new GZIPOutputStream(fos);
			DataOutputStream dos = new DataOutputStream(gzos);

			// number of doc entries
			dos.writeInt (docMap.size());

			// doc entries
			Enumeration doc_nos = docMap.keys();
			while (doc_nos.hasMoreElements()) {
				// doc no
				Long doc_no = (Long)doc_nos.nextElement();
				dos.writeLong (doc_no.intValue());
				if (DEBUG) {
					System.out.println("docmap: wrote docno="+doc_no);
				}

				// url len
				String url_str = ((URL)docMap.get(doc_no)).toString();
				int url_len = url_str.length();
				dos.writeInt (url_len);
				if (DEBUG) {
					System.out.println("docmap: wrote urllen="+url_len);
				}

				// url
				byte[] url_bytes = url_str.getBytes();
				if (url_bytes.length != url_len) {
					throw new IMException("wrong url_str length");
				}
				dos.write (url_bytes, 0, url_bytes.length);
				if (DEBUG) {
					System.out.println("docmap: wrote url="+url_str);
				}
			} 
			// removed document numbers
			Iterator itr = removedDocNumSet.iterator();
			while (itr.hasNext()) {
				Long removedDocNo = (Long)itr.next();
				dos.writeLong(removedDocNo.longValue());
			}

			dos.close();
		}
		catch (IOException e) {
			throw new IMException ("Corrupted map files");
		}
	}

	/**
	 * Add a doc
	 * Retrun doc no
	 */
	public long add (URL u) {
		// check to see if it's already there
		Long old = (Long)revDocMap.get(u);
		if (old != null) {
			return old.longValue();
		}
		long docno = assignNextDocNum();
		docMap.put (new Long(docno), u);
		revDocMap.put (u, new Long(docno));
		return docno;
	}

    /**
     * Remove a doc
     * @return true if successful, false otherwise
     */
    public void remove (long docno) {
	//System.out.println("DocMap: remove docno from docmap");
	URL u = (URL)docMap.remove(new Long(docno));
	if (u != null) {
	    //System.out.println("DocMap: remove url from revdocmap");
	    revDocMap.remove(u);
	}
	docNumSet.remove (new Long(docno));
    }

    /**
     */
    public int getNumEntries() {
	return docMap.size();
    }

	/**
	 * Given an URL, find the doc no
	 * Return -1 if the doc is never indexed
	 */
	public long getDocNo (URL u) {
		Long result = (Long)revDocMap.get(u);
		if (result == null) {
			return -1;
		}
		return result.longValue();
	}

    /**
     * Given a doc no, find the doc name
     */
    public String getDocName(long docno) {
	URL u = (URL)docMap.get(new Long(docno));
	if (u == null) {
	    return null;
	}
	return u.toString();
    }

    /**
     * Print all doc names
     */
    public void printDocs() {
	Enumeration urls = revDocMap.keys();
	while (urls.hasMoreElements()) {
	    URL u = (URL)urls.nextElement();
	    System.out.println (u.toString());
	}
    }

    /**
     * Assign a word number for a new word
     */
    private long assignNextDocNum() {
        // check to see if the next number to be assigned is already
        // occupied. remember the number to started with, so that we
        // don't get stuck in the loop forever
        long firstTry = nextDocNum;
        while (true) {
            Long numObj = new Long(nextDocNum);
            if (docNumSet.contains(numObj)) {
                nextDocNum++;
                if (nextDocNum == firstTry) {
                    // swept thru one round and not be able to find a
                    // good number. overflow.
                    return -1;
                }
            }
            else break;
        }
        long ret = nextDocNum;
        nextDocNum++;
        return ret;
    }
}
