
/**********************************************************************
  $Id: CacheUtil.java,v 1.7 2003/09/13 03:46:15 vpapad Exp $


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


package niagara.data_manager;

/** Niagra DataManager
  * Cache Utility
  */

import java.io.*;
import java.net.*;
import java.util.*;
import gnu.regexp.*;
import org.w3c.dom.*;

import niagara.ndom.*;
import niagara.utils.*;


public class CacheUtil {
    private static RE initUrlRE() {
        RE ret = null;
        try {
            ret = new RE("http:.*", RE.REG_ICASE);
	} catch (gnu.regexp.REException e) {
	    // KT - not sure what to do, throw this for now
	    // previous code just ignored this exception @*#$*@
	    throw new PEException("gnu regexp exception: " + e.getMessage());
	}
        return ret;
    }
    private static RE initPathRE() {
        RE ret = null;
        try {
            ret = new RE("file:(.*)", RE.REG_ICASE);
	} catch (gnu.regexp.REException e) {
	    // KT - not sure what to do, throw this for now
	    // previous code just ignored this exception @*#$*@
	    throw new PEException("gnu regexp exception: " + e.getMessage());
	}
        return ret;
    }
    private static RE initRE(String re) {
        RE ret = null;
        try {
            ret = new RE(re);
	} catch (gnu.regexp.REException e) {
	    // KT - not sure what to do, throw this for now, previous
	    // code just ignored this exception @*#$*@
	    throw new PEException("gnu regexp exception: " + e.getMessage());
	}
        return ret;
    }
    public static final RE UrlRex = initUrlRE();
    public static final RE PathRex = initPathRE();
    public static final RE TrigTmpRex = initRE("^TRIG_.*");

    public static boolean isUrl(String s) {
        return UrlRex.isMatch(s);
    }
    public static boolean isTrigTmp(String s) {
        int tmp = s.lastIndexOf('/');
        String ts = s.substring(tmp+1);
        return TrigTmpRex.isMatch(ts);
    }   
    public static boolean isOrdinary(String s) {
        if(s.lastIndexOf('&')==-1) return true;
        else return false;
    }

    public static boolean isAccumFile(String name) {
	if(DataManager.AccumFileDir.containsKey(name)) {
	    return true;
	} else {
	    return false;
	}
    }

    public static String fileToUrl(String f) {
        int lastSlash = f.lastIndexOf('/');
        String url = f.substring(lastSlash+1);
        url = url.replace('%', ':');
	url = url.replace('@', '~');
        return url.replace('#', '/');
    }

    public static String urlToFile(String u) {
        String ret =  u.replace('/', '#');
	ret = ret.replace('~', '#');
        return ret.replace(':', '#');
    }
    
    public static String normalizePath(Object k) {
        String f = (String)k;
        if(isUrl(f)) return f;
        else if(PathRex.isMatch(f)) f = f.substring(5);
        File tmpF = new File(f);
        String absF = tmpF.getAbsolutePath();
        return absF;
    }

    public static String pathToFile(String f) {
        // file: discarded
        String ret = null;
        if(PathRex.isMatch(f)) ret = f.substring(5);
        else ret = f;
        // convert path to absolute path.  Save trouble.
        File tmpF = new File(ret);
        if(!tmpF.exists()) return null;
        String absF = tmpF.getAbsolutePath();
        ret = absF.replace('/', '#');
        return ret;
    }
    
    public static void fetchUrl(String url, String dfn) throws IOException {
        FileWriter fw=new FileWriter(dfn);
        URL Url = new URL(url);
        BufferedReader in = new BufferedReader(new 
                InputStreamReader(Url.openConnection().getInputStream()));
        String inputline = null;
        while((inputline = in.readLine()) != null) {
            // // System.err.println(inputline);
            fw.write(inputline+"\n");
        }
        fw.close();
	System.out.println("Fetch url done!");
    }

    public static void fetchLocal(String f, String dfn) throws IOException {
        String fn = f;
        if(PathRex.isMatch(f)) fn = f.substring(5);
        // System.err.println("Fetching local file : " + fn);
        FileInputStream fin = new FileInputStream(fn);
        FileOutputStream fout = new FileOutputStream(dfn);
        BufferedInputStream bin = new BufferedInputStream(fin);
        BufferedOutputStream bout = new BufferedOutputStream(fout);
        byte[] tmpB = new byte[4096];
        int count = 0;
        while((count = bin.read(tmpB, 0, 4096))!=-1) {
            // System.err.println("++++++++ WRITTING >>>>> ");
            bout.write(tmpB, 0, count);
        }
        bout.close();
    }

    // If return 0, means timeStamp not supported by URL.
    // or local file not exist.
    public static long getTimeStamp(String s) {
        long ret = 0;
        if(!CacheUtil.isUrl(s)) {
            File tmpF = new File(s);
	    if(!tmpF.exists()) return 0;
            ret = tmpF.lastModified();
        }
        else {
            try {
		URL Url = new URL(s);
		URLConnection uc = Url.openConnection();
		ret = uc.getLastModified();
	    } catch (java.net.MalformedURLException mue) {
		System.err.println("Cannot get network TimeStamp " + 
				   mue.getMessage());
		mue.printStackTrace();
		return 0;
            } catch (java.io.IOException ioe) {
		System.err.println("IO exception getting time stamp " +
				   ioe.getMessage());
		ioe.printStackTrace();
		return 0;
	    }
        }
        return ret;
    }

    public static void flushXML(String fname, Document doc) {
        // System.err.println(" @@@@@@@ Flushing " + fname);
        Element root = doc.getDocumentElement();
        String tsp = root.getAttribute("TIMESPAN");
        // System.err.println("time span is " + tsp);
        if(tsp!=null && !tsp.equals("") && Long.parseLong(tsp)==0) {
            return;
        }
        String dirty = root.getAttribute("DIRTY");
        if(dirty!=null && !dirty.equals("")) root.setAttribute("DIRTY", "FALSE");
        try {
            PrintWriter pw = new PrintWriter(new FileOutputStream(fname));
	    System.err.println("Unable to print document - print method not supported by Document");
            // KT doc.print(pw);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public static Vector getVecSince(Vector v, long since, long to) {
        Vector ret = new Vector();
	System.out.println("CU::getVecSince " + since + " size " + v.size());
        for(int i=0; i<v.size(); i++) {
            Object n = v.elementAt(i);
            if(n instanceof Element) {
                // System.err.println("getVecSince. converting ... " + n);
                Element next = (Element)v.elementAt(i);
                String ts = next.getAttribute("TIMESTAMP");
                long tst = Long.parseLong(ts);
                // System.err.println("timestamp " + tst + " since " + since);
                if(since < tst) {
                    ret.addElement(new StreamTupleElement(next));
                }
            }
            else if(n instanceof StreamTupleElement) {
                StreamTupleElement nste = (StreamTupleElement) n;
                long tst = nste.getTimeStamp();
                if(since < tst) {
                    // System.out.println("timestamp " + tst + " since " + since);
                    ret.addElement(nste);
                }
            }
        }
        System.out.println("%%%%%% CU::getVecSince: pushing " + ret.size());
        return ret;
    }
    public static void flushVec(String fname, long timespan, Vector vec) {
        Document doc = DOMFactory.newDocument();
        Element root = doc.createElement("ROOT");
        root.setAttribute("TIMESPAN", ""+timespan); 
        root.setAttribute("DIRTY", "FALSE");
        long threshold = System.currentTimeMillis() - timespan;
        for(int i=0; i<vec.size(); i++) {
            StreamTupleElement tmp = (StreamTupleElement)vec.elementAt(i);
            root.appendChild(tmp.toEle(doc));
        }
        doc.appendChild(root);
        flushXML(fname, doc);
    }
    public static boolean shrinkVec(long ttimespan, Vector vec) {
	System.err.println("ShrinkVec: timeSpan : " + ttimespan + " " + vec.size());
	long timespan = ttimespan;
        if(ttimespan==0) timespan = 30000; 
        boolean ret = false;
        long threshold = System.currentTimeMillis() - timespan;
        while(true) {
            if(vec.size()==0) break;
            StreamTupleElement tmp = (StreamTupleElement)vec.elementAt(0);
            if(tmp==null) break;
            if(tmp.getTimeStamp() > threshold) break;
	    System.out.println("Shrinking 1");
            vec.removeElement(tmp);
            ret = true;
        }
        return ret;
    }
    public static boolean isPush(String fileName) {
        if(TrigTmpRex.isMatch(fileName)) return true;
        return false;
    }
    public static void setTimeStamp(Vector v, long ts) {
        for(int i=0; i<v.size(); i++) {
            StreamTupleElement ste = (StreamTupleElement)v.elementAt(i);
            if(ste.getTimeStamp()==0) {
                // System.out.println("#### setTimeStamp for vec " + ts);
                ste.setTimeStamp(ts);
            }
            else  {
                // System.out.println("#### Vect Time Stamp: already SET");
            }
        }
    }

    public static boolean debugTitle(String place, Element ele) {
        String title = "Bridges of Madison County";
        NodeList nnll = ele.getElementsByTagName("Title");
        Element ttll = (Element)nnll.item(0);
        String t = ((Text)ttll.getFirstChild()).getData();
        if(title.equals(t)) {
            // System.out.println(place + " Got Title " + title);
            return true;
        }
        return false;
    }

    public static void main(String args[]) {
        String test = "aa345.xml&222";
        String test2 = "CACHE/TRIG_34_a.xml";
        String test3 = "this.is.bad&bad";
        String test4 = "../../trigger/src/data.xml";

        if(isTrigTmp(test2)) {
            System.err.println("Test trigTmp OK");
        }
        else {
            System.err.println("Test TrigTmp BAD");
        }

        System.exit(0);
    }
}
