
/**********************************************************************
  $Id: XMLDiff.java,v 1.3 2002/08/16 17:56:05 tufte Exp $


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


/*
 * Niagra Trigger.  XML Diff
 */

/**
 * <code>XMLDiff</code> use simple algorithm to Diff 2 similiar
 * XML document.  Under reasonable assumption, the cost of diffing
 * is about the same as scaning the 2 xml file.  The diff of the 
 * 2 file is represented by sequence of insert and delete.  Update
 * is simply insert after delete.
 */

package niagara.data_manager.XMLDiff;

import com.ibm.xml.parser.*;
import com.ibm.xml.xpointer.*;
import org.w3c.dom.*;
//import org.xml.*;
import java.io.*;
import java.util.*;
import niagara.utils.*;

class Remap {
    String oldXptr;
    String newXptr;

    public Remap(String o, String n) {
        oldXptr = o;
        newXptr = n;
    }
}

class Result {
    boolean same;
    double diffRatio;
    Vector plus;
    Vector minus;
    Vector remap;
    
    public void print(PrintWriter out) {
        TXDocument doc = toDoc();
        try { 
            doc.print(out);
        } catch(IOException ioErr) {
            // System.err.println(ioErr);
            System.exit(1);
        }
    }

    public void mergeResult(TXDocument doc, long ts) {
        if(same) return;

        Element root = doc.getDocumentElement();
        root.setAttribute("LASTSTAMP", ""+ts);
        // System.out.println("%%%%%% Total result got " + (plus.size() + minus.size()) );
        // System.out.println("%%%%%% XMLDiff Diff Counter "  + plus.size());
        for(int i=0; i<plus.size(); i++) {
            Element Eleins = doc.createElement("Insert");
            Eleins.setAttribute("TIMESTAMP", ""+ts);
            Child n = (Child)plus.elementAt(i);

            XPointer p = n.makeXPointer();
            Eleins.setAttribute("POSITION", p.toString());
            Child nchild = (Child)n.cloneNode(true);
            Eleins.appendChild(nchild);
            root.appendChild(Eleins);
        }
        for(int i=0; i<minus.size(); i++) {
            Element Eledel = doc.createElement("Delete");
            Eledel.setAttribute("TIMESTAMP", ""+ts);
            Child n = (Child)minus.elementAt(i);
            XPointer p = n.makeXPointer();
            Eledel.setAttribute("POSITION", p.toString());
            Child nchild = (Child)n.cloneNode(true);
            Eledel.appendChild(nchild);
            root.appendChild(Eledel);
        }
        if(plus.size()+minus.size()!=0) {
            root.setAttribute("DIRTY", "TRUE");
        }
    }

    public TXDocument toDoc() {
        // bootstrap.  Create root element of output
        TXDocument doc = new TXDocument();
        Element root = doc.createElement("ROOT");
        doc.appendChild(root);
        
        // Now processing ....
        Element Elesame = doc.createElement("Same");
        if(same) {
            Elesame.appendChild(new TXText("TRUE"));
        }
        else Elesame.appendChild(new TXText("FALSE"));
        root.appendChild(Elesame);

        Element Eleratio = doc.createElement("DiffRatio");
        Eleratio.appendChild(new TXText((new Double(diffRatio)).toString()));
        root.appendChild(Eleratio);
        
        if(same) return doc;
        for(int i=0; i<plus.size(); i++) {
            Element Eleins = doc.createElement("Insert");
            Child n = (Child)plus.elementAt(i);
            XPointer p = n.makeXPointer();
            Eleins.setAttribute("POSITION", p.toString());
            Child nchild = (Child)n.cloneNode(true);
            Eleins.appendChild(nchild);
            root.appendChild(Eleins);
        }

        for(int i=0; i<minus.size(); i++) {
            Element Eledel = doc.createElement("Delete");
            Child n = (Child)minus.elementAt(i);
            XPointer p = n.makeXPointer();
            root.setAttribute("POSITION", p.toString());
            Child nchild = (Child)n.cloneNode(true);
            Eledel.appendChild(nchild);
            root.appendChild(Eledel);
        }
        /*
        for(int i=0; i<remap.size(); i++) {
            Element Elemove = doc.createElement("Move");
            Element Elefrom = doc.createElement("From");
            Remap tmp = (Remap)remap.elementAt(i);
            Elefrom.appendChild(new TXText(tmp.oldXptr));
            Element Eleto = doc.createElement("To");
            Eleto.appendChild(new TXText(tmp.newXptr));
            Elemove.appendChild(Elefrom);
            Elemove.appendChild(Eleto);
            root.appendChild(Elemove);
        }
        */
        return doc;
    }

    Result(boolean b) {
	same = b;
	diffRatio = 1.0;
	plus = new Vector();
	minus = new Vector();
        remap = new Vector();
    }

    void setDiffRatio(double f) {
	diffRatio = f;
    }

    double getDiffRatio() {
        return diffRatio;
    }

    void addToPlus(Object o) {
	plus.addElement(o);
    }

    void addToPlus(Vector v) {
        if(v==null) return;
        for(int i=0; i<v.size(); i++) {
            addToPlus(v.elementAt(i));
        }
    }

    void addToMinus(Object o) {
	minus.addElement(o);
    }

    void addToRemap(Child c1, Child c2) {
        /* Remapping elements.  */
        XPointer pc1 = c1.makeXPointer();
        XPointer pc2 = c2.makeXPointer();

        String spc1 = pc1.toString();
        String spc2 = pc2.toString();
        
        if(spc1.equals(spc2)) return;
        // System.err.println("OK> REMAP FOUND");
        remap.addElement(new Remap(spc1, spc2));
    }

    void addToRemap(Vector v) {
        if(v==null) return;
        for(int i=0; i<v.size(); i++) {
            remap.addElement(v.elementAt(i));
        }
    }

    void addToMinus(Vector v) {
        if(v==null) return;
        for(int i=0; i<v.size(); i++) {
            addToMinus(v.elementAt(i));
        }
    }
}

public class XMLDiff 
{
    /** 
     * threshold value for what we think 2 nodes are totally 
     * different, that is, delete/insert whole e1/e2
     */
    double Threshold;
    static boolean Level2 = true;
    // Level2 : Trigger System use QueryEngine as it is.  The
    // query plan used by QE _REQUIRE_ the Diff result to be at
    // Level2 -- This acutally speed up the diffing process.
    
    public XMLDiff(double thr) {
        Threshold = thr;
    }

    /**
     * main routine
     * Usage: java XMLDiff threshold doc1.xml doc2.xml optionout.xml
     */
    public static void main(String[] args) {
        PrintWriter OutPut = new PrintWriter(System.out, true);
	
        if(args.length==3) {
            try {
                OutputStream outstream = new FileOutputStream(args[2]);
                OutPut = new PrintWriter(outstream, true);
            } catch(IOException ioErr) {
                // System.err.println(ioErr);
                System.exit(1);
            }
        }
	else if(args.length!=2) {
            System.out.println("Usage: java XMLDiff xmlfile1 xmlfile2 [result]");
            System.exit(1);
        }
        
        Result res = doDiff(args[0], args[1]);
        if(res==null) System.err.println("Cannot get back diff result");
        res.print(OutPut);
    }
    
    public TXDocument getDiffDoc(String s1, String s2) {
        return doDiff(s1, s2).toDoc();
    }
    
    public static TXDocument getDiffDoc(TXDocument doc1, TXDocument doc2) {
        return doDiff(doc1, doc2).toDoc();
    }

    public static void getDiffDoc(TXDocument doc1, TXDocument doc2, 
            TXDocument resultDoc, long ts) 
    {
        String roottag = doc2.getDocumentElement().getTagName();
	if(roottag==null || roottag.equals("")) return;
	System.out.println("ROOTTAG IS " + roottag);
        resultDoc.getDocumentElement().setAttribute("ROOTTAG", roottag);
        if(Level2) 
            doLevel2(doc1, doc2).mergeResult(resultDoc, ts);
        else 
            doDiff(doc1, doc2).mergeResult(resultDoc, ts);
    }
    
    public static Result doLevel2(TXDocument doc1, TXDocument doc2) 
    {
        Element r1 = doc1.getDocumentElement();
        Element r2 = doc2.getDocumentElement();
        // bad diff.  OR, diff on _SAME_ object.
        // won't return any interesting result anyway.
        if(r1==null || r2==null || r1==r2) {
            Result ret = new Result(true);
            return ret;
        }
        
        NodeList nl1 = r1.getChildNodes();
        NodeList nl2 = r2.getChildNodes();

        Hashtable childHash = new Hashtable();
        Vector plus = new Vector();
        Vector minus = new Vector();

        for(int i=0; i<nl1.getLength(); i++) {
            Node n1 = nl1.item(i);
            if(n1 instanceof TXElement) { 
		TXElement nn1 = (TXElement)n1;
		String k1 = nn1.getText();
		childHash.put(k1, n1);
            }
        }

        for(int j=0; j<nl2.getLength(); j++) {
            Node n2 = nl2.item(j);
            if(n2 instanceof TXElement) {
		TXElement nn2 = (TXElement)n2;
		String k2 = nn2.getText();
                if(childHash.containsKey(k2)) {
		    childHash.remove(k2);
                }
                else plus.addElement(n2);
            }
        }
            
        if((plus.size() + minus.size())==0) {
            return new Result(true);
        }
        else {
            Result ret = new Result(false);
            ret.addToPlus(plus);
            ret.addToMinus(minus);
            return ret;
        }
    }
    
    public TXDocument mergeDiffDoc(TXDocument diff1, TXDocument diff2) {
        // System.err.println("XMLDiff merger not implemented yet!!!");
        return null;
    }
    
    public Element getDiffEle(String s1, String s2) {
        return doDiff(s1, s2).toDoc().getDocumentElement();
    }

    public static Result doDiff(String s1, String s2) {
        InputStream is1;
        InputStream is2;

        try {
            is1 = new FileInputStream(s1);
        } catch(FileNotFoundException notFound) {
             System.err.println(notFound);
            return null;
        }

        try {
            is2 = new FileInputStream(s2);
        } catch(FileNotFoundException notFound) {
            System.err.println(notFound);
            return null;
        }
        
        Parser parser1 = new Parser(s1);
        // parser1.addElementHandler(new HashElementHandler());
        Parser parser2 = new Parser(s2);
        // parser2.addElementHandler(new HashElementHandler());

        TXDocument doc1 = parser1.readStream(is1);
        TXDocument doc2 = parser2.readStream(is2);
        // System.err.println("Parse 2 doc done!");
        return doDiff(doc1, doc2);
    }

    public static Result doDiff(TXDocument doc1, TXDocument doc2) {
        TXElement r1 = (TXElement)doc1.getDocumentElement();
        TXElement r2 = (TXElement)doc2.getDocumentElement();
        // The following line is _NECESSARY_.  Once might pass
        // doc1 same as doc2, same not means the content is same,
        // _BUT_ same java obj.  The the replace thing will cause
        // a exception.
        if(r1==r2) { // this is java obj compare
            Result ret = new Result(true);
            // System.err.println("Diffing on same doc");
            return ret;
        }
        if(r1==null || r2 == null) { // bad diff?
            // System.err.println("%%%%%% Diffing on NULL");
            return(new Result(true));
        }
        HashedElement root1 = new HashedElement(r1, true);
        HashedElement root2 = new HashedElement(r2, true);

        
        // the following 2 lines of code is absolutely necessary.
        // diff use XPointers, which only can be used inside a
        // Document thing.
        doc1.replaceChild(root1, r1);
        doc2.replaceChild(root2, r2);
        
        XMLDiff mydiffer = new XMLDiff(0.85); // threshold value.
        Result result = mydiffer.diff(root1, root2);
        return result;
    }
    
    /**
     * diff routine.
     * 
     * @param
     * @return
     */
    public Result diff(HashedElement e1, HashedElement e2) {
	Result ret;
	if(e1.equals(e2)) {
	    ret = new Result(true);
            ret.addToRemap(e1, e2);
	    return ret;
	}
	
	ret = new Result(false);

	// if either Tag or Id of e1/e2 is different,
	// we thought e1/e2 are totally different

	String tag1 = e1.getTagName();
	String tag2 = e2.getTagName();

	if( (tag1==null && tag2!=null) || ( !tag1.equals(tag2) )) {
	    ret.setDiffRatio(1.0);
	    ret.addToPlus(e1);
	    ret.addToMinus(e2);
            debug.mesg("Diff on TAG");
	    return ret;
	}

	if( !e1.hasChildNodes()
	    || !e2.hasChildNodes() 
	    ) {
	    ret.setDiffRatio(1.0);
	    ret.addToPlus(e1);
	    ret.addToMinus(e2);
            debug.mesg("Diff on ChildList.");
	    return ret;
	}

        if( !e1.goodMatch(e2)) {
            ret.setDiffRatio(1.0);
            ret.addToPlus(e1);
            ret.addToMinus(e2);
            System.err.println("Diff on Not good Match");
            return ret;
        }
	// OK. Both e1 and e2 have children.  Diff on children.

        // debug.mesg("OK.  Go on to next level");
	NodeList nl1 = e1.getChildNodes();
	NodeList nl2 = e2.getChildNodes();
	int size1 = nl1.getLength();
	int size2 = nl2.getLength();
	Vector minus = new Vector();
	Vector plus = new Vector();
        
	Hashtable childHash = new Hashtable();
	
	for(int i1=0; i1<size1; i1++) {
	    Node n1 = nl1.item(i1);
	    if(n1 instanceof TXText) {
                debug.mesg("add 1 TXTest to childHash");
                HashedTXText t = new HashedTXText((TXText)n1);
		if(childHash.containsKey(t)) {
                    Vector vec = (Vector)childHash.get(t);
                    vec.addElement(n1);
		}
		else {
                    Vector vec = new Vector();
                    vec.addElement(n1);
                    childHash.put(t, vec);
                }
	    }
	    else if(n1 instanceof HashedElement) {
                debug.mesg("Adding .. " + ((TXElement)n1).getTagName());
		HashedElement h1 = (HashedElement)n1;
		if(childHash.containsKey(h1)) {
                    Vector vec = (Vector)childHash.get(h1);
                    vec.addElement(n1);
                }
		else {
                    Vector vec = new Vector();
                    vec.addElement(n1);
                    childHash.put(h1, vec);
                    // childHash.put(h1, new Integer(1));
                }
	    }
        }
	 
        for(int i2=0; i2<size2; i2++) {
            Node n2 = nl2.item(i2);
            if(n2 instanceof TXText) {
                HashedTXText s2 = new HashedTXText((TXText)n2);
                if(childHash.containsKey(s2)) {
                    debug.mesg("Deleting 1 TXText");
                    Vector vec = (Vector)childHash.get(s2);
                    ret.addToRemap((Child)vec.elementAt(0), (Child)n2);
                    vec.removeElementAt(0);
                    if(vec.size()==0) childHash.remove(s2);
                }
                else ret.addToPlus((TXText)n2);
            }
		    
            if(n2 instanceof TXElement) {
                debug.mesg("Trying Deleting " + ((TXElement)n2).getTagName());
                HashedElement h2 = (HashedElement)n2;
                // debug.var(h2);
                if(childHash.containsKey(h2)) {
                    debug.mesg("OK Deleted");
                    Vector vec = (Vector)childHash.get(h2);
                    ret.addToRemap((Child)vec.elementAt(0), (Child)n2);
                    vec.removeElementAt(0);
                    if(vec.size()==0) childHash.remove(h2);
                }
                else plus.addElement(h2);
            }
        }
            
        int dcount1 = 0;
        int dcount2 = 0;
        Enumeration k = childHash.keys();
        
        while(k.hasMoreElements()) {
            Object tmp = k.nextElement();
            if(tmp instanceof HashedTXText) {
                Vector count = (Vector)childHash.get(tmp);
                for(int i=0; i<count.size(); i++) {
                    ret.addToMinus((Child)tmp);
                }
            }
            
            else if(tmp instanceof HashedElement) {
                Vector count = (Vector)childHash.get(tmp);
                dcount1 += count.size();
                for(int i=0; i<count.size(); i++) {
                    minus.addElement(tmp);
                }
            }
                
            else debug.var(tmp);
        }
		    
        dcount2 = plus.size();
            
        double ddiff = ((double)(dcount1 + dcount2))
            / ((double)(size1+size2));
        ret.setDiffRatio(ddiff);
        
        boolean done = false;
        while(!done) {
        outer: for(int i=0; i<plus.size(); i++) {
            HashedElement p = 
                (HashedElement)plus.elementAt(i);
            for(int j=0; j<minus.size(); j++) {
                HashedElement tmp = 
                    (HashedElement)minus.elementAt(j);
                Result res = diff(tmp, p);
                double howdiff = res.getDiffRatio();
                if(howdiff < Threshold) {

                    ret.addToPlus(res.plus);
                    ret.addToMinus(res.minus);
                    ret.addToRemap(res.remap);
                    minus.removeElement(tmp);
                    plus.removeElement(p);
                    break outer; 
                    // skip done = true
                    // reset plus since we called plus.remove
                    // never used to this, sigh.
                }
            }
            if(i==plus.size()-1) done = true;
        }
            
        ret.addToPlus(plus);
        ret.addToMinus(minus);
        // ret.addToRemap(remap);
        return ret;
        }
        return ret;
    }
}
				












