
/**********************************************************************
  $Id: DTDScan.java,v 1.2 2003/03/08 01:01:53 vpapad Exp $


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

/**
 * DTDScan.java
 *
 *
 * Created: Wed Apr 21 00:16:10 1999
 *
 * @version
 */

import java.io.*;
import java.net.URL;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.Enumeration;

/**
 * the purpose of this class to to get the root element of an DTD
 * so that we could construct an empty xml document with internal
 * DTD and use IBM xml parsing tool to get the parsed version of
 * DTD
 *
 */
public class DTDScan  {
    
    //the string representation of DTD
    //
    public String dtdStr;
    
    /**
     * constructs an DTD scan, using the url of target DTD
     *
     * @param url of the DTD
     *
     */
    public DTDScan(String url) {
        BufferedReader input;
        String tmpStr="";
        dtdStr = "";
        
        try {
            
            //read from file
            //
            if (url.indexOf(":") < 0) {
                FileInputStream inStream = new FileInputStream(url);
                input = new BufferedReader(new InputStreamReader(inStream));
            }
            
            // read from a URL stream
            //
            else {
                URL aurl = new URL(url);
                InputStream inStream = aurl.openStream();
                input = new BufferedReader(new InputStreamReader(inStream));
            }
            
            tmpStr = input.readLine();
      
            //fill dtdStr
            //
            while(tmpStr != null) {
                dtdStr += tmpStr;
                tmpStr = input.readLine();
            }
        }
        catch (IOException ioe) {
            System.err.println("DTDScan:DTDScan io err");
        }
            
    }//of DTDScan
    

    /**
     * get relevant part of DTD
     * only ELEMENT tags are preserved
     * ATTRIBUTE and ENTITY tags are ignored
     * used by getRootElement
     *
     *
     */
    private String pruneDTDString()
    {
        int index1 = dtdStr.indexOf("<!ELEMENT");
        int index2;
        
        String tmpStr = "";
        int lenEleTag = 9;
      
        //copy the string between <!ELEMENT and >
        //
        while (index1 > 0) {
            index2 = dtdStr.indexOf(">", index1);
            tmpStr += dtdStr.substring(index1+lenEleTag, index2+1);
            index1 = dtdStr.indexOf("<!ELEMENT", index2);
        }
        return tmpStr;
    }
    

    /**
     * get the name of the root element in the DTD, which
     * will be used in DOCTYPE definition of any xml that 
     * conforms to this DTD
     *
     * @return the string, name of the root element
     *
     */
    public String getRootElement()
    {
        String eleStr = pruneDTDString();
        
        if (eleStr == null){
            return null;
        }
        
        StringTokenizer strTk = new StringTokenizer(eleStr, ">");
        Vector eleDefVector = new Vector();
        
        //get each element definition
        //
        while (strTk.hasMoreTokens()){
            eleDefVector.addElement(strTk.nextToken());
        }
        
        StringTokenizer eleTk;
        int size = eleDefVector.size();
        
        Hashtable eleHT = new Hashtable();
        Vector eleVector = new Vector(size);
        Vector setVector = new Vector(size);
        setVector.setSize(size);
        
        
        String ele;
        String eleComponent;
        int maxIndex = 0;
        
        //build the (1)eleVector(all the element)
        //(2)setVector, vector of sets that contains all the child element
        //of one element
        //(3) eleHT, hashtable mapping element(String) to index(int) of the
        //string inside eleVector
        //
        for (int i = 0; i < size; i++) {
            ele = (String)eleDefVector.elementAt(i);
            
            eleTk = new StringTokenizer(ele, " ()*?,|+#");
            
            int j = 0;
            String firstComponent = null;
            HashSet childSet;
            
            while(eleTk.hasMoreTokens()){
                eleComponent = eleTk.nextToken();
                
                if (eleComponent.compareTo("PCDATA") != 0) {
                    
                    if(   !eleHT.containsKey(eleComponent) ) {
                        
                        eleVector.addElement(eleComponent);
                        eleHT.put(eleComponent, new Integer(maxIndex));
                        maxIndex ++;
                    }
                    
                    if (j == 0) {
                        firstComponent = eleComponent;
                        j = 1;
                    }
                    else {
                        //not the first , i.e., is child of element
                        //put in set of this element
                        int eleInd = 
                            ((Integer)eleHT.get(firstComponent)).intValue();
                        if (setVector.elementAt(eleInd) == null) {
                            childSet = new HashSet();
                            setVector.setElementAt(childSet, eleInd);
                        }
                        else {
                            childSet = (HashSet)setVector.elementAt(eleInd);
                    }
                        childSet.add(eleComponent);
                    }//of if(j == 0)
                }//of if(...PCDATA)
                
            }//of while
        }//of for

        //System.out.println(eleVector.toString());
        //System.out.println(setVector.toString());

        //System.out.println("\n------------\n");
        
        int numEle = eleVector.size();
        int numSet = setVector.size();
        String element = null;
        HashSet set = null;
        
        for (int i = 0; i < numEle; i ++) {
            element = (String)eleVector.elementAt(i);
         
            //if this element appears in other element's 
            //set of children, then it cannot be root
            //remove from eleHT
            //
            for (int j = 0; j < numSet; j++) {
                set = (HashSet)setVector.elementAt(j);
                if ((set != null) && (set.contains(element))){
                    eleHT.remove(element);
                }
            }
        }
        
        if (eleHT.size() != 1) {
            System.err.println("invalid root"+eleHT.size());
            //System.out.println(eleHT.toString());
            
        }

        //above : trimmed the eleHT; in a for loop
        //should have only one left, that's the root
        
        Enumeration enum = eleHT.keys();
        if (enum.hasMoreElements()) {
            return (String)enum.nextElement();
        }
        else {
            return null;
        }
        
    }//of getRootElement

} // DTDScan
