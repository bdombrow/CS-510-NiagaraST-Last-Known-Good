
/**********************************************************************
  $Id: PersonDocGen.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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


import java.io.*;
import java.util.Random;

/**
 * Document generator for the DTD:
 *
 * <?xml encoding="US-ASCII"?>
 *
 * <!-- Revision: 60 1.2 data/personal.dtd, docs, xml4j2, xml4j2_0_0  -->
 * 
 * <!ELEMENT personnel (person)+>
 * <!ELEMENT person (name,email*,url*,link?)>
 * <!ATTLIST person id ID #REQUIRED>
 * <!ELEMENT family (#PCDATA)>
 * <!ELEMENT given (#PCDATA)>
 * <!ELEMENT name (#PCDATA|family|given)*>
 * <!ELEMENT email (#PCDATA)>
 * <!ELEMENT url EMPTY>
 * <!ATTLIST url href CDATA #REQUIRED>
 * <!ELEMENT link EMPTY>
 * <!ATTLIST link
 *   manager IDREF #IMPLIED
 * 	subordinates IDREFS #IMPLIED>
 *
 * For usage, type "java PersonDocGen"
 *
 */

public class PersonDocGen {

	private static int nfiles = 0; // number of files to generate
	private static String path = null; // path to put the files in
	private static int nelems = 0;
	private static boolean randomElems = false;

	public static void main (String args[]) {
                
		if (args.length < 4) {
			System.out.println ("Usage: PersonDocGen [options]");
			System.out.println ("options:");
			System.out.println ("\t-nfiles <num-files>");
			System.out.println ("\t-path <path>");
			System.out.println ("\t-nelem <num-elements>  [optional]\n");
			return;
		}
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-nfiles")) {
				i++;
				nfiles = new Integer(args[i]).intValue();
			}
			if (args[i].equals("-path")) {
                i++;
				path = args[i];
			}
			if (args[i].equals("-nelem")) {
				i++;
				nelems = new Integer(args[i]).intValue();
			}
		}

		Random rr=null;
		if (nelems==0) {
			rr = new Random();
			randomElems = true;
		}
		for (int doci=0; doci<nfiles; doci++) {
			try {
				String filename = "personal_"+doci+".xml";
				File fp = new File(path, filename);
				FileOutputStream fo = new FileOutputStream(fp);
				PrintStream ps = new PrintStream(fo);

				// Generate Header
				ps.println ("<?xml version=\"1.0\"?>");

				//DOCTYPE 
				ps.print ("<!DOCTYPE personnel SYSTEM ");
				ps.println ("\"personal.dtd\">\n");

				//<personnel>
				ps.println ("<personnel>\n");

				// Generate Persons
				if (randomElems) {
					nelems = rr.nextInt(100);
					System.out.println ("nelems="+nelems);
				}
				for (int pi = 0; pi < nelems; pi++) {
					ps.println ("<person id=\"id"+doci+"_"+pi+"\">");
					ps.println (" <name>");
					ps.println ("  <family>FamilyName_"+doci+"_"+pi+"</family>");
					ps.println ("  <given>GivenName_"+doci+"_"+pi+"</given>");
					ps.println (" </name>");
					ps.println (" <email>Email_"+doci+"_"+pi+"</email>");
					ps.println (" <url href=\"http://www.blah.blah/person_"+doci+"_"+pi+".html\"/>");
					ps.println ("</person>");
					ps.println ("");
				}

				//</personnel>
				ps.println ("</personnel>\n");

				fo.close();
				ps.close();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		} // main
	}
}
