
/**********************************************************************
  $Id: BibDocGen.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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
 * <?xml encoding="US-ASCII"?>
 *
 * <!ELEMENT bib (vendor)*>
 * <!ELEMENT vendor (name, email, book*)>
 * <!ATTLIST vendor id ID #REQUIRED>
 * <!ELEMENT book (title, author+, price)>
 * <!ELEMENT author (firstname?, lastname)>
 * <!ELEMENT name (#PCDATA)>
 * <!ELEMENT email (#PCDATA)>
 * <!ELEMENT title (#PCDATA)>
 * <!ELEMENT firstname (#PCDATA)>
 * <!ELEMENT lastname (#PCDATA)>
 * <!ELEMENT price (#PCDATA)>
 *
 * For usage, type "java BibDocGen"
 *
 */

public class BibDocGen {

	private static int nfiles = 0; // number of files to generate
	private static String path = null; // path to put the files in

	public static void main (String args[]) {
                
		if (args.length < 4) {
			System.out.println ("Usage: BibDocGen [options]");
			System.out.println ("options:");
			System.out.println ("\t-nfiles <num-files>");
			System.out.println ("\t-path <path>");
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
		}

		Random vendorR=new Random(2);
		Random bookR=new Random(3);
		Random authorR=new Random(5);
		Random firstNameR=new Random(7);
		Random priceR=new Random(11);

		for (int doci=0; doci<nfiles; doci++) {
			try {
				String filename = "bib_"+doci+".xml";
				File fp = new File(path, filename);
				FileOutputStream fo = new FileOutputStream(fp);
				PrintStream ps = new PrintStream(fo);

				// Generate Header
				ps.println ("<?xml version=\"1.0\"?>");

				//DOCTYPE 
				ps.print ("<!DOCTYPE bib SYSTEM ");
				ps.println ("\"bib.dtd\">\n");

				//<bib>
				ps.println ("<bib>\n");

				// Generate Vendors
				int nvendors = vendorR.nextInt(10);
				for (int vi = 0; vi < nvendors; vi++) {
					ps.println ("<vendor id=\"id"+doci+"_"+vi+"\">");
					ps.println (" <name>VendorName_"+doci+"_"+vi+"</name>");
					ps.println (" <email>Vendor_"+doci+"_"+vi+"@blah.blah.blah</email>\n");
					// books
					int nbooks = bookR.nextInt(10);
					if (nbooks==0) nbooks = 1;
					for (int bi=0; bi<nbooks; bi++) {
						ps.println (" <book>");
						ps.println ("  <title>Title "+bi+" of Vendor "+vi+" of Bib "+doci+"</title>");
						// authors
						int nauthors = authorR.nextInt(3);
						if (nauthors==0) nauthors = 1;
						for (int ai=0; ai<nauthors; ai++) {
							ps.println ("  <author>");
							
							int nfirstname = firstNameR.nextInt(1000);
							if (nfirstname % 2==1) {
								ps.println ("    <firstname>FirstName_"+doci+"_"+vi+"_"+bi
                                            +"_"+ai+"</firstname>");
							}
							ps.println ("    <lastname>LastName_"+doci+"_"+vi+"_"+bi+
                                            "_"+ai+"</lastname>");
							ps.println ("  </author>"); 
						}

						ps.println("  <price>"+priceR.nextInt(100)+"."+priceR.nextInt(100)+"</price>");
						ps.println (" </book>\n");
					}

					ps.println ("</vendor>\n");
				}

				//</personnel>
				ps.println ("</bib>\n");

				fo.close();
				ps.close();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		} // main
	}
}
