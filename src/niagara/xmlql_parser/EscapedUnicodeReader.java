
/**********************************************************************
  $Id: EscapedUnicodeReader.java,v 1.1 2003/12/24 01:19:54 vpapad Exp $


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


package niagara.xmlql_parser;


// taken from the CUP website from the handwritten Lexer by Scott Ananian 
// <cananian@alumni.princeton.edu> 

import java.io.Reader;
import java.io.FilterReader;
import java.io.IOException;

public class EscapedUnicodeReader extends FilterReader {

  int pushback=-1;
  boolean isEvenSlash = true;

  public EscapedUnicodeReader(Reader in) {
    super(in);
  }
  public int read() throws IOException {
    int r = (pushback==-1)?in.read():pushback; pushback=-1;
    
    if (r!='\\') {
      isEvenSlash=true;
      return r;
    } else { // found a backslash;
      if (!isEvenSlash) { // Only even slashes are eligible unicode escapes.
	isEvenSlash=true;
	return r;
      }
      
      // Check for the trailing u.
      pushback=in.read();
      if (pushback!='u') {
	isEvenSlash=false;
	return '\\';
      }

      // OK, we've found backslash-u.  
      // Reset pushback and snarf up all trailing u's.
      pushback=-1;
      while((r=in.read())=='u')
	;
      // Now we should find 4 hex digits. 
      // If we don't, we can raise bloody hell.
      int val=0;
      for (int i=0; i<4; i++, r=in.read()) {
	int d=Character.digit((char)r, 16);
	if (r<0 || d<0)
	  throw new Error("Invalid unicode escape character.");
	val = (val*16) + d;
      }
      // yeah, we made it.
      pushback = r;
      isEvenSlash=true;
      return val;
    }
  }

  public boolean markSupported() { return false; }

  public boolean ready() throws IOException {
    if (pushback!=-1) return true;
    else return in.ready();
  }
}
