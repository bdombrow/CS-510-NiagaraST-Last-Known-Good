
/**********************************************************************
  $Id: Entry.java,v 1.1 2000/05/30 21:03:25 tufte Exp $


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


package niagara.client.qbe;

/**
  * entry for the DTD tree
  */

import java.util.*;
import java.awt.*;
import javax.swing.*;

////////////////////////
// parser data structure
// one entry
////////////////////////
public class Entry
{
    public JButton openButton;    // button whether the element is opened or not
    public JButton projButton;    // button for the projection
    public JLabel   fieldLabel;   // label for contents and attributes
    public Font labelFont;

    public boolean isOpened;      // opened, arrowopened.gif; false, arrowclosed.gif

    public boolean isRemoved;     // whether to display it or not

    public int     indent;        // indent level
    public int     line;          // line in the tree


    public boolean isChosen;     // true = this entry is as query 

    // interface 
    // ---------

    public boolean isLeaf;       // the leaf node, if leaf, no arrow.gif
    public boolean isAttribute;  // attribute
    public String  path;         // name path
    public boolean isProjected;  // true, redball.gif; false, blueball.gif
    public boolean isConstructed;// construct contents
    public String  predicate;    // predicate
    public int     joinIndex;    // = 0, not join with other entries
    public Color   joinColor;    // join color

    public String fieldName;

    Entry( JButton open_button, JButton proj_button, JLabel field_label,
	   boolean is_projected, boolean is_leaf, boolean is_opened, 
	   boolean is_removed, 
	   boolean is_attribute,
	   String  pathname,
	   int indent_level, int lineNum )
    {
	openButton = open_button;
	projButton = proj_button;
	fieldLabel = field_label;
	
	fieldName = fieldLabel.getText();

	isProjected = is_projected;
	isLeaf = is_leaf;
	isOpened = is_opened;

	isRemoved = is_removed;

	indent = indent_level;
	line = lineNum;

	isAttribute = is_attribute;

	isChosen = false;

	isConstructed = false;

	predicate = null; //new String();

	path = pathname;

	joinIndex = 0;

	joinColor = Color.black;
	
	// Default Font
	//
	labelFont = new Font("SansSerif", Font.PLAIN, 12);
    }

    public void setPredicate(String cond)
    {
	predicate = cond;
    }

    public void setLabel(String fieldName)
    {
	fieldLabel.setText(fieldName);
    }

    public void setLabelFont(Font f)
    {
	labelFont = f;
    }

    public void printEntry()
    {
	System.out.println(path);
	System.out.print("  isLeaf="+isLeaf);
	System.out.print("  isAttribute="+isAttribute);
	System.out.println("  isConstructed="+isConstructed);

	System.out.print("  isProjected="+isProjected);
	System.out.print("  predicate="+predicate);
	System.out.print("  joinIndex="+joinIndex);
	System.out.println();
    }
}


