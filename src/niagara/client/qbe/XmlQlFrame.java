
/**********************************************************************
  $Id: XmlQlFrame.java,v 1.1 2000/05/30 21:03:25 tufte Exp $


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
import java.io.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.border.*;

////////////////
// Xml Ql frame
////////////////
class XmlQlFrame extends JFrame
{
    String      title;          // frame title

    public XmlQlDialog xmlQlDialog;

    //////////////
    // constructor
    //////////////
    public XmlQlFrame(String tit, String query)
    {
	title = tit;

	//setBounds(200, 200, 410, 410);
	this.setBackground(Color.white);

	// dialog
	xmlQlDialog = new XmlQlDialog(this, "Show XML Query", query);
	xmlQlDialog.setModal(true);

	xmlQlDialog.setBounds(200, 110, 600, 400);
	xmlQlDialog.setVisible(true);
	xmlQlDialog.setBackground(Color.white);
	
    }
}

////////////////
// Xml Ql dialog
////////////////
class XmlQlDialog extends JDialog
{

    final int Width  = 500;   // dialog width
    final int Height = 400;   // dialog height

    int labelPosx = 80;
    int labelPosy = 50;
    int labelWidth = 400;
    int labelHeight = 40;

    int buttonWidth = 110;
    int buttonHeight = 25;

    JTextArea  qlText;       // query text
    JButton    ok;           // ok button
    JButton    cancel;       // cancel button

    ActionLn  actionLn;

    // Static class vars
    //
    static String qltxt = new String("(no XML-QL query generated)");    
    static int fontsize = 16;
    static Font textFont = new Font("SansSerif", Font.PLAIN, 14);
    static Font buttonFont = new Font("SansSerif", Font.PLAIN, 10);
    static Font labelFont = new Font("SansSerif", Font.PLAIN, 12);

    //////////////
    // constructor
    //////////////
    public XmlQlDialog(JFrame parent, String title, String query)
    {
	super(parent);
	setTitle(title);

	this.getContentPane().setLayout(null);
	this.getContentPane().setBackground(Color.white);

	qltxt = query;

	// Add the text field for xml-ql
	//
	qlText = new JTextArea();
	qlText.setBounds(60, 30, 1000, 1000);
	qlText.setFont(textFont);
	qlText.setBackground(new Color(230, 230, 230));
	qlText.setText(qltxt);
	JScrollPane sp = new JScrollPane(qlText, JScrollPane.VERTICAL_SCROLLBAR_ALWAYS,
					 JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);

	sp.setBorder(new EtchedBorder());
	sp.setBounds(60, 30, 450, 250);
	this.getContentPane().add(sp);

	// Create and add the ok and cancel buttons
	//
	ok = new JButton("OK");
	ok.setFont(buttonFont);
	ok.setBounds(110, 320, buttonWidth, buttonHeight);
	ok.setBackground(Color.lightGray);
	ok.setForeground(Color.black);

	cancel = new JButton("Cancel");
	cancel.setFont(buttonFont);
	cancel.setBounds(370, 320, buttonWidth, buttonHeight);
	cancel.setBackground(Color.lightGray);
	cancel.setForeground(Color.black);

	this.getContentPane().add(ok);
	this.getContentPane().add(cancel);

	// action listener
	actionLn = new ActionLn();
	ok.addActionListener(actionLn);
	cancel.addActionListener(actionLn);

    }

    public void setQL(String xmlQlText)
    {
	if(xmlQlText == null || xmlQlText.trim() == "")
	    qlText.setText("(no query generated)");
	else
	    qlText.setText(xmlQlText);
    }

    class ActionLn implements ActionListener
    {
	public void actionPerformed(ActionEvent event)
	{
	    Object object = event.getSource();

	    // ok button
	    if( object == ok)
	    {
		ok_action_performed(object);
	    }
	    // cancel button
	    else if( object == cancel )
	    {
		cancel_action_performed(object);
	    }
	    else
	    {
		System.err.println("Impossible!");
	    }
	}
    }

    void ok_action_performed(Object object)
    {
	setVisible(false);
    }

    void cancel_action_performed(Object object)
    {
	//this.setVisible(false);
	setVisible(false);
    }
}






