
/**********************************************************************
  $Id: QueryDialog.java,v 1.2 2003/07/08 02:08:21 tufte Exp $


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

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.border.*;



/**
 *
 */
class QueryDialog extends JDialog
{

    final int Width  = 500;   // dialog width
    final int Height = 400;   // dialog height

    int fontsize = 12;

    int labelPosx = 80;
    int labelPosy = 20;
    int labelWidth = 400;
    int labelHeight = 40;

    int projCheckWidth = 160;
    int projCheckHeight = 40;
   
    int buttonWidth = 110;
    int buttonHeight = 25;

    JCheckBox  projCheck;       // check box for projection
    JCheckBox  consCheck;       // check box for construct
    JTextField queryText;       // query text
    JButton    ok;              // ok button
    JButton    cancel;          // cancel button

    Font labelFont;
    Font buttonFont;

    ActionLn  actionLn;
    ObjectDes objectDes;

    final Color bg = Color.white;

    // return values
    // -------------
    String    predicate;
    boolean   projected;
    boolean   constructed;    
    boolean   okClicked;

   
    /**
     *  Constructor
     */
    public QueryDialog(JFrame parent, String title, ObjectDes objectDes)
    {
	super(parent);
	setTitle(title);

	// Set up some standard fonts for labels and buttons
	//
	labelFont = new Font("SansSerif", Font.PLAIN, 12);
	buttonFont = new Font("SansSerif", Font.PLAIN, 10);

	// object
	Entry  currEntry  = objectDes.entry;
	int    panelIndex = objectDes.panelIndex;

	this.getContentPane().setLayout(null);
	this.getContentPane().setBackground(bg);

	// Path
	//
	JLabel nameLabel = new JLabel("Element : ");
	nameLabel.setBounds(labelPosx, labelPosy, 100, labelHeight);
	nameLabel.setFont(labelFont);
	nameLabel.setBackground(bg);
	nameLabel.setForeground(Color.black);
	this.getContentPane().add(nameLabel);

	JLabel pathLabel = new JLabel(currEntry.path);
	pathLabel.setBounds(labelPosx+100, labelPosy, labelWidth, labelHeight);
	pathLabel.setFont(labelFont);
	pathLabel.setForeground(Color.blue);
	pathLabel.setBackground(bg);
	this.getContentPane().add(pathLabel);

	// Border panel
	//
	JPanel bordPanel = new JPanel();
	bordPanel.setBounds(labelPosx, labelPosy+50, 300, projCheckHeight*3);
	this.getContentPane().add(bordPanel);
	Border lineBorder = BorderFactory.createLineBorder(Color.black);
	bordPanel.setBackground( new Color( 220, 220, 220 ) );
	bordPanel.setLayout(null);
	bordPanel.setBorder(lineBorder);


	// Check box for projection
	//
	projCheck = new JCheckBox("Project this element",false);
	projCheck.setBounds(30, 20, 240, projCheckHeight);
	projCheck.setFont(labelFont);
	projCheck.setSelected( currEntry.isProjected);
	projCheck.setBackground(new Color( 220, 220, 220 ));
	bordPanel.add(projCheck);

	// Check box for construct
	//
	consCheck = new JCheckBox("Bind Element Content",false);
	consCheck.setBounds(30, projCheckHeight+15, 240, projCheckHeight);
	consCheck.setFont(labelFont);
	consCheck.setSelected( currEntry.isConstructed);
	consCheck.setBackground(new Color(220, 220, 220));
	consCheck.setVisible(false);
	bordPanel.add(consCheck);

	// Example predicate label
	//
	JLabel predLabel = new JLabel("Predicate ");
	predLabel.setBounds(labelPosx, 200, 100, labelHeight);
	predLabel.setFont(labelFont);
	predLabel.setForeground(Color.black);
	this.getContentPane().add(predLabel);

	JLabel exLabel = new JLabel("Ex: > 90 and <= 100; = 'CS'");
	exLabel.setBounds(labelPosx+100, 200, labelWidth, labelHeight);
	exLabel.setFont(labelFont);
	exLabel.setForeground(Color.blue);
	this.getContentPane().add(exLabel);


	// Text field for predicate
	//
	queryText = new JTextField();
	queryText.setBounds(labelPosx, 230, 300, 30);
	queryText.setBackground(new Color(240, 240, 240));
	queryText.setFont(labelFont);
	queryText.setText( currEntry.predicate );
	this.getContentPane().add(queryText);

	// OK button
	//
	ok = new JButton("OK");
	ok.setFont(buttonFont);
	ok.setBackground(Color.lightGray);
	ok.setBounds(labelPosx, labelPosy+290, buttonWidth, buttonHeight);

	// Cancel button
	//
	cancel = new JButton("Cancel");
	cancel.setFont(buttonFont);
	cancel.setBackground(Color.lightGray);
	cancel.setBounds(labelPosx+190, labelPosy+290, buttonWidth, buttonHeight);

	this.getContentPane().add(ok);
	this.getContentPane().add(cancel);

	// Initialize
	//
	predicate = new String();
	projected = false;
	constructed = false;

	okClicked = false;

	// Action listener
	//
	actionLn = new ActionLn();
	ok.addActionListener(actionLn);
	cancel.addActionListener(actionLn);
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
	// Get the predicate, and the projected and constructed check box state
	//
	if(queryText.getText() == null || queryText.getText().trim().length() == 0)
	    predicate = null;
	else
	    predicate = queryText.getText();
	
	projected = projCheck.isSelected();
	constructed = consCheck.isSelected();
	okClicked = true;
	this.setVisible(false);
    }

    void cancel_action_performed(Object object)
    {
	predicate = null;
	okClicked = false;
	this.setVisible(false);
    }

    /////////////////
    // get predicate
    /////////////////
    public String getPredicate()
    {
	return predicate;
    }

    ////////////////
    // is projected
    ////////////////
    public boolean isProjected()
    {
	return projected;
    }

    ////////////////
    // is constructed
    ////////////////
    public boolean isConstructed()
    {
	return constructed;
    }

    ////////////////
    // is constructed
    ////////////////
    public boolean isOkClicked()
    {
	return okClicked;
    }
}
