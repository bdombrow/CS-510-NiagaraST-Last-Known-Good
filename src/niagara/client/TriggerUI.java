
/**********************************************************************
  $Id: TriggerUI.java,v 1.1 2000/05/30 21:03:25 tufte Exp $


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


/**
 *  User interface for installing Triggers
 *  FileName: TriggerUI.java
 */

package niagara.client; 

import java.awt.*;
import java.awt.event.*;
import java.awt.Toolkit;
import java.io.*;
import java.util.Vector;
import java.util.Date;
import javax.swing.*;
import javax.swing.border.*;
import javax.swing.event.*;
import javax.swing.tree.*;

public class TriggerUI extends JDialog implements ActionListener {
    Container trigContPane;
    
    GridBagLayout trigLayout;
    GridBagConstraints trigLayoutC;
    
    String triggerName;
    String triggerString;
    
    JButton trigOKButton;
    JButton trigCancelButton;
    
    JTextField startF;
    JTextField intervalF;
    JTextField expireF;
    JTextField trigNameF;

    JComboBox actionF;
    JComboBox intervalC;
    
    JCheckBox triggerNow;

    public static final int PAD = 10;

    // Constructor
    TriggerUI( Frame frame , boolean f ) {
	super(frame,"Install Trigger",f);
	trigContPane = this.getContentPane();
	trigLayout = new GridBagLayout();
	trigLayoutC = new GridBagConstraints();
	trigContPane.setLayout(trigLayout);
	
	setSize(400,225);
	Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
	Rectangle frameBounds = getBounds();
	
	setLocation((screenSize.width-frameBounds.width)/2,(screenSize.height-frameBounds.width)/2);

	triggerName = null;
	triggerString = null;

	createTrigger();
	
	addWindowListener(new WindowAdapter() {
	    public void windowClosing(WindowEvent e) {
		exitTrigger();
	    }
	});
	
	setVisible(true);
    }
    
    private void createTrigger() {
	GridBagLayout trig = new GridBagLayout();
	GridBagConstraints trigC = new GridBagConstraints();
	JPanel textPanel = new JPanel(trig);
	
	JLabel start = new JLabel("Start Date");
	JLabel interval = new JLabel("Interval");
	JLabel expire = new JLabel("Expiration Date");
	JLabel action = new JLabel("Action");
	JLabel trigName = new JLabel("Trigger Name");
	
	startF = new JTextField();
	intervalF = new JTextField();
	intervalF.setText("0");
	expireF = new JTextField();
	actionF = new JComboBox();
	trigNameF = new JTextField();
	triggerNow =  new JCheckBox("Execute Now");
	
	actionF.setEditable(true);
	actionF.setBackground(Color.white);
	actionF.setFont(startF.getFont());
	actionF.addItem("SendTo: <deafult>");
	actionF.addItem("Mailto: <email>");
	
	intervalC = new JComboBox();
	intervalC.setEditable(false);
	intervalC.addItem(new String("msec"));
	intervalC.addItem(new String("secs"));
	intervalC.addItem(new String("mins"));
	intervalC.addItem(new String("hours"));
	intervalC.addItem(new String("days"));
	
	trigC.gridx = 0;
	trigC.gridy = 0;
	trigC.weightx = 0.1;
	trigC.weighty = 1.0;
	trigC.fill = GridBagConstraints.BOTH;
	trigC.insets = new Insets(PAD,PAD,2,PAD);
	trig.setConstraints(start,trigC);
	textPanel.add(start);
	
	trigC.gridx = 1;
	trigC.gridy = 0;
	trigC.weightx = GridBagConstraints.REMAINDER;
	trigC.gridwidth = GridBagConstraints.REMAINDER;
	trig.setConstraints(startF,trigC);
	textPanel.add(startF);
	
	trigC.gridx = 0;
	trigC.gridy = 1;
	trigC.weightx = 0.1;
	trigC.gridwidth = 1;
	trigC.insets = new Insets(2,PAD,2,PAD);
	trig.setConstraints(interval,trigC);
	textPanel.add(interval);
	
	trigC.gridx = 1;
	trigC.gridy = 1;
	trigC.weightx = 0.3;
	trig.setConstraints(intervalF,trigC);
	textPanel.add(intervalF);
	
	trigC.gridx = 2;
	trigC.gridy = 1;
	trigC.weightx = GridBagConstraints.REMAINDER;
	trig.setConstraints(intervalC,trigC);
	textPanel.add(intervalC);
	
	trigC.gridx = 0;
	trigC.gridy = 2;
	trigC.weightx = 0.1;
	trig.setConstraints(expire,trigC);
	textPanel.add(expire);
	
	trigC.gridx = 1;
	trigC.gridy = 2;
	trigC.weightx = GridBagConstraints.REMAINDER;
	trigC.gridwidth = GridBagConstraints.REMAINDER;
	trig.setConstraints(expireF,trigC);
	textPanel.add(expireF);
	
	trigC.gridx = 0;
	trigC.gridy = 3;
	trigC.weightx = 0.1;
	trigC.gridwidth = 1;
	trig.setConstraints(action,trigC);
	textPanel.add(action);
	
	trigC.gridx = 1;
	trigC.gridy = 3;
	trigC.weightx = GridBagConstraints.REMAINDER;
	trigC.gridwidth = GridBagConstraints.REMAINDER;	
	trig.setConstraints(actionF,trigC);
	textPanel.add(actionF);
	
	trigC.gridx = 0;
	trigC.gridy = 4;
	trigC.weightx = 0.1;
	trigC.gridwidth = 1;
	trig.setConstraints(trigName,trigC);
	textPanel.add(trigName);
	
	trigC.gridx = 1;
	trigC.gridy = 4;
	trigC.weightx = GridBagConstraints.REMAINDER;
	trigC.gridwidth = GridBagConstraints.REMAINDER;
	trig.setConstraints(trigNameF,trigC);
	textPanel.add(trigNameF);
	
	trigC.gridx = 1;
	trigC.gridy = 5;
	trigC.weightx = 1.0;
	trigC.gridwidth = 1;
	trig.setConstraints(triggerNow,trigC);
	textPanel.add(triggerNow);
	
	JPanel buttonPanel = new JPanel(trig);
	Border bb = BorderFactory.createLoweredBevelBorder();
	buttonPanel.setBorder(bb);
	
	trigOKButton = new JButton("OK");
	trigCancelButton = new JButton("Cancel");
	trigOKButton.addActionListener(this);
	trigCancelButton.addActionListener(this);
	
	trigC.gridx = 0;
	trigC.gridy = 0;
	trigC.weightx = 0.5;
	trigC.weighty = 1;
	trigC.insets = new Insets(2,2,2,2);
	trigC.gridwidth = 1;
	trig.setConstraints(trigOKButton,trigC);
	buttonPanel.add(trigOKButton);
	
	trigC.gridx = 1;
	trigC.gridy = 0;
	trig.setConstraints(trigCancelButton,trigC);
	buttonPanel.add(trigCancelButton);
	
	trigLayoutC.gridy = 0;
	trigLayoutC.weightx = 1.0;
	trigLayoutC.weighty = 0.90;
	trigLayoutC.fill = GridBagConstraints.BOTH;
	trigLayout.setConstraints(textPanel, trigLayoutC);
	trigContPane.add(textPanel);
	
	trigLayoutC.gridy = 1;
	trigLayoutC.weighty = 0.10;
	trigLayout.setConstraints(buttonPanel, trigLayoutC);
	trigContPane.add(buttonPanel);
    }
    
    private void exitTrigger() {
	setVisible(false);
    }
    
    public boolean isInstallNow() {
	return triggerNow.isSelected();
    }
    
    public String getTrigName() {
	String name = new String();
	name += "CREATE TRIGGER ";
	if ( trigNameF.getText().equals("") ) 
	    name += "t"+"_"+((new Date()).getTime())+"\n\n";
	else
	    name += trigNameF.getText()+"_"+((new Date()).getTime())+"\n\n";
	return name;
    }
    
    public String createTriggerText(long interval) {
	String trig = new String();
	
	trig += "\n";

	if ( !startF.getText().equals("") ) 
	    trig += "START \"" + startF.getText() + "\" ";
	
	if ( interval != -1 )
	    trig += "EVERY \"" + String.valueOf(interval) + "\" ";
	
	if ( !expireF.getText().equals("") )
	    trig += "EXPIRE \"" + expireF.getText() + "\" ";
	
	trig += "\nDO \"" + (String)actionF.getSelectedItem() + "\"";
	
	return trig;
    }
    
    /**
     * Return the trigger interval
     * If unit is not msec, then convert to msec
     * @return the trigger interval
     */
    public long getInterval() {
	if ( intervalF.getText().equals("") ) return -1;
	
	long interval = Integer.parseInt(intervalF.getText());
	
	int index = intervalC.getSelectedIndex();
	
	switch ( index ) {
	case 1:
	    interval *= 1000;
	    break;
	case 2:
	    interval *= 60*1000;
	    break;
	case 3:
	    interval *= 60*60*1000;
	    break;
	case 4:
	    interval *= 24*60*60*1000;
	    break;
	}
	
	return interval;
    }
    
    public void actionPerformed(ActionEvent e) {
	String actionCommand = e.getActionCommand();
	
	if ( actionCommand.equals(trigOKButton.getText()) ) {
	    long interval = getInterval();
	    
	    triggerName = getTrigName();
	    triggerString = createTriggerText(interval);
	    
	    exitTrigger();
	}
	
	if ( actionCommand.equals(trigCancelButton.getText()) ) {
	    exitTrigger();
	}
    }

    public String getTriggerName() {
	return triggerName;
    }

    public String getTriggerString() {
	return triggerString;
    }
}
