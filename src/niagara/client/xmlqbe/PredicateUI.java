
/**********************************************************************
  $Id: PredicateUI.java,v 1.1 2000/05/30 21:03:25 tufte Exp $


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


package niagara.client.xmlqbe;

import java.awt.*;
import java.awt.event.*;
import java.awt.Toolkit;
import java.io.*;
import java.lang.StringBuffer;
import java.util.Vector;
import java.util.*;
import java.net.URL;
import javax.swing.*;
import javax.swing.border.*;
import javax.swing.event.*;
import javax.swing.tree.*;
import niagara.client.dtdTree.*;

public class PredicateUI extends JDialog {
    private JCheckBox check; 
    private JComboBox opList;
    private String xmlNodeName;
    
    final private String[] opData = {"Operators",
				     "=",
				     "<",
				     ">",
				     ">=",
				     "<=",
				     "AND",
				     "OR",
				     "XOR"
    };
    
    private JTextField pred;
    
    private static final int PAD = 10;

    DTDXMLQLTreeNode xmlNode;

    public Component createComponents() {
	final JPanel panel = new JPanel();
	GridBagLayout gb = new GridBagLayout();
	GridBagConstraints c = new GridBagConstraints();
	//int index  = 0;
	//String initPred = "";

	panel.setLayout(gb);
	
	check = new JCheckBox("Project '"+xmlNodeName+"'");
	check.setSelected(xmlNode.isProjected());
	c.gridy = 0;
	c.weightx = 1.0;
	c.insets = new Insets(PAD,PAD,PAD,PAD);
	c.gridwidth = GridBagConstraints.REMAINDER;
	gb.setConstraints(check, c);
	
	panel.add(check);
	
	opList = new JComboBox(opData);
	opList.setSelectedIndex(0);
	c.gridy = 1;
	c.insets = new Insets(0,PAD,PAD,PAD);
	gb.setConstraints(opList, c);
	panel.add(opList);
	
	JLabel label = new JLabel("Predicate");
	c.gridy = 2;
	c.anchor = GridBagConstraints.WEST;
	gb.setConstraints(label, c);
	panel.add(label);

	pred = new JTextField(xmlNode.getPredicate());
	c.gridy = 3;
	c.weighty = 0.80;
	c.fill = GridBagConstraints.BOTH;
	gb.setConstraints(pred, c);
	panel.add(pred);
	
	final JDialog outer = this;

	final JButton ok = new JButton("OK");
	c.gridy = 4;
	c.gridwidth = 1;
	c.weighty = 0.20;
	gb.setConstraints(ok, c);
	panel.add(ok);
	ok.addActionListener(
			     new ActionListener() {
	    public void actionPerformed(ActionEvent e) {
		if ( fixPredicate() )
		    outer.setVisible(false);
	    }
	});
	
	
	final JButton cancel = new JButton("Cancel");
	c.gridx = 1;
	gb.setConstraints(cancel, c);
	panel.add(cancel);
	cancel.addActionListener(
				 new ActionListener() {
	    public void actionPerformed(ActionEvent e) {	
		outer.setVisible(false);
	    }
	});
	
	ok.setPreferredSize(cancel.getPreferredSize());

	return panel;
    }
        
    // Fix the predicate
    private boolean  fixPredicate() {
	String predText = pred.getText();
	int selIndex = opList.getSelectedIndex();

	// set the projected status
	xmlNode.setProjection(check.isSelected());

	if ( predText.trim().equals("") ) return true;

	if ( selIndex == 0 ) {
	    Toolkit.getDefaultToolkit().beep();
	    JOptionPane.showMessageDialog(null,"No Operator Choosen!","Niagara",JOptionPane.ERROR_MESSAGE);
	    return false;
	}
	
	String selOp = (String)(opList.getItemAt(selIndex));
	
	String finalPred = new String();

	finalPred += selOp + " ";

	if(predText.startsWith("\"") && predText.startsWith("\""))
	    finalPred += predText;
	else
	    finalPred += "\""+predText+"\"";
	
	xmlNode.setPredicate(finalPred);

	return true;
    }
    
    // Constructor
    public PredicateUI(JDialog owner, DefaultMutableTreeNode n) {
	super(owner, "Predicate Selector", true);

	xmlNode = (DTDXMLQLTreeNode)(n.getUserObject());

	StringTokenizer xmlNName = new StringTokenizer(xmlNode.toString(),":");
	xmlNodeName = new String(xmlNName.nextToken());

	Component contents = createComponents();
	this.getContentPane().add(contents, BorderLayout.CENTER);
	
	setSize(300,215);
	Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
	Rectangle frameBounds = getBounds();
	
	setLocation((screenSize.width-frameBounds.width)/2,(screenSize.height-frameBounds.width)/2);
	
	setVisible(true);
    }
}
