
/**********************************************************************
  $Id: PredicateGUI.java,v 1.2 2003/07/08 02:10:01 tufte Exp $


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

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Rectangle;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.border.Border;
import javax.swing.tree.DefaultMutableTreeNode;

import niagara.client.dtdTree.DTDXMLQLTreeNode;

public class PredicateGUI extends JDialog implements ActionListener {

    JPanel pPanel;
    JPanel vPanel;

    JPanel predPanel;

    JLabel predicate;
    JLabel element;

    JButton okButton;
    JButton cancelButton;
    JButton addButton;
    JButton deleteButton;

    JScrollPane predScroll;

    GridBagLayout predLayout;
    GridBagConstraints predC;

    private JCheckBox check; 

    private Vector values;
    
    private static final int PAD = 10;
    private static final int PPAD = 5;
    
    private String xmlNodeName;
    private DTDXMLQLTreeNode xmlNode;

    public PredicateGUI(JDialog owner, DefaultMutableTreeNode n) {
	super(owner, "Predicate Selector", true);

	xmlNode = (DTDXMLQLTreeNode)(n.getUserObject());

	xmlNodeName = new String(xmlNode.getName());

	values = new Vector();

	if ( xmlNode.hasPredicate() ) {
	    getPredicates();
	}

	Component contents = createComponents();
	this.getContentPane().add(contents, BorderLayout.CENTER);
	
	setSize(550,400);
	Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
	Rectangle frameBounds = getBounds();
	
	setLocation((screenSize.width-frameBounds.width)/2,(screenSize.height-frameBounds.width)/2);
	
	setVisible(true);
    }

    private void getPredicates() {
	String pred = xmlNode.getPredicate();

	StringTokenizer tokens = new StringTokenizer(pred);

	while ( tokens.hasMoreTokens() ) {
	    String token = tokens.nextToken();

	    if ( token.equals("AND") || token.equals("OR") ) {
		tokens.nextToken();
		values.addElement(new PredicatePanel(false,token,tokens.nextToken(), tokens.nextToken()));
	    }
	    else {
		values.addElement(new PredicatePanel(true,null,tokens.nextToken(),tokens.nextToken()));
	    }
	}
    }

    public void exitPUI() {
	this.setVisible(false);
    }
    
    // Method to create a button
    private JButton makeButton(String buttonText, int keyBoardMnemonic, String toolTipText) {
        JButton button = new JButton(buttonText);
	button.setMnemonic(keyBoardMnemonic);
	button.setToolTipText(toolTipText);
	button.addActionListener(this);
	return button;
    }    

     public Component createComponents() {
	 final JPanel panel = new JPanel();
	 GridBagLayout gb = new GridBagLayout();
	 GridBagConstraints c = new GridBagConstraints();

	 panel.setLayout(gb);

	 check = new JCheckBox("Project");
	 check.setSelected(xmlNode.isProjected());
	 c.gridy = 0;
	 c.weightx = 1.0;
	 c.weighty = 0.0;
	 c.insets = new Insets(PAD,PAD,PAD,PAD);
	 c.gridwidth = GridBagConstraints.REMAINDER;
	 gb.setConstraints(check, c);
	 panel.add(check);
	 
	 element = new JLabel("Element: '"+xmlNode.getName()+"'");
	 c.gridy = 1;
	 gb.setConstraints(element, c);
	 panel.add(element);

	 GridBagLayout pgb = new GridBagLayout();
	 GridBagConstraints pc = new GridBagConstraints();
	 
	 pPanel = new JPanel(pgb);
	 Border border = BorderFactory.createCompoundBorder(BorderFactory.createTitledBorder("Predicates"), BorderFactory.createLoweredBevelBorder());
	 pPanel.setBorder(border);

	 predLayout = new GridBagLayout();
	 predC = new GridBagConstraints();
	 predPanel = new JPanel(predLayout);
	 predScroll = new JScrollPane(predPanel);

	 predC.gridx = 0;
	 predC.gridy = GridBagConstraints.RELATIVE;
	 predC.weightx = 1.0;
	 predC.weighty = 1.0;
	 predC.insets = new Insets(PPAD,PPAD,PPAD,PPAD);
	 predC.anchor = GridBagConstraints.NORTH;
	 predC.fill = GridBagConstraints.HORIZONTAL;
	 
	 addButton = makeButton("Add",KeyEvent.VK_A,"Add New Predicate");
	 deleteButton = makeButton("Delete",KeyEvent.VK_D,"Delete Last Predicate");

	 if ( values.size() == 0 ) {
	     PredicatePanel pred = new PredicatePanel(true);
	     predLayout.setConstraints(pred,predC);
	     predPanel.add(pred);
	     values.addElement(pred);
	 }
	 else 
	     initPredicates();

	 pc.gridx = 0;
	 pc.gridy = 0;
	 pc.weightx = 1.0;
	 pc.weighty = 1.0;
	 pc.gridwidth = GridBagConstraints.REMAINDER;
	 pc.insets = new Insets(PAD,PAD,PAD,PAD);
	 pc.fill = GridBagConstraints.BOTH;
	 pgb.setConstraints(predScroll,pc);
	 pPanel.add(predScroll);

	 pc.gridx = 0;
	 pc.gridy = 1;
	 pc.weightx = 0.0;
	 pc.weighty = 0.0;
	 pc.gridwidth = 1;
	 pc.anchor = GridBagConstraints.WEST;
	 pgb.setConstraints(addButton,pc);
	 pPanel.add(addButton);

	 pc.gridx = 1;
	 pc.gridy = 1;
	 pc.anchor = GridBagConstraints.EAST;
	 pc.fill = GridBagConstraints.NONE;
	 pgb.setConstraints(deleteButton,pc);
	 pPanel.add(deleteButton);

	 addButton.setPreferredSize(deleteButton.getPreferredSize());
	 
	 c.gridy = 2;
	 c.weighty = 1.0;
	 c.fill = GridBagConstraints.BOTH;
	 gb.setConstraints(pPanel, c);
	 panel.add(pPanel);

	 okButton = makeButton("OK",KeyEvent.VK_O,"OK");
	 c.gridx = 0;
	 c.gridy = 3;
	 c.weightx = 0.5;
	 c.weighty = 0.0;
	 c.gridwidth =  1;
	 gb.setConstraints(okButton, c);
	 panel.add(okButton);

	 cancelButton = makeButton("Cancel",KeyEvent.VK_C,"Cancel");
	 c.gridx = 1;
	 c.gridy = 3;
	 gb.setConstraints(cancelButton, c);
	 panel.add(cancelButton);

	 okButton.setPreferredSize(cancelButton.getPreferredSize());

	 return panel;
     }

    private boolean constructPredicate() {
	StringBuffer predicate = new StringBuffer();

	if ( values.size() == 0 ) return true;

	// Check if input correctness
	for ( int i = 0 ; i < values.size() ; i++ ) {
	    PredicatePanel panel = (PredicatePanel)values.elementAt(i);
	    
	    if ( panel.getOperatorIndex() == 0 ) { 
		Toolkit.getDefaultToolkit().beep();
		JOptionPane.showMessageDialog(null,"Predicate "+(i+1)+": No Operator Choosen!","Niagara",JOptionPane.ERROR_MESSAGE);
		return false;
	    }
	  
	    if ( i > 0 ) {
		if ( panel.getLogicalIndex() == 0 ) {
		    Toolkit.getDefaultToolkit().beep();
		    JOptionPane.showMessageDialog(null,"Predicate "+(i+1)+": No Logical Operator Choosen!","Niagara",JOptionPane.ERROR_MESSAGE);
		    return false;
		}
	    }
	}

	// Construct predicate
	for ( int i = 0 ; i < values.size() ; i++ ) {
	    PredicatePanel panel = (PredicatePanel)values.elementAt(i);
	    
	    String predValue = panel.getValue();
	    if ( i >  0 )
		predicate.append(" "+panel.getLogicalOp()+" ");

		predicate.append(xmlNode.getVariableName()+" ");
		predicate.append(panel.getSelectedOp()+" ");

		if ( predValue.startsWith("\"") && predValue.endsWith("\"") )
		    predicate.append(panel.getValue());
		else
		    predicate.append("\""+panel.getValue()+"\"");
	}

	xmlNode.setPredicate(predicate.toString());

	xmlNode.setProjection(check.isSelected());

	return true;
	
    }

    private void initPredicates() {
	for ( int i = 0 ; i < values.size() ; i++ ) {
	    PredicatePanel panel = (PredicatePanel)values.elementAt(i);
	    
	    if ( i == 0 ) {
		predC.insets = new Insets(PPAD,PPAD,PPAD,PPAD);
		deleteButton.setEnabled(true);
	    }
	    else
		predC.insets = new Insets(0,PPAD,PPAD,PPAD);

	    predLayout.setConstraints(panel,predC);
	    predPanel.add(panel);
	    
	    predPanel.revalidate();
	}
    }
    
    public void actionPerformed(ActionEvent e) {
	String command = e.getActionCommand();
	
	if ( command.equals(okButton.getText()) ) {
	    if ( constructPredicate() )
		exitPUI();
	}

	if ( command.equals(cancelButton.getText()) ) {
	    exitPUI();
	}

	if ( command.equals(addButton.getText()) ) {
	    PredicatePanel pred;

	    if ( values.size() == 0 ) {
		pred = new PredicatePanel(true);
		predC.insets = new Insets(PPAD,PPAD,PPAD,PPAD);
		deleteButton.setEnabled(true);
	    }
	    else {
		pred = new PredicatePanel(false);
		predC.insets = new Insets(0,PPAD,PPAD,PPAD);
	    }

	    predLayout.setConstraints(pred,predC);
	    predPanel.add(pred);

	    predPanel.revalidate();

	    values.addElement(pred);
	}
	
	if ( command.equals(deleteButton.getText()) ) {
	    if ( values.size() == 0 ) {
		deleteButton.setEnabled(false);
		return;
	    }

	    PredicatePanel panel = (PredicatePanel)values.lastElement();
	    predPanel.remove(panel);
	    predPanel.repaint();

	    values.remove(panel);

	    if ( values.size() == 0 )
		deleteButton.setEnabled(false);

	    predPanel.revalidate();
	}
	
    }

    class PredicatePanel extends JPanel {
	private final String logicalOpData[] = { "Logical", 
				   "AND",
				   "OR"
	};

	private final String opData[] = { "OPERATORS",
			    "=",
			    "<",
			    ">",
			    "<=",
			    ">="
	};

	private JComboBox logicalOp;
	private JComboBox op;
	private JTextField value;

	PredicatePanel(boolean isFirst) {
	    this.setLayout(new GridLayout());

	    logicalOp = new JComboBox(logicalOpData);
	    logicalOp.setEditable(false);

	    if ( isFirst ) 
		logicalOp.setEnabled(false);
	    else
		logicalOp.setEnabled(true);

	    logicalOp.setSelectedIndex(0);

	    op = new JComboBox(opData);
	    op.setSelectedIndex(0);
	    op.setEditable(false);

	    value = new JTextField();
	    
	    this.add(logicalOp);
	    this.add(op);
	    this.add(value);
	}

	PredicatePanel(boolean isFirst, String log, String oper, String pred) {
	    this(isFirst);

	    if ( !isFirst )
		logicalOp.setSelectedItem(log);

	    op.setSelectedItem(oper);
	    value.setText(pred.substring(1,pred.length()-1));
	}

	public String getLogicalOp() {
	    return (String)logicalOp.getSelectedItem();
	}

	public String getSelectedOp() {
	    return (String)op.getSelectedItem();
	}

	public int getOperatorIndex() {
	    return op.getSelectedIndex();
	}

	public int getLogicalIndex() {
	    return logicalOp.getSelectedIndex();
	}

	public String getValue() {
	    return value.getText();
	}
    }

}
