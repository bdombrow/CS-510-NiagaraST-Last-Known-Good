
/**********************************************************************
  $Id: PredicatePanel.java,v 1.1 2000/05/30 21:03:24 tufte Exp $


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


package niagara.client;

import java.awt.*;
import java.awt.event.*;
import java.awt.Toolkit;
import java.io.*;
import java.util.Vector;
import java.util.*;
import java.net.URL;
import javax.swing.*;
import javax.swing.border.*;
import javax.swing.event.*;
import javax.swing.tree.*;
import niagara.client.dtdTree.*;

public class PredicatePanel extends JDialog 
{
	
	DefaultMutableTreeNode node;
	private JCheckBox check; 
	private JComboBox opList;
	final private String[] opData = {"(OPERATORS)","CONTAINS",
									 "CONTAINED IN",
									 "IS",
									 "<",
									 ">",
									 ">=",
									 "<="
	};
	private JTree subTree;
	private JTextField pred;
	
	DTDSETreeNode seNode;
		
	public Component createComponents()
		{
			final JPanel panel = new JPanel();
			GridBagLayout gb = new GridBagLayout();
			GridBagConstraints c = new GridBagConstraints();

			panel.setLayout(gb);
			
			check = new JCheckBox("Project");
			check.setSelected(seNode.isChecked());
			c.weightx = 1.0;
			c.weighty = 0.0;
			c.gridwidth = GridBagConstraints.REMAINDER;
			gb.setConstraints(check, c);
						
			panel.add(check);

			opList = new JComboBox(opData);
			opList.setSelectedIndex(0);
			c.weightx = 1.0;
			c.weighty = 0.0;
			c.gridwidth = GridBagConstraints.REMAINDER;
			gb.setConstraints(opList, c);
			panel.add(opList);
			
			subTree = new JTree(node);
			JScrollPane treeScrollPane = new JScrollPane(subTree);
			c.weightx = 1.0;
			c.weighty = 0.8;
			c.gridwidth = GridBagConstraints.REMAINDER;
			c.fill = GridBagConstraints.BOTH;
			gb.setConstraints(treeScrollPane, c);
			panel.add(new JPanel().add(treeScrollPane));

			c.fill = GridBagConstraints.HORIZONTAL;
			c.ipadx = 5;
			c.ipady = 5;
			c.insets = new Insets(5,5,5,5);

			JLabel label = new JLabel("Predicate");
			c.weightx = 1.0;
			c.weighty = 0.0;
			c.gridwidth = GridBagConstraints.REMAINDER;
			gb.setConstraints(label, c);
			panel.add(label);

			pred = new JTextField();
			c.weightx = 1.0;
			c.weighty = 0.05;
			c.gridwidth = GridBagConstraints.REMAINDER;
			gb.setConstraints(pred, c);
			panel.add(pred);
			
			final JDialog outer = this;

			final JButton ok = new JButton("OK");
			c.weightx = 1.0;
			c.weighty = 0.05;
			c.gridwidth = 1;
			c.gridheight = 1;
			gb.setConstraints(ok, c);
			panel.add(ok);
			ok.addActionListener(
				new ActionListener()
				{
					public void actionPerformed(ActionEvent e)
						{
							fixPredicate();
							outer.setVisible(false);
						}
				});
			
			
			final JButton cancel = new JButton("Cancel");
			c.weightx = 1.0;
			c.weighty = 0.05;
			c.gridwidth = 1;
			c.gridheight = 1;
			gb.setConstraints(cancel, c);
			panel.add(cancel);
			cancel.addActionListener(
				new ActionListener()
				{
					public void actionPerformed(ActionEvent e)
						{
							outer.setVisible(false);
						}
				});
			
			return panel;
		}
	

	// Fix the predicate
	private void  fixPredicate()
		{
			String predText = pred.getText();
			int selIndex = opList.getSelectedIndex();

			String selOp = (String)(opList.getItemAt(selIndex));
		     
			String finalPred = "";
			
			// get the last selected tree node
			DefaultMutableTreeNode n = (DefaultMutableTreeNode)(subTree.getLastSelectedPathComponent());
			DTDSETreeNode selNode = null;
			if(n != null){
				selNode = (DTDSETreeNode)(n.getUserObject());
			}
			
			// set the checked status
			seNode.setCheckedFlag(check.isSelected());

			/*if(!(selOp.equals(opData[0]))
				&& (!(predText.equals("")))){
				finalPred += selOp + " ";
				}*/

			if(selIndex != 0){
				finalPred += selOp + " ";
			}

			if(!(predText.equals(""))){
				predText.trim();
				if(predText.startsWith("\"") && predText.startsWith("\"")){
					finalPred += predText;
				} else {
					finalPred += "\""+predText+"\"";
				}
			} else if(selNode != null){
				finalPred += selNode.getName();
			}
			
			if(!finalPred.equals("")){
				seNode.setCheckedFlag(true);
			}

			seNode.setPredicate(finalPred);
		}

	// Constructor
	public PredicatePanel(JDialog owner, 
						  DefaultMutableTreeNode n)
		{
			super(owner, "Predicate Selector", true);

			node = n;
			seNode = (DTDSETreeNode)(node.getUserObject());
			
			Component contents = createComponents();
			this.getContentPane().add(contents, BorderLayout.CENTER);
			
			setSize(250,400);
			Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
			Rectangle frameBounds = getBounds();
			
			setLocation((screenSize.width-frameBounds.width)/2,(screenSize.height-frameBounds.width)/2);

			setVisible(true);
		}
}
