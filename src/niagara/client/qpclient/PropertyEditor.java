package niagara.client.qpclient;

import java.awt.event.*;

import java.util.*;
import java.awt.*;
import javax.swing.*;
import javax.swing.text.JTextComponent;
import htmllayout.*;

public class PropertyEditor extends JDialog {
    HashMap name2tf = new HashMap();

    QPNode qpn;
    public PropertyEditor(JFrame parent, String title, QPNode _qpn) {
	super(parent, title);
	qpn = _qpn;
	setLocation(400, 400);
	Container cp = getContentPane();
	int size = qpn.getNumProperties() + 1; // +1 for save/cancel buttons
	String layoutstr = "<table rows=" + size + " cols=2>";
	Iterator iter = qpn.propertyKeys();
	while (iter.hasNext()) {
	    String name = (String) iter.next();
	    layoutstr += "<tr><td>" + name + "<td component=" + name + ">";
	}
	layoutstr += "<tr><td horz=max colspan=2><table rows=1 cols=2><tr><td horz=center component=save><td horz=center component=cancel></table></table>";
	cp.setLayout(new HtmlLayout(layoutstr));
	iter = qpn.propertyKeys();
	while (iter.hasNext()) {
	    String name = (String) iter.next();
	    String value = qpn.getProperty(name).toString();
	    JTextComponent jt;
	    if (value.indexOf("\n") != -1) {
		jt = new JTextArea(value);
	    }
	    else {
		jt = new JTextField(value, Math.min(40, Math.max(value.length(), 20)));
	    }
	    name2tf.put(name, jt);
	    cp.add(name, jt);
	}

	JButton save = new JButton("Save");
	save.addActionListener(new ActionListener() {
		public void actionPerformed(ActionEvent e) {
		    Iterator it = qpn.propertyKeys();
		    while (it.hasNext()) {
			String pname = (String) it.next();
			qpn.setProperty(pname, 
					((JTextComponent) name2tf.get(pname)).
					getText());
		    }
		    hide();
		}
	    });
	JButton cancel = new JButton("Cancel");
	cancel.addActionListener(new ActionListener() {
		public void actionPerformed(ActionEvent e) {
		    hide();
		}
	    });

	cp.add("save", save);
	cp.add("cancel", cancel);
	pack();
	setVisible(true);
    }
}