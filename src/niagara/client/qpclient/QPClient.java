package niagara.client.qpclient;

import niagara.client.SimpleClient;

import javax.swing.*;
import javax.swing.filechooser.*;
import java.io.File;

import java.awt.*;
import java.awt.event.*;
import java.awt.geom.*;

import diva.canvas.toolbox.*;
import diva.canvas.*;
import diva.graph.*;
import diva.graph.model.*;

import com.ibm.xml.parsers.*;
import org.w3c.dom.*;
import org.xml.sax.*;
import java.io.*;

import java.util.Iterator;
import htmllayout.*;

public class QPClient extends JFrame implements ActionListener, SimpleClient.ResultsListener {
    QPGraphController qgc;
    QPGraphImpl qgi;
    GraphPane gp;
    ButtonGroup bg;

    QPNode root_node;

    // The file currently being edited (and the default target on Save)
    private File current_file;

    public void load() {
	JFileChooser jfc = new JFileChooser(System.getProperty("user.dir"));
	jfc.setFileFilter(new XQPFilter());
	int retval = jfc.showOpenDialog(this);
	if (retval == JFileChooser.APPROVE_OPTION) {
	    current_file = jfc.getSelectedFile();
	    System.out.println(current_file);
	    loadFile(current_file);
	}
    }

    public void save_as() {
	JFileChooser jfc = new JFileChooser(System.getProperty("user.dir"));
	jfc.setFileFilter(new XQPFilter());
	int retval = jfc.showSaveDialog(this);
	if (retval == JFileChooser.APPROVE_OPTION) {
	    current_file = jfc.getSelectedFile();
	    System.out.println("Saving: " + current_file);
	    saveFile(current_file);
	}
    }

    public void loadFile(File file) {
	try {
	    GraphModel gm = getGraphModel(file);
	    gp.setGraphModel(gm);
	    (new QPLayout()).layout(gp.getGraphView(), gm.getGraph());
	}
	catch (Exception e) {
	    notifyError("An error occured while loading " + file + ":\n" + e.getMessage());
	}
    }

    public void saveFile(File file) {
	QPGraph g = (QPGraph) gp.getGraphModel().getGraph();
	PrintWriter pw = null;
	try {
	    pw = new PrintWriter(new FileWriter(file));
	}
	catch (Exception e) {
	    notifyError("An error occured while saving " + file + ":\n" + e.getMessage());
	}

	root_node.save(pw, gp.getGraphView());
	pw.close();
    }

    public class XQPFilter extends javax.swing.filechooser.FileFilter {
	public boolean accept(File f) {
	    if (f.isDirectory()) 
		return true;

	    String filename = f.getName();
	    int last_dot_idx = filename.lastIndexOf(".");
	    if (last_dot_idx == -1) // No dots
		return false;
	    if (filename.substring(last_dot_idx).equalsIgnoreCase(".xqp"))
		return true;

	    return false;
	}
	public String getDescription() {
	    return "XML Query Plan files  (*.xqp)";
	}
    }

    public GraphModel getGraphModel(File qpfile) 
	throws InvalidQueryPlanException {
	GraphModel gm = new GraphModel(new QPGraphImpl());

	DOMParser parser = new DOMParser();

	try {
	    parser.parse(new InputSource(new FileInputStream(qpfile)));
	}
	catch (SAXException e) {
	    throw new InvalidQueryPlanException("Error parsing query plan:\n " + e.getMessage());
	}
	catch (IOException e) {
	    throw new InvalidQueryPlanException("An error occured while reading file:\n " + qpfile);
	}
	
	Document doc = parser.getDocument();
	Element root = doc.getDocumentElement();

	IDREFResolver idr = new IDREFResolver();

	if (root == null) {
	    notifyError("The root of the query plan is null!");
	}
	QPNode qpn = QPNodeFactory.parse(root, idr, gm);
	if (qpn != null) {
	    root_node = qpn;
	}

	if (!idr.everythingResolved()) {
	    Iterator iter = idr.getUnresolved();
	    while (iter.hasNext()) {
		System.out.println("Unresolved symbol: " + iter.next());
	    }
	    throw new InvalidQueryPlanException("Unresolved variables");
	}

	return gm;
    }

    public void constructMenuBar() {
	JMenuBar jmb = new JMenuBar();

	JMenu file = new JMenu("File");
	file.setMnemonic(KeyEvent.VK_F);

	JMenuItem mi;
	mi = new JMenuItem("New", KeyEvent.VK_N);
	mi.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_N, 
						 ActionEvent.CTRL_MASK));
	mi.addActionListener(new ActionListener() {
		public void actionPerformed(ActionEvent e) {
		    new QPClient("");
		}
	    });
	file.add(mi);

	mi = new JMenuItem("Open...", KeyEvent.VK_O);
	mi.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_O, 
						 ActionEvent.CTRL_MASK));
	mi.addActionListener(new ActionListener() {
		public void actionPerformed(ActionEvent e) {
		    load();
		}});
	file.add(mi);

	mi = new JMenuItem("Save", KeyEvent.VK_S);
	mi.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_S, 
						 ActionEvent.CTRL_MASK));
	mi.addActionListener(new ActionListener() {
		public void actionPerformed(ActionEvent e) {
		    saveFile(current_file);
		}
	    });
	file.add(mi);

	mi = new JMenuItem("Save as...", KeyEvent.VK_A);
	mi.addActionListener(new ActionListener() {
		public void actionPerformed(ActionEvent e) {
		    save_as();
		}
	    });
	file.add(mi);

	mi = new JMenuItem("Exit", KeyEvent.VK_X);
	mi.addActionListener(new ActionListener() {
		public void actionPerformed(ActionEvent e) {
		    System.exit(0);
		}
	    });
	file.add(mi);

	jmb.add(file);

	JMenu plan = new JMenu("Plan");
	plan.setMnemonic(KeyEvent.VK_P);

	mi = new JMenuItem("Run!", KeyEvent.VK_R);
	mi.addActionListener(new ActionListener() {
		public void actionPerformed(ActionEvent e) {
		    run();
		}
	    });
	plan.add(mi);
	jmb.add(plan);

	setJMenuBar(jmb);
    }

    JTextArea jta;
    JFrame results;
    JScrollPane jsp;    
    SimpleClient sc;
    JButton again;
    public void notifyNewResults(String results) {
	jta.append(results + "\n");
    }

    public void notifyError(String error) {
	JOptionPane.showMessageDialog(this, error, "Error", JOptionPane.ERROR_MESSAGE);
    }

    public void run() {
	if (results == null) {
	    jta = new JTextArea(40, 50);
	    results = new JFrame("Results: " + current_file);
	    jsp = new JScrollPane(jta);
	    jsp.setPreferredSize(new Dimension(500, 500));
	    jta.setEditable(false);
	    again = new JButton("Run again!");
	    again.addActionListener(new ActionListener() {
		    public void actionPerformed(ActionEvent e) {
			run();
		    }
		});
	    Container c = results.getContentPane();
	    c.setLayout(new HtmlLayout("<table rows=2 cols=1><tr><td vert=max component=ta><tr><td horz=center component=again>"));
	    c.add("ta", jsp);
	    c.add("again", again);
	}
	jta.setText("");

	StringWriter sw = new StringWriter();
	PrintWriter pw = new PrintWriter(sw);
	root_node.save(pw, gp.getGraphView());

	SimpleClient sc = new SimpleClient();
	sc.addResultsListener(this);
	sc.processQuery(sw.toString());
	results.pack();
	results.move(450, 400);
	results.setVisible(true);
    }
    
    public QPClient(String title) {
	super(title);
	setSize(750, 800);


	//Handle the close window event
	addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e) {
                System.exit(0);
            }
        });

	Container cp = getContentPane();

	constructMenuBar();

	cp.setLayout(new BorderLayout());

	qgi = new QPGraphImpl();
	GraphModel gm = new GraphModel(qgi);
	qgc = new QPGraphController();
	QPJGraph jg = new QPJGraph();
	gp = new GraphPane(gm, qgc);
	gp.getGraphView().setNodeRenderer(new OperatorRenderer());
	jg.setGraphPane(gp);

	JPanel jp = new JPanel();
	MyButton j1 = new MyButton("j1");
	MyButton j2 = new MyButton("j1");
	MyButton j3 = new MyButton("j1");
	MyButton j4 = new MyButton("j1");
	bg = new ButtonGroup();
	bg.add(j1); jp.add(j1);
	bg.add(j2);jp.add(j2);
	bg.add(j3);jp.add(j3);
	bg.add(j4);jp.add(j4);
	j1.addActionListener(this);
	cp.add(jg, BorderLayout.CENTER);

	JToolBar jtb = new JToolBar();
	addButtons(jtb);
       
	//	cp.add(jtb, BorderLayout.NORTH);

	setVisible(true);
    }

    public void addButtons(JToolBar jtb) {
	JButton scan = new JButton(" Scan ");
	jtb.add(scan);
	scan.addActionListener(this);
	JButton dtdscan = new JButton(" DTD Scan ");
	jtb.add(dtdscan);
	JButton fhscan = new JButton(" Firehose Scan ");
	jtb.add(fhscan);
    }

    public void actionPerformed(ActionEvent e) {
	qgc.addNode(qgi.createScan(null));
    }

    public static void main(String args[]) {
	QPClient qpc = new QPClient("QPClient");
	if (args.length > 0) { 
	    qpc.current_file = new File(args[0]);
	    qpc.loadFile(qpc.current_file);
	}
    }


    class OperatorRenderer implements NodeRenderer {
	public Figure render(diva.graph.model.Node n) {
	    return ((QPNode) n).render();
	}
    }
    
    public class MyButton extends JButton {
	public MyButton(String s) {
	    super(s);
	}

	public void processMouseEvent(MouseEvent me) {
	    if (me.getID() == MouseEvent.MOUSE_CLICKED) {
		if (!getModel().isPressed()) {
		    getModel().setArmed(true);
		    getModel().setPressed(true);
		    fireActionPerformed(new ActionEvent(this, 0, null));
		}
		else {
		    getModel().setArmed(false);
		    getModel().setPressed(false);
		}
	    }
	}
    }
    public class InvalidQueryPlanException extends Exception {
	public InvalidQueryPlanException(String msg) {
	    super("Invalid Plan Exception: " + msg + " ");
	}
    }
}


