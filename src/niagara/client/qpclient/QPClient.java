/**
 * $Id: QPClient.java,v 1.9 2008/10/21 23:11:25 rfernand Exp $
 */
/*
package niagara.client.qpclient;

import niagara.client.ClientException;
import niagara.client.SimpleClient;
import htmllayout.HtmlLayout;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.MouseEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;

import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JToolBar;
import javax.swing.KeyStroke;

import niagara.client.ClientException;
import niagara.client.SimpleClient;
import niagara.ndom.DOMFactory;
import niagara.ndom.DOMParser;

import com.jgraph.JGraph;
import com.jgraph.graph.DefaultGraphModel;
import com.jgraph.graph.ConnectionSet;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import diva.canvas.Figure;
import diva.graph.GraphPane;
import diva.graph.NodeRenderer;
import diva.graph.model.GraphModel;

public class QPClient
    extends JFrame
    implements ActionListener, SimpleClient.ResultsListener {
    QPGraphController qgc;
    QPGraphImpl qgi;
    GraphPane gp;
    ButtonGroup bg;

    QPNode root_node;

    // The file currently being edited (and the default target on Save)
    private File current_file;

    public void load() {
        JFileChooser jfc = new JFileChooser(System.getProperty("user.dir"));
        int retval = jfc.showOpenDialog(this);
        if (retval == JFileChooser.APPROVE_OPTION) {
            loadFile(jfc.getSelectedFile().getName());
        }
    }

    public void save_as() {
        JFileChooser jfc = new JFileChooser(System.getProperty("user.dir"));
        int retval = jfc.showSaveDialog(this);
        if (retval == JFileChooser.APPROVE_OPTION) {
            current_file = jfc.getSelectedFile();
            System.out.println("Saving: " + current_file);
            saveFile(current_file);
        }
    }

    public void loadFile(String filename) {
        current_file = new File(filename);

        try {
            GraphModel gm = getGraphModel(current_file);
            gp.setGraphModel(gm);
            (new QPLayout()).layout(gp.getGraphView(), gm.getGraph());
        } catch (Exception e) {
            notifyError(
                "An error occured while loading "
                    + current_file
                    + ":\n"
                    + e.getMessage());
        }
    }

    public void saveFile(File file) {
        QPGraph g = (QPGraph) gp.getGraphModel().getGraph();
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new FileWriter(file));
        } catch (Exception e) {
            notifyError(
                "An error occured while saving "
                    + file
                    + ":\n"
                    + e.getMessage());
        }

        root_node.save(pw, gp.getGraphView());
        pw.close();
    }

    public GraphModel getGraphModel(File qpfile)
        throws InvalidQueryPlanException {
        GraphModel gm = new GraphModel(new QPGraphImpl());

        DOMParser parser = DOMFactory.newParser();

        try {
            parser.parse(new InputSource(new FileInputStream(qpfile)));
        } catch (SAXException e) {
            throw new InvalidQueryPlanException(
                "Error parsing query plan:\n " + e.getMessage());
        } catch (IOException e) {
            throw new InvalidQueryPlanException(
                "An error occured while reading file:\n " + qpfile);
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

    class QPClientThread extends Thread {
        public void run() {
            new QPClient("QPClient");
        }
    }

    public void constructMenuBar() {
        JMenuBar jmb = new JMenuBar();

        JMenu file = new JMenu("File");
        file.setMnemonic(KeyEvent.VK_F);

        JMenuItem mi;
        mi = new JMenuItem("New", KeyEvent.VK_N);
        mi.setAccelerator(
            KeyStroke.getKeyStroke(KeyEvent.VK_N, ActionEvent.CTRL_MASK));
        mi.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                QPClientThread qpct = new QPClientThread();
                qpct.start();
            }
        });
        file.add(mi);

        mi = new JMenuItem("Open...", KeyEvent.VK_O);
        mi.setAccelerator(
            KeyStroke.getKeyStroke(KeyEvent.VK_O, ActionEvent.CTRL_MASK));
        mi.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                load();
            }
        });
        file.add(mi);

        mi = new JMenuItem("Save", KeyEvent.VK_S);
        mi.setAccelerator(
            KeyStroke.getKeyStroke(KeyEvent.VK_S, ActionEvent.CTRL_MASK));
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
    StringBuffer resultsBuffer = new StringBuffer();

    public void notifyNewResults(String results) {
        resultsBuffer.append(results + "\n");
    }

    public void notifyError(String error) {
        JOptionPane.showMessageDialog(
            this,
            error,
            "Error",
            JOptionPane.ERROR_MESSAGE);
        resultsBuffer.setLength(0);
        again.setEnabled(true);
    }

    public void notifyFinal() {
        jta.setText(resultsBuffer.toString());
        resultsBuffer.setLength(0);
        again.setEnabled(true);
    }

    public void run() {
        try {
        if (results == null) {
            jta = new JTextArea(20, 40);
            results = new JFrame("Results: " + current_file);
            results.setLocation(200, 200);
            jsp = new JScrollPane(jta);
            jsp.setPreferredSize(new Dimension(500, 300));
            jta.setEditable(false);
            again = new JButton("Run again!");
            again.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent e) {
                    run();
                }
            });
            Container c = results.getContentPane();
            c.setLayout(
                new HtmlLayout("<table rows=2 cols=1><tr><td vert=max component=ta><tr><td horz=center component=again>"));
            c.add("ta", jsp);
            c.add("again", again);
        }
        again.setEnabled(false);
        jta.setText("");

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        root_node.save(pw, gp.getGraphView());

        SimpleClient sc = new SimpleClient(server_host);
        sc.addResultsListener(this);
        sc.processQuery(SimpleClient.RequestType.RUN, sw.toString());
        results.pack();
        results.setVisible(true);
	} catch (ClientException ce) {
	    // nothing to do...
	}
    }

    static String server_host = "localhost";

    static int instancesRunning = 0;

    public QPClient(String title) {
        super(title);
        instancesRunning++;

        //Handle the close window event
        addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e) {
                instancesRunning--;
                if (instancesRunning == 0)
                    System.exit(0);
            }
        });

        Container cp = getContentPane();

        constructMenuBar();

        cp.setLayout(new BorderLayout());

        DefaultGraphModel dgm = new DefaultGraphModel();
        dgm.insert(new Object[] {"lala"}, new ConnectionSet(), null, null);
        JGraph graph = new JGraph(dgm);
        
        cp.add(graph, BorderLayout.CENTER);
//        cp.add(jg, BorderLayout.CENTER);

        //  	JPanel jp = new JPanel();
        //  	MyButton j1 = new MyButton("j1");
        //  	bg = new ButtonGroup();
        //  	bg.add(j1); jp.add(j1);
        //  	j1.addActionListener(this);

        //  	JToolBar jtb = new JToolBar();
        //  	addButtons(jtb);

        //	cp.add(jtb, BorderLayout.NORTH);

        graph.setPreferredSize(new Dimension(680, 530));
        pack();
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
        int i = 0;

        if (args.length >= 2 && args[0].equals("-h")) {
            server_host = args[1];
            i = 2;
        }

        if (args.length > i) {
            qpc.loadFile(args[i]);
            qpc.gp.repaint();
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
                } else {
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
*/