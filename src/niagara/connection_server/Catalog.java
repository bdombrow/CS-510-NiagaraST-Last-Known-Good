/*
 * $Id: Catalog.java,v 1.6 2002/10/23 22:33:59 vpapad Exp $
 *
 */

package niagara.connection_server;
import java.util.*;
import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.w3c.dom.*;
import org.xml.sax.InputSource;

import niagara.logical.NodeDomain;
import niagara.logical.Variable;
import niagara.ndom.*;
import niagara.utils.*;

import niagara.optimizer.colombia.ATTR;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Rule;
import niagara.optimizer.rules.SimpleRule;

public class Catalog implements ICatalog {

    private static final boolean debug = true;
    // This primitive Catalog maintains a mapping from URNs to URLs, 
    // or location of servers that know to resolve the URNs

    Hashtable urn2urls; // locally resolvable
    Hashtable urn2resolvers;

    HashMap parameters; // Columbia cost model parameters

    // Operator name <-> class mapping
    private HashMap name2class;
    private HashMap class2name;

    private HashMap rulesets;

    // Logical operator -> Array of Physical operator classes
    HashMap logical2physical;

    // Resource name -> its logical properties
    HashMap logicalProperties;

    public Catalog() {
        urn2urls = new Hashtable();
        urn2resolvers = new Hashtable();
        parameters = new HashMap();
        name2class = new HashMap();
        class2name = new HashMap();
        rulesets = new HashMap();
        logical2physical = new HashMap();
        logicalProperties = new HashMap();
    }

    public void addResolver(String urn, String resolver) {
        if (!urn2resolvers.containsKey(urn)) {
            urn2resolvers.put(urn, new Vector());
        }
        Vector resolvers = (Vector) urn2resolvers.get(urn);
        resolvers.add(resolver);
    }

    public void addURL(String urn, String url) {
        if (!urn2urls.containsKey(urn)) {
            urn2urls.put(urn, new Vector());
        }
        Vector urls = (Vector) urn2urls.get(urn);
        urls.add(url);
    }

    public boolean isLocallyResolvable(String urn) {
        return urn2urls.containsKey(urn);
    }

    public Vector getURL(String urn) {
        return (Vector) urn2urls.get(urn);
    }

    public Vector getResolvers(String urn) {
        return (Vector) urn2resolvers.get(urn);
    }

    public Hashtable getResolvers(Vector urns) {
        Hashtable resolver2urns = new Hashtable();
        for (int i = 0; i < urns.size(); i++) {
            String urn = (String) urns.get(i);
            Vector resolvers = (Vector) urn2resolvers.get(urn);
            if (resolvers == null)
                continue;
            for (int j = 0; j < resolvers.size(); j++) {
                String resolver = (String) resolvers.get(j);
                if (!resolver2urns.containsKey(resolver)) {
                    resolver2urns.put(resolver, new Vector());
                }
                Vector resolvable = (Vector) resolver2urns.get(resolver);
                resolvable.add(urn);
            }
        }
        return resolver2urns;
    }

    public Catalog(String filename) {
        this();

        // parse the catalog file
        niagara.ndom.DOMParser parser = DOMFactory.newParser();

        InputSource is = null;
        try {
            is = new InputSource(new FileInputStream(filename));
            parser.parse(is);

            // Get document
            Document d = parser.getDocument();

            // Get root element
            Element root = d.getDocumentElement();

            System.err.println("Parsing catalog...");
            loadOperators(root);
            loadResources(root);
            loadParameters(root, "costmodel");
            loadParameters(root, "config");
            loadRules(root);
        } catch (FileNotFoundException e) {
            throw new PEException("Catalog file not found: " + e.getMessage());
        } catch (org.xml.sax.SAXException se) {
            throw new PEException(
                "Error parsing catalog file " + se.getMessage());
        } catch (IOException ioe) {
            throw new PEException(
                "Error reading catalog file " + ioe.getMessage());
        }
    }

    void loadOperators(Element root) {
        NodeList nl = root.getElementsByTagName("operators");
        if (nl.getLength() != 1)
            confError("The catalog must contain exactly one <operators> element");
        Node cm = nl.item(0);

        nl = cm.getChildNodes();

        // Populate the operator hashmaps
        for (int i = 0; i < nl.getLength(); i++) {
            Node n = nl.item(i);
            if (n.getNodeType() != Node.ELEMENT_NODE)
                continue;
            Element e = (Element) n;
            String name = mustHaveAttribute(e, "name");
            String className = mustHaveAttribute(e, "class");
            Class opClass = null;
            try {
                opClass = Class.forName(className);
            } catch (ClassNotFoundException cnfe) {
                confError("Could not find class: " + className);
            }
            name2class.put(name, opClass);
            class2name.put(opClass, name);

            ArrayList al = new ArrayList();
            NodeList physical = e.getChildNodes();
            for (int j = 0; j < physical.getLength(); j++) {
                Node m = physical.item(j);
                if (m.getNodeType() != Node.ELEMENT_NODE)
                    continue;
                Element elt = (Element) m;
                String physName = mustHaveAttribute(elt, "name");
                String physicalClassName = mustHaveAttribute(elt, "class");
                try {
                    Class c = Class.forName(physicalClassName);
                    al.add(c);
                    class2name.put(c, physName);
                    name2class.put(physName, c);
                } catch (ClassNotFoundException exc) {
                    confError("Could not find class: " + physicalClassName);
                }
            }
            Class[] implementations = new Class[al.size()];
            for (int k = 0; k < al.size(); k++)
                implementations[k] = (Class) al.get(k);
            logical2physical.put(className, implementations);
        }
        NiagraServer.info("Loaded " + name2class.size() + " query operators");
    }

    void loadResources(Element root) {
        NodeList resources = root.getElementsByTagName("resource");
        for (int i = 0; i < resources.getLength(); i++) {
            Element resource = (Element) resources.item(i);
            String urn = resource.getAttribute("name");

            NodeList urls = resource.getElementsByTagName("url");
            for (int j = 0; j < urls.getLength(); j++) {
                Element url = (Element) urls.item(j);
                String location = url.getAttribute("location");
                if (!urn2urls.containsKey(urn)) {
                    urn2urls.put(urn, new Vector());
                }
                Vector v = (Vector) urn2urls.get(urn);
                v.add(location);
            }

            NodeList resolvers = resource.getElementsByTagName("resolver");
            for (int j = 0; j < resolvers.getLength(); j++) {
                Element resolver = (Element) resolvers.item(j);
                String location = resolver.getAttribute("location");
                if (!urn2resolvers.containsKey(urn)) {
                    urn2resolvers.put(urn, new Vector());
                }
                Vector v = (Vector) urn2resolvers.get(urn);
                v.add(location);
            }

            LogicalProperty lp = null;
            boolean local = (urls.getLength() > 0);
            lp =
                new LogicalProperty(
                    urls.getLength(),
                    new Attrs(new Variable(urn, NodeDomain.getDOMNode())),
                    local);
            logicalProperties.put(urn, lp);
        }
    }

    public void loadRules(Element root) {
        NodeList rs = root.getElementsByTagName("ruleset");
        if (rs.getLength() < 1) {
            confError("The catalog must contain at least one <ruleset> element");
        }
        for (int j = 0; j < rs.getLength(); j++) {
            Element ruleList = (Element) rs.item(j);
            String name = ruleList.getAttribute("name");
            ArrayList rules = new ArrayList();
            NodeList nl = ruleList.getChildNodes();
            for (int i = 0; i < nl.getLength(); i++) {
                Node n = nl.item(i);
                if (n.getNodeType() != Node.ELEMENT_NODE)
                    continue;
                Element e = (Element) n;
                if (!e.getTagName().equals("rule"))
                    confError("The rules list must contain only <rule> elements");
                String typeName = e.getAttribute("type");
                if (typeName.equals("simple"))
                    rules.add(SimpleRule.fromXML(e, this));
                if (typeName.equals("custom")) {
                    String className = e.getAttribute("class");
                    if (className.length() == 0)
                        confError("Custom rules must specify a class name");
                    try {
                        Class c = Class.forName(className);
                        // Rules must have a zero-argument public constructor
                        Constructor ruleConstructor =
                            c.getConstructor(new Class[] {
                        });
                        Rule rule =
                            (Rule) ruleConstructor.newInstance(new Object[] {
                        });
                        rules.add(rule);
                    } catch (ClassNotFoundException cnfe) {
                        confError("Could not load class: " + className);
                    } catch (NoSuchMethodException nsme) {
                        throw new PEException(
                            "Constructor could not be found for: " + className);
                    } catch (InvocationTargetException ite) {
                        throw new PEException(
                            "Constructor of "
                                + className
                                + " has thrown an exception: "
                                + ite.getTargetException());
                    } catch (IllegalAccessException iae) {
                        throw new PEException(
                            "An illegal access exception occured: " + iae);
                    } catch (InstantiationException ie) {
                        throw new PEException(
                            "An instantiation exception occured: " + ie);
                    }
                }
            }
            rulesets.put(name, rules);
            NiagraServer.info("Loaded " + rules.size() + " optimizer rules for the " + name);
        }
    }

    private static void confError(String msg) {
        throw new ConfigurationError(msg);
    }

    /**
     * Find the logical properties for a resource
     */
    public LogicalProperty getLogProp(String resourceName) {
        if (logicalProperties.containsKey(resourceName))
            return (LogicalProperty) logicalProperties.get(resourceName);
        // If we don't know anything about the resource, 
        // treat URLs as local resources, unknown URNs as remote
        float card;
        boolean local;

        if (resourceName.startsWith("http:")) {
            card = 1;
            local = true;
        } else {
            card = 0;
            local = false;
        }
        return new LogicalProperty(
            card,
            new Attrs(new Variable(resourceName, NodeDomain.getDOMNode())),
            local);
    }

    void loadParameters(Element root, String category) {
        NodeList nl = root.getElementsByTagName(category);
        if (nl.getLength() != 1) {
            throw new PEException(
                "The catalog must contain exactly one <"
                    + category
                    + "> element");
        }
        Node cm = nl.item(0);

        nl = cm.getChildNodes();

        // Populate the cost model HashMap
        for (int i = 0; i < nl.getLength(); i++) {
            Node n = nl.item(i);
            if (n.getNodeType() != Node.ELEMENT_NODE)
                continue;
            Element e = (Element) n;
            parameters.put(e.getTagName(), e.getAttribute("value"));
        }
    }

    public String getOperatorName(Class operatorClass) {
        return (String) class2name.get(operatorClass);
    }

    public Class getOperatorClass(String operatorName) {
        return (Class) name2class.get(operatorName);
    }

    public Class[] getPhysical(String logicalClass) {
        return (Class[]) logical2physical.get(logicalClass);
    }

    public ArrayList getRules(String ruleSetName) {
        return (ArrayList) rulesets.get(ruleSetName);
    }

    public String getCostModelParameter(String param) {
        return (String) parameters.get(param);
    }

    public int getInt(String param) {
        String s = (String) parameters.get(param);
        return Integer.parseInt(s);
    }

    public double getDouble(String param) {
        String s = (String) parameters.get(param);
        return Double.parseDouble(s);
    }

    private String mustHaveAttribute(Element e, String attrName) {
        String value = e.getAttribute(attrName);
        if (value.length() == 0)
            confError("Missing attribute: " + attrName);
        return value;
    }

    public static void cerr(String msg) {
        System.err.println(msg);
    }

    // for testing 
    public void dumpStats() {
        cerr("Locally resolvable URNs:");
        Enumeration e = urn2urls.keys();
        while (e.hasMoreElements()) {
            String urn = (String) e.nextElement();
            cerr("\t" + urn + ":");
            Vector urls = (Vector) urn2urls.get(urn);
            for (int i = 0; i < urls.size(); i++) {
                cerr("\t\t" + urls.get(i));
            }
        }
        Vector allResolvableURNs = new Vector();
        e = urn2resolvers.keys();
        while (e.hasMoreElements()) {
            String urn = (String) e.nextElement();
            allResolvableURNs.add(urn);
        }

        Hashtable resolver2urns = getResolvers(allResolvableURNs);

        cerr("Resolvers for URNs:");
        Enumeration resolvers = resolver2urns.keys();
        while (resolvers.hasMoreElements()) {
            String resolver = (String) resolvers.nextElement();
            cerr(resolver + ": ");
            Vector resolvables = (Vector) resolver2urns.get(resolver);
            for (int i = 0; i < resolvables.size(); i++) {
                cerr("\t" + resolvables.get(i));
            }
        }
    }

    public void dumpCostModel() {
        cerr("Cost model parameters: ");
        Iterator keys = parameters.keySet().iterator();
        while (keys.hasNext()) {
            String k = (String) keys.next();
            cerr(k + " = " + parameters.get(k));
        }
    }
}
