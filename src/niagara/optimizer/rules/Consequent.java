/* $Id: Consequent.java,v 1.1 2002/12/10 01:18:27 vpapad Exp $ */
package niagara.optimizer.rules;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import niagara.connection_server.Catalog;
import niagara.connection_server.ConfigurationError;
import niagara.optimizer.colombia.Expr;
import niagara.optimizer.colombia.LeafOp;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.Op;
import niagara.optimizer.colombia.PhysicalOp;
import niagara.utils.PEException;

/** Consequent patterns for <code>ConstructedRule</code>s.
 * A node in a consequent pattern can be a simple
 * copy of an operator in the antecedent, or a new operator,
 * optionally initialized with one or more operators from the antecedent. */
public class Consequent extends Pattern {
    /** Additional names of antecedent operators to initialize from. */
    private String[] initializers;
    /** Inputs to this operator */
    private Consequent[] inputs;
    /** A sequence of numbers that identify the position of <code>name</code>
     * in the antecedent pattern. */
    private byte[] address;
    /** Addresses of the initializers */
    private byte[][] extraAddresses;

    private Consequent(
        String name,
        String[] initializers,
        Op operator,
        Consequent[] inputs) {
        assert inputs.length < Byte.MAX_VALUE
            && ((name != null) ^  (operator != null));
        this.name = name;
        this.initializers = initializers;
        this.operator = operator;
        this.inputs = inputs;
    }

    /** Parse a pattern from a DOM element */
    public static Consequent fromXML(Element e, Catalog catalog) {
        ArrayList al = new ArrayList();
        NodeList nl = e.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            Node n = nl.item(i);
            if (n.getNodeType() != Node.ELEMENT_NODE)
                continue;
            al.add(Consequent.fromXML((Element) n, catalog));
        }

       if (e.getTagName().equals("leaf")) {
            String posStr = e.getAttribute("name");
            if (posStr.length() == 0)
                catalog.confError("Leaves must be annotated with a name attribute");
            
            return new Consequent(posStr, new String[]{}, null, new Consequent[]{});
       }

        String name = e.getAttribute("name");
        if (name.length() == 0)
            name = null;
            
        String opName = e.getAttribute("op");
        if (!((name != null) ^ (opName.length() != 0)))
            catalog.confError("Must provide either a pattern or an operator name (but not both)");

        Op op = null;
        
        if (opName.length() != 0) {
            Class opClass = catalog.getOperatorClass(opName);
            if (opClass == null)
                catalog.confError("Cannot find operator " + opName);
                
            try {
                // Operator must have a zero-argument public constructor
                Constructor constructor = opClass.getConstructor(new Class[] {
                });

                op = (Op) constructor.newInstance(new Object[] {
                });
            } catch (NoSuchMethodException nsme) {
                throw new PEException(
                    "Constructor could not be found for: " + opName);
            } catch (InvocationTargetException ite) {
                throw new PEException(
                    "Constructor of "
                        + opClass
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

        String strInitializers = e.getAttribute("init");
        StringTokenizer st = new StringTokenizer(strInitializers, ",");
        String[] initializers = new String[st.countTokens()];
        for (int i = 0; i < initializers.length; i++)
            initializers[i] = st.nextToken();

        Consequent[] inputs = new Consequent[al.size()];
        for (int i = 0; i < inputs.length; i++)
            inputs[i] = (Consequent) al.get(i);

        return new Consequent(name, initializers, op, inputs);
    }

    public Expr constructSubstitute(Expr before) {
        Expr[] inpExprs = new Expr[inputs.length];
        for (int i = 0; i < inputs.length; i++)
            inpExprs[i] = inputs[i].constructSubstitute(before);

        Op newOp;
        // Create a new operator from scratch
        if (operator != null)
            newOp = operator.copy();
        // Copy operator from "before" pattern
        else // name != null 
            newOp = followAddress(address, before).copy();

        for (int i = 0; i < extraAddresses.length; i++)
            ((Initializable) newOp).initFrom(
                (LogicalOp) followAddress(extraAddresses[i],
                before));

        return new Expr(newOp, inpExprs);
    }

    public void resolveAddresses(HashMap addresses) {
        if (name != null) {
            address = (byte[]) addresses.get(name);
            if (address == null)
                throw new ConfigurationError(
                    "Could not resolve pattern reference to: " + name);
        }
        
        extraAddresses = new byte[initializers.length][];
        for (int i = 0; i < initializers.length; i++) {
            extraAddresses[i] = (byte[]) addresses.get(initializers[i]);
            if (extraAddresses[i] == null)
                throw new ConfigurationError(
                    "Could not resolve pattern reference to: "
                        + initializers[i]);
        }

        for (int i = 0; i < inputs.length; i++)
            inputs[i].resolveAddresses(addresses);
    }

    public Pattern[] getInputs() {
        return inputs;
    }
}
