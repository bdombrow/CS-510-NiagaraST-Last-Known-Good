/* $Id: Antecedent.java,v 1.3 2003/08/01 17:29:06 tufte Exp $ */
package niagara.optimizer.rules;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import niagara.connection_server.Catalog;
import niagara.connection_server.ConfigurationError;
import niagara.optimizer.colombia.LeafOp;
import niagara.optimizer.colombia.Op;
import niagara.utils.PEException;

/** Antecedent patterns for <code>ConstructedRule</code>s.
 * A node in an antecedent pattern is an operator, with an optional 
 * identifying name. */
public class Antecedent extends Pattern {
    /** Inputs to this operator */
    private Antecedent[] inputs;

    private Antecedent(String name, Op operator, Antecedent[] inputs) {
        assert inputs.length < Byte.MAX_VALUE && operator != null;
        this.name = name;
        this.operator = operator;
        this.inputs = inputs;
    }

    /** Parse a pattern from a DOM element */
    public static Antecedent fromXML(Element e, Catalog catalog) {
        ArrayList al = new ArrayList();
        NodeList nl = e.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            Node n = nl.item(i);
            if (n.getNodeType() != Node.ELEMENT_NODE)
                continue;
            al.add(Antecedent.fromXML((Element) n, catalog));
        }

       if (e.getTagName().equals("leaf")) {
            String posStr = e.getAttribute("name");
            if (posStr.length() == 0)
                Catalog.confError("Leaves must have a name attribute");
            int pos = Integer.parseInt(posStr);
            
            return new Antecedent(posStr, new LeafOp(pos), new Antecedent[]{});
       }
                
        // Optional rule name
        String name = e.getAttribute("name");
        if (name.length() == 0)
            name = null;
            
        // Required operator name
        String opName = e.getAttribute("op");
        if (opName.length() == 0)
            Catalog.confError("Must provide an operator name");

        Class opClass = catalog.getOperatorClass(opName);
        Op op = null;
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
            throw new PEException("An instantiation exception occured: " + ie);
        }
        
        Antecedent[] inputs = new Antecedent[al.size()];
        for (int i = 0; i < inputs.length; i++)
            inputs[i] = (Antecedent) al.get(i);
            
        return new Antecedent(name, op, inputs);
    }


    /** An address is a sequence of numbers that identify the position of this 
     * node in the pattern (e.g. [2, 3, 0] is the address of the 0th child
     * of the 4th child of the 3rd child of the root) */
    public void getAddresses(byte[] currentAddress, HashMap addresses) {
        if (name != null) {
            if (addresses.containsKey(name))
                throw new ConfigurationError(
                    "Duplicate variable name in pattern: " + name);

            addresses.put(name, currentAddress);
        }
        
        for (int i = 0; i < inputs.length; i++) {
            byte[] childAddress = new byte[currentAddress.length + 1];
            for (int j = 0; j < currentAddress.length; j++)
                childAddress[j] = currentAddress[j];
            childAddress[currentAddress.length] = (byte) i;
            inputs[i].getAddresses(childAddress, addresses);
        }
    }

    public Pattern[] getInputs() {
        return inputs;
    }
}
