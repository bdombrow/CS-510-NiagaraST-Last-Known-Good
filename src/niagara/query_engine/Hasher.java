/* $Id: Hasher.java,v 1.8 2003/03/03 08:20:13 tufte Exp $ */
package niagara.query_engine;

import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.utils.*;

import java.util.ArrayList;
import java.util.Vector;
import java.util.StringTokenizer;

public class Hasher {
    AtomicEvaluator[] evaluators;
    ArrayList[] values;

    public Hasher(Attrs attrs) {
        evaluators = new AtomicEvaluator[attrs.size()];
        values = new ArrayList[attrs.size()];
        for (int i = 0; i < attrs.size(); i++) {
            evaluators[i] = new AtomicEvaluator(attrs.get(i).getName());
            values[i] = new ArrayList(1);
        }
    }
    
    public Hasher(Vector attributeList) {
        evaluators = new AtomicEvaluator[attributeList.size()];
        values = new ArrayList[attributeList.size()];

        for (int i = 0; i < attributeList.size(); i++) {
            evaluators[i] = new AtomicEvaluator(((Attribute) attributeList.get(i)).getName());
            values[i] = new ArrayList(1);
        }
    }

    /**
     * This function generates a hash key for a tuple given the list of
     * relevant attributes. This is NOT a hash code - it is a key to be
     * input into a hash table. equality of keys => equality of elements
     * this is not true for hash codes. KT - this hashing stuff is hard
     * to understand. This key created isn't really a key, but I don't
     * think that matters because of the way the hash tables are
     * structured - java's native hash table isn't used directly.
     *
     * @param tupleElement The tuple to be hashed on
     * @param attributeList The list of attributes in the tuple to hash on
     *
     * @return The hash key. If any of the attributes in the attribute list
     *         do not have a hashable value, then returns null.
     */
    public String hashKey (StreamTupleElement tupleElement) {
	// Create storage for the result
	StringBuffer hashResult = new StringBuffer();

	// For each attribute, get the atomic value and add that to the
	// hash code
	int numAttributes = evaluators.length;

	for (int att = 0; att < numAttributes; ++att) {
	    evaluators[att].getAtomicValues(tupleElement, null, values[att]);

	    // If there is not exactly one atomic value, then it is an error
	    if (values[att].size() != 1) {
		throw new PEException("More than one atomic value in hashCode eval " + values[att].size());
	    }

	    // Add the atomic value (a string) to the current result
	    hashResult.append('<');
	    hashResult.append((String) values[att].get(0));
	    hashResult.append('<');

            values[att].clear();
	}

	// Return the hash result
	return hashResult.toString();
    }

    public void getValuesFromKey(String hashKey, String[] rgstRet) {
	StringTokenizer stok = new StringTokenizer(hashKey, "<");
	int i=0;

	while (stok.hasMoreTokens()) {
	    rgstRet[i] = stok.nextToken();
	    i++;
	}
    }
    
    public void resolveVariables(TupleSchema ts) {
        for (int i = 0; i < evaluators.length; i++) {
            // XXX vpapad: setting stream id to 0
            // Do we ever need a hasher on attributes from multiple 
            // streams?
            evaluators[i].resolveVariables(ts, 0);           
        }
    }
}


