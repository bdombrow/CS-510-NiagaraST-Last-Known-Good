package niagara.query_engine;

import niagara.utils.*;
import niagara.xmlql_parser.syntax_tree.*;

import org.w3c.dom.*;

import java.util.ArrayList;
import java.util.Vector;

public class Hasher {
    
    AtomicEvaluator[] evaluators;
    ArrayList[] values;

    StreamTupleElement[] oneTuple;

    public Hasher(Vector attributeList) {
        evaluators = new AtomicEvaluator[attributeList.size()];
        values = new ArrayList[attributeList.size()];

        for (int i = 0; i < attributeList.size(); i++) {
            evaluators[i] = new AtomicEvaluator(attributeList.get(i));
            values[i] = new ArrayList(1);
        }

        oneTuple = new StreamTupleElement[1];
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
	//
	StringBuffer hashResult = new StringBuffer();

	// For each attribute, get the atomic value and add that to the
	// hash code
	//
	int numAttributes = evaluators.length;

	for (int att = 0; att < numAttributes; ++att) {

	    // Get the atomic value of attribute
	    //
	    oneTuple[0] = tupleElement;

            values[att].clear();
	    evaluators[att].getAtomicValues(oneTuple, values[att]);

	    // If there is not exactly one atomic value, then it is an error
	    //
	    if (values[att].size() != 1)
		throw new PEException("More than one atomic value in hashCode eval");

	    // Add the atomic value (a string) to the current result
	    //
	    hashResult.append('<');
	    hashResult.append((String) values[att].get(0));
	    hashResult.append('<');
	}

	// Return the hash result
	//
	return hashResult.toString();
    }
}
