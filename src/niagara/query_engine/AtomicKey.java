package niagara.query_engine;

/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

import niagara.xmlql_parser.syntax_tree.*;
import niagara.utils.type_system.*;

/**
 * Implementation <code> AtomicKey </code> class which 
 * represents the smallest unit of a key - that is one
 * path and how to match on that path (i.e. content at the
 * end of that path, tag existence, etc.)
 * A list of AtomicKeys makes a LocalKey.
 */

class AtomicKey {

    int mergeType; /* must be MatchTemplate.TAG_EXISTENCE or .CONTENT */
    regExp path;
    NodeHelper nodeHelper;

    AtomicKey(int _mergeType, regExp _path, NodeHelper _nodeHelper) {
	mergeType = _mergeType;
	path = _path;
	nodeHelper = _nodeHelper;
    }

    regExp path() {
	return path;
    } 

    int mergeType() {
	return mergeType;
    }

    NodeHelper nodeHelper() {
	return nodeHelper;
    }

    public boolean isNever() {
	return path.isNever();
    }
}
