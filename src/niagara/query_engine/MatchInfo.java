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
 * Implementation of a <code> MatchInfo </code> class which 
 * represents one path's worth of matching information for
 * an element - a list of MatchInfo objects makes up the
 * matching information for an element
 */

class MatchInfo {

    int mergeType; /* must be MatchTemplate.TAG_EXISTENCE or .CONTENT */
    regExp path;
    NodeHelper nodeHelper;

    MatchInfo(int _mergeType, regExp _path, NodeHelper _nodeHelper) {
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
}
