package niagara.utils.type_system;

/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

/* just a class to hold one instance of each node helper - so we don't
 * have to creat these all over the place - almost all (maybe all) methods
 * on NodeHelpers are static, but we need a way to call them - need
 * to have a reference to an object on which .valueOf or .equals can
 * be called.  If I have Class object referring to the NodeHelper class -
 * I don't think I can easily invoke methods on it - have to wrap up
 * args, etc, I think this will be easier
 */

/* uugh, very bad hack */
import niagara.physical.MTException;

public class NodeHelpers {
    public static final DoubleNodeHelper DOUBLEHELPER
	= new DoubleNodeHelper();
    public static final IntegerNodeHelper INTHELPER
	= new IntegerNodeHelper();
    public static final StringNodeHelper STRINGHELPER 
	= new StringNodeHelper();
    public static final DateNodeHelper DATEHELPER 
	= new DateNodeHelper();

    public static final String INT = "int";
    public static final String DOUBLE = "double";
    public static final String DATE = "date";
    public static final String STR = "string";

    /**
     * Given two value - determines
     * what the two types should be promoted to in order to do a
     * comparison between the two
     *
     * @param lValueType one of the type strings
     * @param rValueType the other type string
     *
     * @return An instance of the appropriate NodeHelper class
     */
    static public NodeHelper getAppropriateNodeHelper(String lValueType,
						      String rValueType)
	throws MTException {

	if(lValueType.equals(NodeHelpers.STR)) {
	    if(rValueType.equals(NodeHelpers.STR)) {
		return NodeHelpers.STRINGHELPER;
	    } else if(rValueType.equals(NodeHelpers.INT)) {
		return NodeHelpers.INTHELPER;
	    } else if (rValueType.equals(NodeHelpers.DOUBLE)) {
		return NodeHelpers.DOUBLEHELPER;
	    } else if (rValueType.equals(NodeHelpers.DATE)) {
		return NodeHelpers.DATEHELPER;
	    } else {
		throw new MTException("Invalid type R: " + rValueType);
	    }
	} else if (lValueType.equals(NodeHelpers.INT)) {
	    if(rValueType.equals(NodeHelpers.DOUBLE)) {
		return NodeHelpers.DOUBLEHELPER;
	    } else if (rValueType.equals(NodeHelpers.DATE)) {
		throw new MTException("Invalid coercion L: " + lValueType +
				      "R: " + rValueType);
	    } else if (rValueType.equals(NodeHelpers.INT) ||
		       rValueType.equals(NodeHelpers.STR)) {
		return NodeHelpers.INTHELPER;
	    } else {
		throw new MTException("Invalid type R: " + rValueType);
	    }
	} else if (lValueType.equals(NodeHelpers.DATE)) {
	    if(rValueType.equals(NodeHelpers.DATE) ||
	       rValueType.equals(NodeHelpers.STR)) {
		return NodeHelpers.DATEHELPER;
	    } else if (rValueType.equals(NodeHelpers.DOUBLE) ||
		       rValueType.equals(NodeHelpers.INT)) {
		throw new MTException("Invalid coercion L: " + lValueType +
				      "R: " + rValueType);
	    } else {
		throw new MTException("Invalid type R: " + rValueType);
	    } 
	} else if (lValueType.equals(NodeHelpers.DOUBLE)) {
	    if(rValueType.equals(NodeHelpers.DOUBLE) ||
	       rValueType.equals(NodeHelpers.INT) ||
	       rValueType.equals(NodeHelpers.STR)) {
		return NodeHelpers.DOUBLEHELPER;
	    } else if (rValueType.equals(NodeHelpers.DATE)) {
		throw new MTException("Invalid coercion L: " + lValueType +
				      "R: " + rValueType);
	    } else {
		throw new MTException("Invalid type. R: " + rValueType);
	    }
	} else {
	    throw new MTException("Invalid type. L: " + lValueType);
	}
    }

    /**
     * Given a value type as a string returns the appropriate node
     * helper
     *
     * @param valueType The string of the value type
     *
     * @return An instance of the appropriate NodeHelper class
     */
    static public NodeHelper getAppropriateNodeHelper(String valueType)
	throws MTException {

	if(valueType.equals(STR)) {
	    return NodeHelpers.STRINGHELPER;
	} else if (valueType.equals(INT)) {
	    return NodeHelpers.INTHELPER;
	} else if (valueType.equals(DATE)) {
	    return NodeHelpers.DATEHELPER;
	} else if (valueType.equals(DOUBLE)) {
	    return NodeHelpers.DOUBLEHELPER;
	} else {
	    throw new MTException("Invalid type");
	}
    }

}
