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

public class NodeHelpers {
    public static final DoubleNodeHelper DOUBLEHELPER
	= new DoubleNodeHelper();
    public static final IntegerNodeHelper INTEGERHELPER
	= new IntegerNodeHelper();
    public static final StringNodeHelper STRINGHELPER 
	= new StringNodeHelper();
    public static final DateNodeHelper DATEHELPER 
	= new DateNodeHelper();
}
