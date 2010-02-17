package niagara.xmlql_parser;

import java.util.Hashtable;

/**
 * This class declares different operators in XML-QL
 * 
 */

@SuppressWarnings("unchecked")

public class opType {
     
     public static final int UNDEF = -1;
     
     public static final int STAR = 0;
     public static final int PLUS = 1;
     public static final int QMARK = 2;
     public static final int DOT = 3;
     public static final int BAR = 4;
     public static final int DOLLAR = 5;
     
     public static final int LT = 6;
     public static final int GT = 7;
     public static final int LEQ = 8;
     public static final int GEQ = 9;
     public static final int NEQ = 10;
     public static final int EQ = 11;

     public static final int OR = 14;
     public static final int AND = 15;
     public static final int NOT = 16;

     public static final int XMLQL = 21;

     public static final int CONTAIN = 30;
     public static final int DIRECT_CONTAIN = 31;
     public static final int IS = 32;

    static String[] code2name;
    static Hashtable name2code;

    static {
        code2name = new String[32];
        name2code = new Hashtable();
        
        code2name[LT] = "lt";
        name2code.put("lt", new Integer(LT));

        code2name[GT] = "gt";
        name2code.put("gt", new Integer(GT));

        code2name[LEQ] = "le";
        name2code.put("le", new Integer(LEQ));

        code2name[GEQ] = "ge";
        name2code.put("ge", new Integer(GEQ));

        code2name[NEQ] = "ne";
        name2code.put("ne", new Integer(NEQ));

        code2name[EQ] = "eq";
        name2code.put("eq", new Integer(EQ));

        code2name[OR] = "or";
        name2code.put("or", new Integer(OR));

        code2name[AND] = "and";
        name2code.put("and", new Integer(AND));

        code2name[NOT] = "not";
        name2code.put("not", new Integer(NOT));

	code2name[CONTAIN] = "contain";
	name2code.put("contain", new Integer(CONTAIN));
    }

    public static String getName(int code) {
        return code2name[code];
    }
    
    public static int getCode(String name) {
	Object o = name2code.get(name);
	if(o == null)
	    return UNDEF;
	else
	    return ((Integer) name2code.get(name)).intValue();
    }
};



