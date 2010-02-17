package niagara.xmlql_parser;

/**
 * This class is used to represent operators in a regular expression with the
 * operands.
 * 
 */

public class regExpOpNode extends regExp {

	private int operator; // type of operator

	private regExp lchild; // left operand (regExp itself)
	private regExp rchild; // right operand if binary operator

	/**
	 * Constructor
	 * 
	 * @param the
	 *            operator like *, +, etc.
	 * @param left
	 *            operand
	 * @param right
	 *            operand
	 */

	public regExpOpNode(int operator, regExp lchild, regExp rchild) {
		this.operator = operator;
		this.lchild = lchild;
		this.rchild = rchild;
	}

	/**
	 * Constructor for unary operator
	 * 
	 * @param the
	 *            operator
	 * @param the
	 *            operand
	 */

	public regExpOpNode(int operator, regExp lchild) {
		this.operator = operator;
		this.lchild = lchild;
		this.rchild = null;
	}

	/**
	 * Constructor without operand
	 * 
	 * @param the
	 *            operator
	 */

	public regExpOpNode(int operator) {
		this.operator = operator;
		lchild = rchild = null;
	}

	/**
	 * @return the left operand or the only operand if unary operator
	 */

	public regExp getLeftChild() {
		return lchild;
	}

	/**
	 * @return the right operand
	 */

	public regExp getRightChild() {
		return rchild;
	}

	/**
	 * @return the operator
	 */

	public int getOperator() {
		return operator;
	}

	/**
	 * print to the screen
	 * 
	 * @param number
	 *            of tabs at the beginning of each line
	 */

	public void dump(int depth) {

		System.out.println();
		genTab(depth);

		switch (operator) {

		case opType.UNDEF:
			System.out.println("Undefined op type");
			break;
		case opType.STAR:
			System.out.println("*");
			break;
		case opType.PLUS:
			System.out.println("+");
			break;
		case opType.QMARK:
			System.out.println("?");
			break;
		case opType.DOT:
			System.out.println(".");
			break;
		case opType.BAR:
			System.out.println("|");
			break;
		case opType.DOLLAR:
			System.out.println("$");
			break;
		case opType.LT:
			System.out.println("<");
			break;
		case opType.GT:
			System.out.println(">");
			break;
		case opType.LEQ:
			System.out.println("<=");
			break;
		case opType.GEQ:
			System.out.println(">=");
			break;
		case opType.NEQ:
			System.out.println("!=");
			break;
		case opType.EQ:
			System.out.println("==");
			break;
		case opType.OR:
			System.out.println("OR");
			break;
		case opType.AND:
			System.out.println("AND");
			break;
		case opType.NOT:
			System.out.println("NOT");
			break;
		case opType.XMLQL:
			System.out.println("XMLQL");
			break;
		default:
			System.out.println("Invalid op type");
			break;
		}

		if (operator == opType.DOLLAR)
			return;

		genTab(depth);
		if (rchild == null)
			System.out.print("[unary] ");
		else
			System.out.print("[left ] ");

		if (lchild instanceof regExpOpNode)
			((regExpOpNode) lchild).dump(depth + 1);
		else
			((regExpDataNode) lchild).dump(depth + 1);

		if (rchild == null)
			return;

		genTab(depth);
		System.out.print("[right] ");
		if (rchild instanceof regExpOpNode)
			((regExpOpNode) rchild).dump(depth + 1);
		else
			((regExpDataNode) rchild).dump(depth + 1);

	}

	// only covers path expressions
	public String toString() {
		switch (operator) {
		case opType.STAR:
			return "(" + lchild.toString() + ")*";
		case opType.PLUS:
			return "(" + lchild.toString() + ")+";
		case opType.QMARK:
			return "(" + lchild.toString() + ")?";
		case opType.DOT:
			return lchild.toString() + "." + rchild.toString();
		case opType.BAR:
			return "(" + lchild.toString() + "|" + rchild.toString() + ")";
		case opType.DOLLAR:
			return "$";
		default:
			return "INVALID";
		}
	}

	public boolean isNever() {
		return false;
	}
}
