package niagara.utils;

/**
 * Definition of class <code> ArrayStack </code>. Just a stack implemented using
 * an array. Wrote this because the ArrayList class from java is too slow. The
 * allocation of a ListIterator in the equals and hashCode functions is too
 * expensive!!
 * 
 * @version 1.0
 * 
 * @author Kristin Tufte
 */

public class ArrayStack {

	/* Contains a list of the key value lists */
	Object[] stack;
	int allocSize; /* number of spots alloced in array */
	int stackSize; /* number of elts in stack */
	public static int count = 0;

	/**
	 * Constructor - does nothing
	 */
	public ArrayStack() {
		allocSize = 10;
		stack = new Object[allocSize];
		stackSize = 0;
	}

	/**
	 * Constructor - does nothing
	 */
	public ArrayStack(int initialSize) {
		allocSize = initialSize;
		stack = new Object[allocSize];
		stackSize = 0;
	}

	public void quickReset() {
		stackSize = 0;
	}

	public void fullReset() {
		for (int i = 0; i < allocSize; i++)
			stack[i] = null;
		stackSize = 0;
	}

	public int hashCode() {
		int hashCode = 1;
		for (int i = 0; i < stackSize; i++) {
			hashCode = 31 * hashCode + stack[i].hashCode();
		}
		return hashCode;
	}

	/**
	 * getStackSize - returns the size of the stack
	 * 
	 * @return the size of the stack
	 */
	public int size() {
		return stackSize;
	}

	/**
	 * push - pushes a string onto the stack
	 * 
	 * @param str
	 *            the string to be appended
	 */
	public void push(Object obj) {
		if (stackSize == allocSize) {
			expandStack();
		}
		stack[stackSize] = obj;
		stackSize++;
	}

	/**
	 * pop - removes a string from the stack
	 * 
	 * @return the popped Object
	 */
	public Object pop() {
		stackSize--;
		Object ret = stack[stackSize];
		stack[stackSize] = null;
		return ret;
	}

	/**
	 * determines if two Stacks are equal
	 * 
	 * @param other
	 *            the Stack to be compared with
	 * @return true if equal, false otherwise
	 */
	public boolean equals(Object obj) {
		ArrayStack other = (ArrayStack) obj;
		if (stackSize != other.stackSize) {
			return false;
		}

		/*
		 * assume here that differences are likely to be near the top of the
		 * stack (this is certainly what I want for the rooted key value stuff)
		 */
		for (int i = stackSize - 1; i >= 0; i--) {
			if (!(stack[i] == null ? other.stack[i] == null : stack[i]
					.equals(other.stack[i]))) {
				return false;
			}
		}
		return true;
	}

	public Object get(int i) {
		return stack[i];
	}

	public Object getAllowExpand(int i) {
		while (i >= allocSize) {
			expandStack();
		}
		return stack[i];
	}

	public Object clone() {
		count++;
		ArrayStack clone = new ArrayStack(allocSize);
		for (int i = 0; i < stackSize; i++) {
			clone.stack[i] = stack[i];
		}
		clone.stackSize = stackSize;
		return clone;
	}

	/**
	 * prints the stack with / separating levels
	 */
	public void print() {

		/* case one - outer stack */
		if (stack[0] instanceof ArrayStack) {
			System.out.print("{");
			for (int i = 0; i < stackSize; i++) {
				System.out.print("/");
				if (stack[i] instanceof ArrayStack) {
					((ArrayStack) stack[i]).print();
				} else {
					System.out.print(stack[i]);
				}
			}
			System.out.println("}");
			return;
		}

		/* case 2 - inner stack */
		System.out.print(stack[0] + "[");
		for (int i = 1; i < stackSize; i++) {
			if (i != 1)
				System.out.print(", ");
			System.out.print(stack[i]);
		}
		System.out.print("]");

		return;
	}

	private void expandStack() {
		Object[] newStack = new Object[allocSize * 2];
		for (int i = 0; i < stackSize; i++) {
			newStack[i] = stack[i];
		}
		stack = newStack;
		allocSize = allocSize * 2;
	}
}
