package niagara.utils;

/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */


/**
 * A class to hold a boolean value. Need this so I can have a
 * reference to a boolean value and so that I can change the
 * value of the boolean in that class.  Note the class Boolean
 * does not provide the desired functionality.
 *
 * @version 1.0
 *
 * @author Kristin Tufte
 */

public class BooleanHolder {
    boolean value;

    public BooleanHolder(boolean bvalue) {
	value = bvalue;
    }

    public boolean getValue() {
	return value;
    }

    public void setValue(boolean bvalue) {
	value = bvalue;
    }

}

