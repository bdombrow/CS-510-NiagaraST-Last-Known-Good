/* $Id$ */
package niagara.query_engine;

import niagara.utils.PEException;
import niagara.xmlql_parser.syntax_tree.opType;

abstract public class ComparisonImpl implements PredicateImpl {
    int operator;

    public ComparisonImpl(int operator) {
        this.operator = operator;
    }
    /**
     *  The equality operator.  Compares different types of objects for equality.
     *
     *  @param leftValue The left value for the equality comparison
     *  @param rightValue The right value for the equality comparison
     *
     *  @return True if the two objects are equal, false otherwise
     */

    boolean stringEquals(String leftValue, String rightValue) {
        return leftValue.equals(rightValue);
    }

    /**
     *  The greater than operator. ">"
     *
     *  @param leftValue The left value for the equality comparison
     *  @param rightValue The right value for the equality comparison
     *
     *  @return True if left object > right object, false otherwise
     */

    boolean stringGreaterThan(String leftValue, String rightValue) {
        return (leftValue.compareTo(rightValue) > 0);
    }

    /**
     *  The less than operator. "<"
     *
     *  @param leftValue The left value for the equality comparison
     *  @param rightValue The right value for the equality comparison
     *
     *  @return True if left object < right object, false otherwise
     */

    boolean stringLessThan(String leftValue, String rightValue) {
        return (leftValue.compareTo(rightValue) < 0);
    }

    /**
     *  The greater than or equal operator. ">="
     *
     *  @param leftValue The left value for the equality comparison
     *  @param rightValue The right value for the equality comparison
     *
     *  @return True if left object >= right object, false otherwise
     */

    boolean stringGreaterThanEquals(String leftValue, String rightValue) {

        // Either greater than or equal
        return (
            stringEquals(leftValue, rightValue)
                || stringGreaterThan(leftValue, rightValue));
    }

    /**
     *  The less than or equal operator. "<="
     *
     *  @param leftValue The left value for the equality comparison
     *  @param rightValue The right value for the equality comparison
     *
     *  @return True if left object <= right object, false otherwise
     */

    boolean stringLessThanEquals(String leftValue, String rightValue) {
        // Either less than or equal
        return (
            stringEquals(leftValue, rightValue)
                || stringLessThan(leftValue, rightValue));
    }

    /**
      *  The Contains operator. "Contains"
      *
      *  @param leftValue The containing string
      *  @param rightValue The string to find in the leftValue
      *
      *  @return True if right is contained in the left, false otherwise
      */
    boolean stringContains(String leftValue, String rightValue) {
        return (leftValue.indexOf(rightValue) != -1);
    }

    /**
     *  The numeric compare function is used to attempt to compare two strings
     *  as numeric double values
     *
     *  @param leftVal The left string for the comparison
     *  @param rightVal The right string for the comparison
     *  @param opCode The comparison operator to use
     *
     *  @return boolean value, true if comparison succeeds
     */

    private boolean numericCompare(String leftVal, String rightVal)
        throws java.lang.NumberFormatException {

        double rightDouble = -1;
        double leftDouble = -1;

        // Convert to doubles if possible, if this fails, an
        // exception will be thrown
        rightDouble = new Double(rightVal).doubleValue();
        leftDouble = new Double(leftVal).doubleValue();

        switch (operator) {
            case opType.EQ :
                return (leftDouble == rightDouble);
            case opType.NEQ :
                return (leftDouble != rightDouble);
            case opType.GT :
                return (leftDouble > rightDouble);
            case opType.LT :
                return (leftDouble < rightDouble);
            case opType.LEQ :
                return (leftDouble <= rightDouble);
            case opType.GEQ :
                return (leftDouble >= rightDouble);
                //Ugly hack for containment
            case opType.CONTAIN :
                throw new java.lang.NumberFormatException();
            default :
                throw new PEException("Unknown operator for numeric comparison");
        }
    }

    /**
     *  The string compare function is used to attempt to compare two strings
     *
     *  @param leftVal The left string for the comparison
     *  @param rightVal The right string for the comparison
     *  @param opCode The comparison operator to use
     *
     *  @return boolean value, true if comparison succeeds
     */

    private boolean stringCompare(String leftValue, String rightValue) {

        /**
         * This function compares two atomic values. KT - atomic values
         * are always strings.
         *
         * @param leftValue The left value in the comparison
         * @param rightValue The right value in the comparison
         * @param oper The comparison operator
         *
         * @return True if the comparison succeeds and false otherwise
         */
        switch (operator) {
            case opType.NEQ :
                return !stringEquals(leftValue, rightValue);

            case opType.EQ :
                return stringEquals(leftValue, rightValue);

            case opType.GT :
                return stringGreaterThan(leftValue, rightValue);

            case opType.LT :
                return stringLessThan(leftValue, rightValue);

            case opType.LEQ :
                return stringLessThanEquals(leftValue, rightValue);

            case opType.GEQ :
                return stringGreaterThanEquals(leftValue, rightValue);

            case opType.CONTAIN :
                return stringContains(leftValue, rightValue);

            default :
                throw new PEException("ERROR: invalid opType for arithOpNode");
        }
    }

    //This is a simple test to determine if the string can be converted
    // into a numeric value. It only handles negative numbers and decimal
    // numbers. It does not check for exponents ('e').
    private boolean isNumber(String st) {
        boolean fNumber=true;
        int cPeriod=0;

        for (int i=0; i < st.length() && fNumber; i++) {
            if (Character.isDigit(st.charAt(i)) == false) {
                if (st.charAt(i) == '.') {
                    //Allow exactly one decimal point
                    cPeriod++;
                    if (cPeriod > 1)
                        fNumber=false;
                } else if (st.charAt(i) == '-') {
                    if (i != 0)
                        //negative is only allowed in the first position
                        fNumber = false;
                } else
                    fNumber = false;
            }
        }

        return fNumber;
    }

    protected boolean compareAtomicValues(String leftValue, String rightValue) {
        // Check to see whether values exist
        if (leftValue == null || rightValue == null)
            throw new PEException("A null value passed for Comparison");

        //See if there is a non-numeric character in either string
        boolean fNumber = isNumber(leftValue) && isNumber(rightValue);

        //If every value was a number, go ahead and try to convert to
        // a double and do a numeric comparison. If that fails, or if there
        // are non-numeric characters in the string, do a string
        // comparison.
        if (fNumber) {
            try {
                return numericCompare(leftValue, rightValue);
            } catch (java.lang.NumberFormatException e) {
                return stringCompare(leftValue, rightValue);
            }
        } else
            return stringCompare(leftValue, rightValue);
    }
}
