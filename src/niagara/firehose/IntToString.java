package niagara.firehose;

// avoid int to string conversions which are expensive

class IntToString {
    public static int NUM_VALS = 10000;
    public static String strings[] = new String[NUM_VALS];

    static {
	for(int i = 0; i<NUM_VALS; i++) {
	    strings[i] = String.valueOf(i);
	}
    }
}
