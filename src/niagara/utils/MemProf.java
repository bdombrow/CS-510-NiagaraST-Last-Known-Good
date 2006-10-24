package niagara.utils;

public class MemProf {
	
	public MemProf() { };
	
	// this is our way of requesting garbage collection to be run:
    // how aggressive it is depends on the JVM to a large degree, but
    // it is almost always better than a single Runtime.gc() call
    public static void runGC () 
    {
        // for whatever reason it helps to call Runtime.gc()
        // using several method calls:
    	try {
    		for (int r = 0; r < 4; ++ r) _runGC ();
    	} catch (Exception e) {
    		System.err.print("Exception: MemProf.runGC();");
    	}
    }

    private static void _runGC () throws Exception
    {
        long usedMem1 = usedMemory (), usedMem2 = Long.MAX_VALUE;

        for (int i = 0; (usedMem1 < usedMem2) && (i < 1000); ++ i)
        {
            s_runtime.runFinalization ();
            s_runtime.gc ();
            Thread.currentThread ().yield ();
            
            usedMem2 = usedMem1;
            usedMem1 = usedMemory ();
        }
    }

    public static long usedMemory ()
    {
        return s_runtime.totalMemory () - s_runtime.freeMemory ();
    }
    
    private static final Runtime s_runtime = Runtime.getRuntime ();

} // end of class
