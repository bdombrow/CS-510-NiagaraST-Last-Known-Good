package niagara.utils;

// java jni wrapper for c function getrusage
public class JProf {
    long time_nsec; // cpu time in nanoseconds since last called
	
    public JProf() { }

    public static native long getCurrentThreadCpuTime();
    public static native int registerThreadName(String name);
    public static native int requestDataDump();
    
    public static void main(String[] args) {
	JProf t = new JProf();
	System.out.println("Initial: " + t.getCurrentThreadCpuTime());
	int count = 0;
	//for(int i = 0; i<1000000000; i++) {
	for(int i = 0; i<1000; i++) {
	    count +=i; 
	}
	System.out.println("count " + count);
	System.out.println("After count loop: " + t.getCurrentThreadCpuTime());
	//for(int i = 0; i<10000000; i++) {
	for(int i = 0; i<1000; i++) {
	    count +=i; 
	}
	System.out.println("count " + count);
	System.out.println("After 2nd count loop: " + t.getCurrentThreadCpuTime());
	    
    }
}
