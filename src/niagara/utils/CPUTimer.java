package niagara.utils;

public class CPUTimer {

    // differences
    long start_time;
    long stop_time;
    long used_time;

    private JProf jprof;
    private boolean timerRunning = false;

    public CPUTimer() {
	jprof = new JProf();
    }
    
    public void start() {
	if(timerRunning)
	    throw new PEException("Attempt to start timer when it is already running");
	start_time = jprof.getCurrentThreadCpuTime();
	timerRunning = true;
    }

    public void stop() {
	if(!timerRunning)
	    throw new PEException("Attempt to stop timer that is not running");
	stop_time = jprof.getCurrentThreadCpuTime();
	timerRunning = false;
	calculateTimeUsed();
    }

    public long getTimeNSec() {
	if(timerRunning)
	    throw new PEException("Can't get time while timer is running");
	return used_time;
    }

    private void calculateTimeUsed() {
	used_time = stop_time - start_time;
    }

    public void print(String msg) {
	if(timerRunning)
	    throw new PEException("Can't print while timer is running");
	System.out.println(msg);
	System.out.println("Time (nsec) " + used_time);
    }

    /*
    public static void main(String[] args) {
       Times t = new Times();
       t.times();
       t.print("Initial");
       int count = 0;
       for(int i = 0; i<1000000000; i++) {
          count +=i; 
       }
       System.out.println("count " + count);
       t.times();
       t.print("After count loop");
       for(int i = 0; i<10000000; i++) {
          count +=i; 
       }
       System.out.println("count " + count);
       t.times();
       t.print("After 2nd count loop");

    }
    */
}
