package niagara.utils;

import java.text.DecimalFormat;

public class CPUTimer {

    static double nspmin = StrictMath.pow(10,9)*60;
    static double nspsec = StrictMath.pow(10,9);
    static double nspms = StrictMath.pow(10,6);
    static double nspus = StrictMath.pow(10,3);

    // differences
    long start_time;
    long stop_time;
    long used_time;

    private boolean timerRunning = false;

    public CPUTimer() {
    }
    
    public void start() {
	assert !timerRunning : 
	    "Attempt to start timer when it is already running";
	start_time = JProf.getCurrentThreadCpuTime();
	timerRunning = true;
    }

    public void stop() {
	assert timerRunning : "Attempt to stop timer that is not running";
	stop_time = JProf.getCurrentThreadCpuTime();
	timerRunning = false;
	calculateTimeUsed();
    }

    public long getTimeNSec() {
	assert !timerRunning : "Can't get time while timer is running";
	return used_time;
    }

    private void calculateTimeUsed() {
	used_time = stop_time - start_time;
    }

    public void print(String msg) {
	assert !timerRunning : "Can't print while timer is running";
	System.out.print(msg);
	double time = used_time;
	long min;
	long sec;
	long ms;
	long us;
	long ns;
	
	min = (long)StrictMath.floor(time/nspmin);
	time -= min*nspmin;
	sec = (long)StrictMath.floor(time/nspsec);
	time -= sec*nspsec;
	ms = (long)StrictMath.floor(time/nspms);
	time -= ms*nspms;
	us = (long)StrictMath.floor(time/nspus);
	time -= us*nspus;
	ns = (long)time;

	DecimalFormat df = new DecimalFormat("#.000");
	System.out.println(" Time: " + min + ":" + sec + df.format(ms/1000.0));
	//+ us/1000000.0 + ns/1000000000.0);
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
