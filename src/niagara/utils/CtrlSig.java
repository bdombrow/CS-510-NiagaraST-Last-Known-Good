package niagara.utils;

public class CtrlSig {
    private int flag = CtrlFlags.NULLFLAG;
    private String msg = null;
    
    public CtrlSig () {}
    
    public CtrlSig (int ctrlFlag, String ctrlMsg) {
    	flag = ctrlFlag;
    	msg = ctrlMsg; 
    }
    
    public int getCtrlFlag() {return flag;}
    public String getCtrlMsg() {return msg;}
    
    public void setCtrlFlag(int ctrlFlag) {flag = ctrlFlag;}
    public void setCtrlMsg(String ctrlMsg) {msg = ctrlMsg;}
    
}