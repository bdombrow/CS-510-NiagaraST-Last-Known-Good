package niagara.utils;

public class CtrlSig {
    private ControlFlag flag = ControlFlag.NULLFLAG;
    private String msg = null;
    
    public CtrlSig () {}
    
    public CtrlSig (ControlFlag ctrlFlag, String ctrlMsg) {
    	flag = ctrlFlag;
    	msg = ctrlMsg; 
    }
    
    public ControlFlag getCtrlFlag() {return flag;}
    public String getCtrlMsg() {return msg;}
    
    public void setCtrlFlag(ControlFlag ctrlFlag) {flag = ctrlFlag;}
    public void setCtrlMsg(String ctrlMsg) {msg = ctrlMsg;}
    
}