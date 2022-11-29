package com.database_engine.common.ProcessResult;

/**
 * ProcessResult class is the standardized output returned after any action
 * is performed.
 */
public class ProcessResult implements IProcessResult {
    boolean isSucceeded;
    String message;
    Object data;
    String methodName;

    public boolean isSucceeded() {
        return isSucceeded;
    }

    public void setSucceeded(boolean isSucceeded) {
        this.isSucceeded = isSucceeded;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public ProcessResult(boolean isSucceeded, String message, Object data, String methodName) {
        this.isSucceeded = isSucceeded;
        this.message = message;
        this.data = data;
        this.methodName = methodName;
    }
}
