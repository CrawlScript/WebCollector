package cn.edu.hfut.dmic.webcollector.util;

public class ExceptionUtils {
    public static void fail(String message){
        throw new RuntimeException(message);
    }

    public static void fail(String message, Throwable cause){
        throw new RuntimeException(message,cause);
    }
    public static void fail(Throwable cause){
        throw new RuntimeException(cause);
    }

    public static void fail(){
        throw new RuntimeException();
    }
}
