package cn.edu.hfut.dmic.webcollector.net;

import cn.edu.hfut.dmic.webcollector.model.Page;

import javax.swing.*;

public class Test {

    public static Object obj = null;

    public static<T> T get(){
        return (T)obj;
    }

    public static void main(String[] args) {
//        Page page = new Page(null,null);
//        page.con
        obj = new JFrame();
        JFrame frame = get();
        frame.show();
    }
}
