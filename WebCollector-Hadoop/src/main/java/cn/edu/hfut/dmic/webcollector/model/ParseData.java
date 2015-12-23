/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.model;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author hu
 */
public class ParseData implements Writable{
    
    public ParseData(){
        
    }
    
    public ParseData(CrawlDatums next){
        this.next=next;
    }
    
    public CrawlDatums next=null;

    public CrawlDatums getNext() {
        return next;
    }

    public void setNext(CrawlDatums next) {
        this.next = next;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        next.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        next=new CrawlDatums();
        next.readFields(di);
    }
}
