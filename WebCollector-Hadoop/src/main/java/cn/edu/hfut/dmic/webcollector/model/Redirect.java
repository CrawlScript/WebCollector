/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author hu
 */
public class Redirect implements Writable{

    
    public Redirect(){
        
    }

    public Redirect(CrawlDatum datum, String realUrl) {
        this.datum = datum;
        this.realUrl = realUrl;
    }
    
   
    
    public CrawlDatum datum;
    public String realUrl;
    
    @Override
    public void write(DataOutput d) throws IOException {
        datum.write(d);
        d.writeUTF(realUrl);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        datum=new CrawlDatum();
        datum.readFields(di);
        realUrl=di.readUTF();
    }
    
}
