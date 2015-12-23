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
public class Content implements Writable{
    public Content(){
        
    }
    
    public Content(String key,byte[] content){
        this.key=key;
        this.content=content;
    }
    
    public String key;
    public byte [] content=new byte[0];

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeUTF(key);
        d.writeInt(content.length);
        if(content.length>0){
            d.write(content);
        }
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        key=di.readUTF();
        int len=di.readInt();
        if(len==0){
            content=new byte[0];
        }else{
            content=new byte[len];
            di.readFully(content);
        }
    }
    
    
}
