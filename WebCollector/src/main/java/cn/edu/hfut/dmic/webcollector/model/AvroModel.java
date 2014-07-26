/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.model;

import cn.edu.hfut.dmic.webcollector.generator.Injector;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;

/**
 *
 * @author hu
 */
public class AvroModel {
    public static Schema getPageSchema(){
        try {
            return new Schema.Parser().parse(AvroModel.class.getResourceAsStream("/page.avsc"));
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }
    
}
