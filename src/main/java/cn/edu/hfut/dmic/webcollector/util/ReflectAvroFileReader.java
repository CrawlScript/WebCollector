//package cn.edu.hfut.dmic.webcollector.util;
//
//import org.apache.avro.file.DataFileReader;
//import org.apache.avro.io.DatumReader;
//import org.apache.avro.reflect.ReflectDatumReader;
//
//import java.io.File;
//import java.io.IOException;
//
//public class ReflectAvroFileReader<T>{
//
//    protected DataFileReader<T> reader;
//
//    public ReflectAvroFileReader(File file, Class<T> _class) throws IOException {
//        DatumReader<T> datumReader = new ReflectDatumReader<T>(_class);
//        reader = new DataFileReader<T>(file, datumReader);
//    }
//
//    public boolean hasNext(){
//        return reader.hasNext();
//    }
//
//    public T next(){
//        return reader.next();
//    }
//
//    public void close() throws IOException {
//        reader.close();
//    }
//
//}
