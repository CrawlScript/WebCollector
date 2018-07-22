//package cn.edu.hfut.dmic.webcollector.util;
//
//import org.apache.avro.Schema;
//import org.apache.avro.file.CodecFactory;
//import org.apache.avro.file.DataFileWriter;
//import org.apache.avro.io.DatumWriter;
//import org.apache.avro.reflect.ReflectData;
//import org.apache.avro.reflect.ReflectDatumWriter;
//
//import java.io.File;
//import java.io.IOException;
//
//public class ReflectAvroFileWriter<T>{
//
//    protected DataFileWriter<T> writer;
//    protected Schema schema;
//
//    public ReflectAvroFileWriter(File file, Class<T> _class) throws IOException {
//        this(file,_class,false);
//    }
//
//    public ReflectAvroFileWriter(File file, Class<T> _class, boolean append) throws IOException {
//        schema = ReflectData.get().getSchema(_class);
//        DatumWriter<T> datumWriter = new ReflectDatumWriter<T>(_class);
//        writer = new DataFileWriter<T>(datumWriter)
//                .setCodec(CodecFactory.deflateCodec(9));
//
//        if(append && file.exists()){
//            writer = writer.appendTo(file);
//        }else{
//            writer = writer.create(schema, file);
//        }
//    }
//
//    public void append(T data) throws IOException {
//        writer.append(data);
//    }
//
//    public void append(T data, boolean flush) throws IOException {
//        writer.append(data);
//        if (flush) {
//            writer.flush();
//        }
//    }
//    public void flush() throws IOException {
//        writer.flush();
//    }
//
//    public void close() throws IOException {
//        writer.close();
//    }
//}