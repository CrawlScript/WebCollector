//package cn.edu.hfut.dmic.webcollector.util;
//
//import org.junit.Test;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Random;
//
//import static org.junit.Assert.assertEquals;
//
//public class AvroTest {
//
//    public static class User{
//        public String name;
//        public int age;
//
//        public User() {
//        }
//
//        public User(String name, int age) {
//            this.name = name;
//            this.age = age;
//        }
//
//        @Override
//        public String toString() {
//            return "User{" +
//                    "name='" + name + '\'' +
//                    ", age=" + age +
//                    '}';
//        }
//    }
//
//    @Test
//    public void testAvro() throws IOException {
//
//        Random random = new Random();
//        ArrayList<User> userList = new ArrayList<User>();
//        for(int i=0;i<10;i++){
//            userList.add(new User("user"+i, random.nextInt(100)));
//        }
//
//        File avroFile = new File("test.avro");
//        ReflectAvroFileWriter<User> writer =
//                new ReflectAvroFileWriter<User>(avroFile, User.class);
//        for(User user:userList.subList(0,userList.size()/2)){
//            writer.append(user);
//        }
//        writer.close();
//
//        writer = new ReflectAvroFileWriter<User>(avroFile,User.class, true);
//        for(User user:userList.subList(userList.size()/2,userList.size())){
//            writer.append(user);
//        }
//        writer.close();
//
//        ReflectAvroFileReader<User> reader =
//                new ReflectAvroFileReader<User>(avroFile, User.class);
//
//        ArrayList<User> readList = new ArrayList<User>();
//        while(reader.hasNext()){
//            readList.add(reader.next());
//        }
//        reader.close();
//        avroFile.delete();
//        assertEquals(userList.size(), readList.size());
//        for(int i=0;i<userList.size();i++){
//            User expectedUser = userList.get(i);
//            User readUser = readList.get(i);
//            assertEquals(expectedUser.name, readUser.name);
//            assertEquals(expectedUser.age, readUser.age);
//        }
//
//    }
//}
