package flink.app;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.codec.digest.DigestUtils;


import org.apache.flink.calcite.shaded.com.google.common.hash.HashFunction;
import org.apache.flink.calcite.shaded.com.google.common.hash.Hashing;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;


/**
 * <b>一句话简述此类的用途</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/7/20 19:26<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class KryoTest {
    public static void main(String[] args) {
        Kryo kryo = new Kryo();
        ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
        Output output = new Output(byteArrayOS);

        ArrayList<String> className01 = new ArrayList<>();
        className01.add("Spark");
        className01.add("Flink");

        ArrayList<String> className02 = new ArrayList<>();
        className02.add("Spark");
        className02.add("Flink");


        StudentPojo stu01 = new StudentPojo("张三", 22,new BigDecimal("22.20"));
        stu01.setClassName(className01);

        StudentPojo stu02 = new StudentPojo("张三", 22,new BigDecimal("22.20"));
        stu02.setClassName(className02);

        StudentPojo stu03 = new StudentPojo("李四", 22,new BigDecimal("22.2"));
        kryo.writeObject(output,stu01);
        output.flush();
        String bytesString01 = byteArrayOS.toString();
        byte[] bytes01 = byteArrayOS.toByteArray();
        byteArrayOS.reset();

        kryo.writeObject(output,stu02);
        output.flush();
        String bytesString02 = byteArrayOS.toString();
        byte[] bytes02 = byteArrayOS.toByteArray();
        byteArrayOS.reset();

        kryo.writeObject(output,stu03);
        output.flush();
        String bytesString03 = byteArrayOS.toString();
        byte[] bytes03 = byteArrayOS.toByteArray();
        byteArrayOS.reset();

        System.out.println(String.format("stu01: %s", bytesString01));
        System.out.println(String.format("stu02: %s", bytesString02));
        System.out.println(String.format("stu03: %s", bytesString03));

        System.out.println(String.format("stu01和 stu02: %s", bytesString01.equals(bytesString02)));
        System.out.println(String.format("stu01和 stu03: %s", bytesString01.equals(bytesString03)));

        //MD5 加密
        System.out.println(String.format("stu01 MD5: %s", DigestUtils.md5Hex(bytes01)));
        System.out.println(String.format("stu02 MD5: %s", DigestUtils.md5Hex(bytes02)));
        System.out.println(String.format("stu03 MD5: %s", DigestUtils.md5Hex(bytes03)));

        HashFunction hashFunc = Hashing.murmur3_128();
        //MurmurHash 加密
        System.out.println(String.format("stu01 MurmurHash: %s", hashFunc.hashBytes(bytes01)));
        System.out.println(String.format("stu02 MurmurHash: %s", hashFunc.hashBytes(bytes02)));
        System.out.println(String.format("stu03 MurmurHash: %s", hashFunc.hashBytes(bytes03)));

    }
}
class StudentPojo implements java.io.Serializable {
    private static final long serialVersionUID=1L;

    public String name;
    public Integer age;
    public BigDecimal score;
    public List<String> className;

    public StudentPojo(){

    }

    public StudentPojo(String name, Integer age,BigDecimal score) {
        this.name = name;
        this.age = age;
        this.score=score;
    }

    public void setClassName(List<String> className) {
        this.className = className;
    }

    @Override
    public String toString() {
        return "StudentPojo{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", score=" + score +
                ", className=" + className +
                '}';
    }
}
