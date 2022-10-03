package flink.app;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 * <b>一句话简述此类的用途</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/7/5 21:01<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class EnvTest {
    public static void main(String[] args) {
        //Batch DataStream API、DataSet API
        ExecutionEnvironment env01 = ExecutionEnvironment.getExecutionEnvironment();
        //Batch Table API 、SQL
        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(env01);
        //Stream DataStream API DataSet API
        StreamExecutionEnvironment env02 = StreamExecutionEnvironment.getExecutionEnvironment();
        //Stream Table API 、SQL
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env02);

        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("wwww");
        stringBuffer.append(",");
        stringBuffer.deleteCharAt(stringBuffer.length()-1);
        System.out.println(stringBuffer.toString());
    }
}
