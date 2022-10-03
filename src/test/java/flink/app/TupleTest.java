package flink.app;

import com.esotericsoftware.kryo.Serializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.expressions.In;

/**
 * <b>一句话简述此类的用途</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/7/12 17:02<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class TupleTest {
    public static void main(String[] args) throws  Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        Tuple3<Integer, Integer, String> tuple1 = Tuple3.of(1, 3,"a");
        Tuple3<Integer, Integer, String> tuple2 = Tuple3.of(2, 3,"b");
        Tuple3<Integer, Integer, String> tuple3 = Tuple3.of(3, 3,"c");
        DataStreamSource<Tuple3<Integer, Integer, String>> tuple3DataStreamSource = env.fromElements(tuple1, tuple2, tuple3);



        tuple3DataStreamSource.filter(
                tuple-> tuple.f0>2
        ).print();
        tuple3DataStreamSource.filter(
                tuple-> (Integer)tuple.getField(0)>2
        ).print();

       env.execute("TupleTest");

    }
}
