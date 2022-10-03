package flink.app;


import com.red.flink.bean.StudentPojo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <b>一句话简述此类的用途</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/7/13 19:28<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class TypeInformationTest {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //禁止使用Kryo 序列化器
        env.getConfig().disableGenericTypes();
        //开启 Avro 序列化器
        env.getConfig().enableForceAvro();
        //开启 Kryo 序列化器
        env.getConfig().enableForceKryo();
        //自定义序列化器
//        env.getConfig().addDefaultKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass);

        TypeInformation<String> info = TypeInformation.of(String.class);

        TypeInformation<String> info2 = TypeInformation.of(new TypeHint<String>() {
        });

        TypeInformation<String> info3 = new TypeHint<String>() {
        }.getTypeInfo();

        DataStreamSource<String> dataStreamSource = env.fromElements("a", "bb", "a", "v");
        dataStreamSource.map(
                value -> value+=",1"
        ).returns(TypeInformation.of(String.class));
        dataStreamSource.map(
        value -> value+=",1"
        ).returns(BasicTypeInfo.STRING_TYPE_INFO);
        dataStreamSource.map(
                value -> Tuple2.of(value,1)
        ).returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

        dataStreamSource.map(
                value -> Tuple2.of(value,1)
        ).returns(new TypeHint<Tuple2<String, Integer>>() {});


        dataStreamSource.map(
                value -> Tuple2.of(value,1)
        ).returns(Types.TUPLE(Types.STRING,Types.INT));




         class MyMapFunction implements MapFunction<String, StudentPojo>, ResultTypeQueryable{
            @Override
            public StudentPojo map(String value) throws Exception {
                StudentPojo stu = new StudentPojo( 1,value);
                return stu;
            }

            @Override
            public TypeInformation getProducedType() {
                return Types.POJO(StudentPojo.class);
            }
        }
        dataStreamSource.map(
                new MyMapFunction()
        );

    }
}
