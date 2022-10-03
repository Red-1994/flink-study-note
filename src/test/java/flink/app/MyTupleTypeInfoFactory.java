package flink.app;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * <b>一句话简述此类的用途</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/7/13 21:03<br><br>
 *
 * @author 31528
 * @version 1.0
 */
@TypeInfo(MyTupleTypeInfoFactory.class)
class MyTuple<T0,T1> {
 public T0 t0;
 public T1 t1;
}
class MyTupleTypeInfo<T0,T1> extends TypeInformation<MyTuple<T0,T1>>{

    public TypeInformation col0;
    public TypeInformation col1;
  public MyTupleTypeInfo(TypeInformation t0,TypeInformation t1){
      col0=t0;
      col1=t1;
  }
    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 0;
    }

    @Override
    public int getTotalFields() {
        return 0;
    }

    @Override
    public Class<MyTuple<T0, T1>> getTypeClass() {
        return null;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<MyTuple<T0, T1>> createSerializer(ExecutionConfig config) {
        return null;
    }

    @Override
    public String toString() {
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean canEqual(Object obj) {
        return false;
    }
}
public class MyTupleTypeInfoFactory  extends TypeInfoFactory<MyTuple> {
    @Override
    public TypeInformation<MyTuple> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        return new MyTupleTypeInfo(genericParameters.get("T0"),genericParameters.get("T1"));
    }
}


