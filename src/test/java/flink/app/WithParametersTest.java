package flink.app;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

/**
 * <b>WithParametersTest 传参 </b><br>
 *
 * <p>WithParametersTest 只适用于 批处理</p>
 * <p>
 * Date: 2022/7/12 14:16<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class WithParametersTest implements SourceFunction<Row> {

    @Override
    public void run(SourceContext<Row> sourceContext) throws Exception {
        System.out.println("Run");
    }

    @Override
    public void cancel() {
        System.out.println("Cancel");
    }

    public static void main(String[] args) throws  Exception {

        //withParameters setting Configuration
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Integer> integerDataSource = batchEnv.fromElements(1, 2, 4, 5, 66, 22, 12, 3);
        Configuration batchConf = new Configuration();
        batchConf.setInteger("limit",8);
        integerDataSource.filter(
                new RichFilterFunction<Integer>() {
                    private int limit;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        limit = parameters.getInteger("limit", 0);
                    }

                    @Override
                    public boolean filter(Integer value) throws Exception {
                        return value>limit;
                    }
                }
        ).withParameters(batchConf).print();
        //Flink Bach Job not need execute
        // batchEnv.execute();
    }

}
