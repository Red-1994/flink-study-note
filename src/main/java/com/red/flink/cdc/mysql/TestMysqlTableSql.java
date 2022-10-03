package com.red.flink.cdc.mysql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * <b>一句话简述此类的用途</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/8/15 10:52<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class TestMysqlTableSql {
    public static void main(String[] args) throws Exception {

        //0.创建 StreamTableEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1.FlinkSQL 构建 CDC表
        tableEnv.executeSql("CREATE TABLE orders (\n" +
                "     order_id INT,\n" +
                "     order_date TIMESTAMP(0),\n" +
                "     customer_name STRING,\n" +
                "     price DECIMAL(10, 5),\n" +
                "     product_id INT,\n" +
                "     order_status BOOLEAN,\n" +
                "     PRIMARY KEY(order_id) NOT ENFORCED\n" +
                "     ) WITH (\n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = 'localhost',\n" +
                "     'port' = '3306',\n" +
                "     'username' = 'user_flink',\n" +
                "     'password' = '123456',\n" +
                "     'database-name' = 'srm',\n" +
                "     'table-name' = 'orders')");
        //2. 执行查询SQL
        Table ordersTable = tableEnv.sqlQuery(" select * from  orders");
        tableEnv.toRetractStream(ordersTable, Row.class).print();

        env.execute("TestMysqlTableSql");
    }
}
