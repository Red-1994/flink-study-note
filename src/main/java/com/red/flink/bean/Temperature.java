package com.red.flink.bean;

import lombok.Data;

import java.sql.Timestamp;

/**
 * <b>一句话简述此类的用途</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/8/9 10:58<br><br>
 *
 * @author 31528
 * @version 1.0
 */
@Data
public class Temperature {
    String id;
    Timestamp ts;
    Double temperatureValue;

}
