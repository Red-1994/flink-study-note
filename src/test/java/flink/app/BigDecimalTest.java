package flink.app;

import java.math.BigDecimal;
import java.util.Arrays;

/**
 * <b>一句话简述此类的用途</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/7/20 20:46<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class BigDecimalTest {
    public static void main(String[] args) {
        double a=0.3D;
        double b=0.1D;

        BigDecimal bd01 = new BigDecimal("0.3");
        BigDecimal bd02 = new BigDecimal("0.1");


        System.out.println("double 相减:" + (a-b));
        System.out.println("BigDecimal 相减 :" + (bd01.subtract(bd02)));


        BigDecimal bd03 = BigDecimal.valueOf(0.22D);

    }
}
