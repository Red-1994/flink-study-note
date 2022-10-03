package flink.bean;

/**
 * <b>一句话简述此类的用途</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/8/9 11:00<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class Temperature {
    private String tid;
    private Long  ts;
    private Double tvalue;

    public Temperature(){

    }

    public Temperature(String tid, Long ts, Double tvalue) {
        this.tid = tid;
        this.ts = ts;
        this.tvalue = tvalue;
    }

    public String getTid() {
        return tid;
    }

    public void setTid(String tid) {
        this.tid = tid;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Double getTvalue() {
        return tvalue;
    }

    public void setTvalue(Double tvalue) {
        this.tvalue = tvalue;
    }
}
