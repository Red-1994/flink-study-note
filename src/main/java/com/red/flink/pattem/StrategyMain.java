package com.red.flink.pattem;

/**
 * <b>策略模式</b><br>
 *
 * <p>示例：选择不同的支付方式：</p>
 * <p>
 *      微信支付
 *      支付宝支付
 *      ePlay支付
 * Date: 2022/7/18 12:46<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class StrategyMain {
    public static void main(String[] args) {
        ContextPlay play01 = new ContextPlay(new WeiPlay());
        play01.choosePlay();
        ContextPlay play02 = new ContextPlay(new ZfbPlay());
        play02.choosePlay();
        ContextPlay play03 = new ContextPlay(new EPlay());
        play03.choosePlay();

    }
}

/**
 * 定义一个支付策略接口
 */
interface PlayStrategy {
    //支付方法
   public  void  play();
}
class WeiPlay implements PlayStrategy{
    @Override
    public void play() {
        System.out.println("微信支付");
    }
}
class ZfbPlay implements PlayStrategy{
    @Override
    public void play() {
        System.out.println("支付宝支付");
    }
}
class EPlay implements PlayStrategy{
    @Override
    public void play() {
        System.out.println("ePlay支付");
    }
}

/**
 * 支付环境上下文
 */
class ContextPlay {
   private PlayStrategy playStrategy;

   public ContextPlay(PlayStrategy ps){
       this.playStrategy=ps;
   }

   //调用支付方式
   public void choosePlay(){
       playStrategy.play();
   }
}


