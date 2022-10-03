package com.red.flink.pattem;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * <b>状态模式</b><br>
 *
 * <p>示例： 订单状态</p>
 * <p>
 *     下单 -> 配送 -> 收货
 *                         ->完成
 * Date: 2022/7/18 13:48<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class StateMain {
    public static void main(String[] args) {

        OrderStateContext context = new OrderStateContext();
        OrderState placeOrderState = new PlaceOrderState();
        OrderState delverOrderState = new DelverOrderState();
        OrderState receiveOrderState = new ReceiveOrderState();
        OrderState finishOrderState = new FinishOrderState();

        context.registerState(StateCode.PLACE,placeOrderState);
        context.registerState(StateCode.DELVER,delverOrderState);
        context.registerState(StateCode.RECEIVE,receiveOrderState);
        context.registerState(StateCode.FINISH,finishOrderState);

         //设置初始值
        context.setStateCode(StateCode.PLACE);
        context.changeState();
        context.changeState();
        context.changeState();
        context.changeState();
        context.changeState();

    }
}

/**
 * 定义一个订单状态 抽象类
 */
abstract class OrderState{
  public abstract void doAction();

  //获取下一个订单状态
  public abstract  StateCode  getNext();
  //
}
enum StateCode{
   PLACE,//下单
   DELVER,//配送
   RECEIVE,//收货
    FINISH//完成
}
class PlaceOrderState extends OrderState{
    @Override
    public void doAction() {
        System.out.println("下单完成");
    }

    @Override
    public StateCode getNext() {
        return StateCode.DELVER;
    }
}
class DelverOrderState extends OrderState{
    @Override
    public void doAction() {
        System.out.println("配送完成");
    }

    @Override
    public StateCode getNext() {
        return StateCode.RECEIVE;
    }
}
class ReceiveOrderState extends OrderState{
    @Override
    public void doAction() {
        System.out.println("收货完成");
    }

    @Override
    public StateCode getNext() {
        return StateCode.FINISH;
    }
}
class FinishOrderState extends OrderState{
    @Override
    public void doAction() {
        System.out.println("流程结束");
    }

    @Override
    public StateCode getNext() {
        return StateCode.FINISH;
    }
}
class OrderStateContext{
 private StateCode stateCode;
 private OrderState orderState;
 private Map<StateCode,OrderState> stateMap=new HashMap<>();



 //注册 StateCode stateCode, OrderState orderState 到 Map字典中
 public void registerState(StateCode stateCode, OrderState orderState){
     stateMap.put(stateCode,orderState);
 }

    public OrderState getOrderState() {
        return stateMap.get(this.stateCode);
    }

    public void setStateCode(StateCode stateCode) {
        this.stateCode = stateCode;
        this.orderState= getOrderState();
    }



    public StateCode getStateCode() {
        return stateCode;
    }

    //改变状态的方法
    public void changeState(){
        this.orderState.doAction();
        this.stateCode=orderState.getNext();
        this.orderState=getOrderState();
    }
}