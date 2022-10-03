package flink.app;

/**
 * <b>一句话简述此类的用途</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/8/23 11:27<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class TestInterface implements FruitImpl{

    @Override
    public void m1() {
        System.out.println(" implements m1()");
    }

    @Override
    public void m11() {
        System.out.println(" implements m11()");

    }

    @Override
    public void m2() {
        System.out.println("Override m2()");
    }

    public static void main(String[] args) {
        TestInterface testInterface = new TestInterface();
        testInterface.m1();
        testInterface.m11();
        testInterface.m2();
        FruitImpl.m3();
    }

}

 interface FruitImpl {

//    public FruitImpl(){
//
//    }
  int F1=1;
  public final  static int F2=3;
//  String f3;
    void m1();
    public abstract void m11();
    default void m2(){
        System.out.println("I am default method");
    }
    static void m3(){
        System.out.println("I am static method");
    }
}
