package com.red.flink.bean;

/**
 * <b>一句话简述此类的用途</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/8/9 14:21<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class WordCountPojo {
    public String word;
    public Integer count;

    public WordCountPojo(){

    }

    public WordCountPojo(String word, Integer count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "WordCountPojo{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}
