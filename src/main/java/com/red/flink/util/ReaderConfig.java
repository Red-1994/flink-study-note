package com.red.flink.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

/**
 * <b>解析配置文件</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/7/4 16:43<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class ReaderConfig {
    private  static Logger logger= LoggerFactory.getLogger(ReaderConfig.class);
    private   Properties prop=new Properties();

    public  Properties getProp() {
        return prop;
    }

    /**
     * 读取配置信息文件加载到 Properties
     * @param path 文件路径
     */
    public void readerProperties(String path){
        InputStream inputStream = getClass().getResourceAsStream(path);
        try {
            prop.load(inputStream);
        }catch (Exception e){
            logger.error(String.format("readerProperties ERROR! file path Not exists:%s,Exception:%s",path,e.toString()));
            e.printStackTrace();
        }
    }

}
