package com.test.ecommerceuserprofile.utils;

import org.apache.spark.sql.SparkSession;

/**
 * @author sunzhipeng
 * @create 2020-11-08 10:47
 */
public class SparkUtils {
    //定义一个spark session的会话池
    private static ThreadLocal<SparkSession> sessionPool=new ThreadLocal<>();
    //初始化spark session的方法
    public static SparkSession initSession(){
        //先判断会话池中是否有session,如果有直接用，如果没有再创建
        if (sessionPool.get()!=null){
            return sessionPool.get();
        }

        SparkSession session= SparkSession.builder()
                .appName("userprofile-etl")
                .master("local[*]")
                .config("es.nodes","localhost")
                .config("es.port","9200")
                .config("es.index.auto.create","false")
                .enableHiveSupport()
                .getOrCreate();
        sessionPool.set(session);
        return  session;
    }
}
