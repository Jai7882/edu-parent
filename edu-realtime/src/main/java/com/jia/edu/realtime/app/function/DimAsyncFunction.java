package com.jia.edu.realtime.app.function;

import com.alibaba.fastjson.JSONObject;
import com.jia.edu.realtime.bean.DimJoinFunction;
import com.jia.edu.realtime.common.EduConfig;
import com.jia.edu.realtime.util.HbaseUtil;
import com.jia.edu.realtime.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T> {

    private StatefulRedisConnection<String,String> redisConn;
    private AsyncConnection hbaseConn;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        redisConn = RedisUtil.getAsyncRedisConnection();
        hbaseConn = HbaseUtil.getAsyncConnection();
    }

    @Override
    public void close() throws Exception {
        RedisUtil.closeAsyncRedisConn(redisConn);
        HbaseUtil.closeAsyncConnection(hbaseConn);
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        CompletableFuture.supplyAsync(
            new Supplier<JSONObject>() {
                @Override
                public JSONObject get() {
                    //从Redis中查询
                    JSONObject jsonObj = RedisUtil.asyncGetDimInfo(redisConn, tableName + ":" + getKey(obj));
                    return jsonObj;
                }
            }
            //有入参，有返回值，将上一个线程任务返回的结果，作为参数传递到当前线程任务中
        ).thenApplyAsync(
            new Function<JSONObject, JSONObject>() {
                @Override
                public JSONObject apply(JSONObject jsonObj) {
                    if(jsonObj == null){
                        //从Hbase中查询
                        jsonObj = HbaseUtil.getDimInfoFromHbaseByAsync(hbaseConn, EduConfig.HBASE_NAMESPACE,tableName,getKey(obj));
                        //将查询的内容放到Redis中进行缓存
                        RedisUtil.asyncWriteDim(redisConn,tableName+":"+getKey(obj),jsonObj);
                    }else{
                    }
                    return jsonObj;
                }
            }
        ).thenAcceptAsync(
            new Consumer<JSONObject>() {
                @Override
                public void accept(JSONObject jsonObj) {
                    //补充维度
                    if(jsonObj != null){
                        join(obj,jsonObj);
                        //向下游传递补充维度属性后的数据
                        resultFuture.complete(Collections.singleton(obj));
                    }
                }
            }
        );
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        // 根据自己情况进行处理
        throw new RuntimeException("异步超时:一般是其他原因导致的异步超时, 请检查: \n" +
            "1.检查集群是否都 ok: hdfs redis hbase kafka. \n" +
            "2.检查下 redis 的配置 bind 0.0.0.0 \n" +
            "3.检查用到的6张维度表是否都在,并且每张表都数据. 最好通过 maxwell-bootstrap 同步一下 \n" +
            "4. 找我" +
            "");

    }
}