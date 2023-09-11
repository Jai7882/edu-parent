package com.jia.edu.realtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

/**
 * ClassName: DimUtil
 * Package: com.jia.edu.realtime.util
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/8 10:11
 * @Version 1.0
 */
public class DimUtil {

	public static JSONObject getDimInfo(Jedis redisClient, Connection conn, String nameSpace, String tableName, String rowKey) {
		String key = tableName.toLowerCase() + ":" + rowKey;
		// redis查询结果
		String dimJsonStr = null;
		// 方法返回结果
		JSONObject jsonObject = null;
		try {
			dimJsonStr = redisClient.get(key);
			// 命中缓存
			if (dimJsonStr!=null && dimJsonStr.length()>0){
				jsonObject = JSON.parseObject(dimJsonStr);
			}else {
				// 未命中缓存 从hbase中查询
				jsonObject = HbaseUtil.getObjByRowKey(conn, nameSpace, tableName, rowKey);
				if (jsonObject != null){
					// 说明从hbase中查询到了维度数据 将数据存到redis中一份
					redisClient.setex(key,3600*24L,jsonObject.toJSONString());
				}else {
					System.out.println("在维度表中未查询到维度数据");
				}
			}
		} catch (Exception e) {
			System.out.println("从redis中查询维度信息出现异常");
		}
		return jsonObject;
	}

	public static void deleteCached(String tableName , String rowKey) {
		String key = tableName.toLowerCase() + ":" + rowKey;
		Jedis jedis = null;
		try {
			jedis = RedisUtil.getJedisClient();
			jedis.del(key);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("~~~清除Redis中缓存数据发生了异常~~~");
		}finally {
			RedisUtil.closeJedis(jedis);
		}

	}

}
