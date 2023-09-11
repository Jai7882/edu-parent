package com.jia.edu.realtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

/**
 * ClassName: RedisUtil
 * Package: com.jia.edu.realtime.util
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/8 10:15
 * @Version 1.0
 */
public class RedisUtil {

	private static JedisPool jedisPool;

	static {
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(100);
		poolConfig.setMinIdle(5);
		poolConfig.setMaxIdle(5);
		poolConfig.setMaxWait(Duration.ofSeconds(10L));
		poolConfig.setTestOnBorrow(true);
		poolConfig.setBlockWhenExhausted(true);
		jedisPool = new JedisPool(poolConfig, "hadoop102", 6379);
	}

	public static Jedis getJedisClient() {
		return jedisPool.getResource();
	}

	public static void closeJedis(Jedis jedis) {
		if (jedis != null) {
			jedis.close();
		}
	}

	public static StatefulRedisConnection<String, String> getAsyncRedisConnection() {
		RedisClient redisClient = RedisClient.create("redis://hadoop102:6379/0");
		StatefulRedisConnection<String, String> connection = redisClient.connect();
		return connection;
	}

	public static void closeAsyncRedisConn(StatefulRedisConnection<String, String> redisConn) {
		if (redisConn != null && redisConn.isOpen()) {
			redisConn.close();
		}
	}

	public static JSONObject asyncGetDimInfo(StatefulRedisConnection<String, String> redisConn, String key) {
		RedisAsyncCommands<String, String> async = redisConn.async();
		try {
			String dimJsonStr = async.get(key).get();
			if (StringUtils.isNotEmpty(dimJsonStr)) {
				// 命中缓存
				return JSON.parseObject(dimJsonStr);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return null;
	}

	public static void asyncWriteDim(StatefulRedisConnection<String, String> redisConn, String key, JSONObject jsonObj) {
		RedisAsyncCommands<String, String> async = redisConn.async();
		async.setex(key,3600*24L,jsonObj.toJSONString());
	}
}
