package com.rsclouds.common;

import java.io.UnsupportedEncodingException;

import redis.clients.jedis.Jedis;

public class RedisUtils {

	public static void redisCheck(String gtPath, String redisHost)
			throws UnsupportedEncodingException {
		gtPath = gtPath.substring(0, gtPath.lastIndexOf("//"));
		gtPath = GtDataUtils.replaceLast(gtPath, "/", "//");
		Jedis jedis = new Jedis(redisHost, GtDataConfig.REDIS_PORT);
		if (jedis.exists(gtPath)) {
			String value = jedis.get(gtPath);
			if (value.endsWith("1,")) {
				value = GtDataUtils.replaceLast(value, "1,", "0,");
				jedis.set(gtPath, value);
			}
		}
		jedis.close();
	}

	public static Long redisDel(String outputPath, String redisHost) {
		Jedis jedis = new Jedis(redisHost, GtDataConfig.REDIS_PORT);
		try {
			return jedis.del(outputPath);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}

	}

	/**
	 * @param args
	 * @throws UnsupportedEncodingException
	 */
	public static void main(String[] args) throws UnsupportedEncodingException {
		// TODO Auto-generated method stub
		redisCheck("/map/ssh/data/map/theme//京津冀_PM2_5_20140513", "10.0.78.6");
		// Jedis jedis = new Jedis("10.0.78.6",6379);
		// Set keys = jedis.keys("*");//列出所有的key，查找特定的key如：redis.keys("foo")
		// Iterator t1=keys.iterator() ;
		// while(t1.hasNext()){
		// Object obj1=t1.next();
		// // System.out.println(obj1);
		// // System.out.println(jedis.get((String) obj1));
		// // System.out.println();
		// if(jedis.get((String) obj1).endsWith("1,")){
		// System.out.println(obj1);
		// System.out.println(jedis.get((String) obj1));
		// }
		// }
	}

}
