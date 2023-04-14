package com.geekbang.flink.cep;

import com.alibaba.fastjson.JSON;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @program: my-flink
 * @description:
 * @author: soulx
 * @create: 2023-04-14 12:07
 **/
public class MySource extends RichSourceFunction<String> {

	@Override
	public void run(SourceContext<String> ctx) throws Exception {

		Map<String,String> map = new HashMap<>();
		map.put("userid","1");
		map.put("orderid","2222");
		map.put("behave","order");
		ctx.collect(JSON.toJSONString(map));

		Thread.sleep(1000);

		map.put("userid","1");
		map.put("orderid","2222");
		map.put("behave","pay");
		ctx.collect(JSON.toJSONString(map));
		Thread.sleep(1000);

		map.put("userid","2");
		map.put("orderid","2223");
		map.put("behave","pay");
		ctx.collect(JSON.toJSONString(map));
		Thread.sleep(1000);

		map.put("userid","2");
		map.put("orderid","2224");
		map.put("behave","order");
		ctx.collect(JSON.toJSONString(map));
		Thread.sleep(3000);
//		Thread.sleep(4000);

		map.put("userid","2");
		map.put("orderid","2225");
		map.put("behave","order");
		ctx.collect(JSON.toJSONString(map));
		Thread.sleep(4000);


		map.put("userid","2");
		map.put("orderid","2226");
		map.put("behave","order");
		ctx.collect(JSON.toJSONString(map));
		Thread.sleep(8000);
	}
	@Override
	public void cancel() {

	}
}

