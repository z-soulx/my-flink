package com.geekbang.flink.state.broadcaststate;

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
public class MySource2 extends RichSourceFunction<Long> {

	@Override
	public void run(SourceContext<Long> ctx) throws Exception {


		ctx.collect(0L);

		Thread.sleep(1000);


		ctx.collect(1L);
		Thread.sleep(1000);


		ctx.collect(2L);
		Thread.sleep(1000);


		ctx.collect(3L);
		Thread.sleep(3000);
//		Thread.sleep(4000);

		ctx.collect(4L);
		Thread.sleep(4000);



		Thread.sleep(8000);
	}
	@Override
	public void cancel() {

	}
}

