package com.geekbang.flink.watermark;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Watermark ProcessingTime 案例
 */
public class StreamingProcessingTimeWindowWatermark {

	/**
	 * 在机器上启动 nc -lk 9000
	 */

	public static void main(String[] args) throws Exception {
		//定义socket的端口号
		int port = 9000;
		//获取运行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		//设置使用eventtime，默认是使用processtime
//		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		//设置并行度为1,默认并行度是当前机器的cpu数量
		env.setParallelism(1);

		//连接socket获取输入的数据
		DataStream<String> text = env.socketTextStream("localhost", port, "\n");

		//解析输入的数据
		SingleOutputStreamOperator<String> filter = text.filter(new FilterFunction<String>() {
			@Override
			public boolean filter(String s) throws Exception {
				return !"".equals(s);
			}
		});
//		text.assignTimestampsAndWatermarks()
		DataStream<Tuple2<String, Long>> inputMap = filter
				.map(new MapFunction<String, Tuple2<String, Long>>() {
					@Override
					public Tuple2<String, Long> map(String value) throws Exception {
						if ("".equalsIgnoreCase(value)) {
							return null;
						}
						String[] arr = value.split(",");
						return new Tuple2<>(arr[0], 0L);
					}
				});


		DataStream<String> window = inputMap.keyBy(0)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(3)))//按照消息的EventTime分配窗口，和调用TimeWindow效果一样
				.apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
					/**
					 * 对window内的数据进行排序，保证数据的顺序
					 * @param tuple
					 * @param window
					 * @param input
					 * @param out
					 * @throws Exception
					 */
					@Override
					public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input,
							Collector<String> out) throws Exception {
						String key = tuple.toString();
						List<Long> arrarList = new ArrayList<Long>();
						Iterator<Tuple2<String, Long>> it = input.iterator();
						while (it.hasNext()) {
							Tuple2<String, Long> next = it.next();
							arrarList.add(next.f1);
						}
						Collections.sort(arrarList);
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
						String result =
								key + "," + arrarList.size() + "," + sdf.format(arrarList.get(0)) + "," + sdf
										.format(arrarList.get(arrarList.size() - 1))
										+ "," + sdf.format(window.getStart()) + "," + sdf.format(window.getEnd());
						out.collect(result);
					}
				});
		//测试-把结果打印到控制台即可
		window.print();

		//注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
		env.execute("eventtime-watermark");

	}


}
