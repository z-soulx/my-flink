package com.geekbang.flink.watermark;


import com.geekbang.flink.data.EventTimeData;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by jun on 04/12/15.
 */
public class WatermarkTest {

	/**
	 * IllegalStateException: No ExecutorFactory found to execute the application.
	 *
	 *
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// Window base on event time
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		/**
		 * 这种一次性读取所有时间的数据，由于水位推动理论等原因。不会把按照数据排列的过期数据过滤
		 */
		DataStream<Tuple3<Long, String, Long>> interest =
				env.fromCollection(EventTimeData.watermark)
						.assignTimestampsAndWatermarks(
								new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, String, Long>>(Time.of(0,TimeUnit.SECONDS)) {
									@Override
									public long extractTimestamp(Tuple3<Long, String, Long> t) {
										return t.f0;
									}
								});
		interest.keyBy(1)
				.window(TumblingTimeWindows.of(Time.of(3, TimeUnit.SECONDS)))
				.apply(new WindowFunction<Tuple3<Long, String, Long>, String, Tuple, TimeWindow>() {
					private static final long serialVersionUID = 5895421441687138440L;

					/**
					 * 对window内的数据进行排序，保证数据的顺序
					 * @param tuple
					 * @param window
					 * @param input
					 * @param out
					 * @throws Exception
					 */
					@Override
					public void apply(
							Tuple tuple, TimeWindow window, Iterable<Tuple3<Long, String, Long>> input,
							Collector<String> out) throws Exception {
						String key = tuple.toString();
						List<Long> arrarList = new ArrayList<Long>();
						Iterator<Tuple3<Long, String, Long>> it = input.iterator();
						while (it.hasNext()) {
							Tuple3<Long, String, Long> next = it.next();
							arrarList.add(next.f0);
						}
						Collections.sort(arrarList);
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
						String result = "\n  键值 : " + key + "\n              触发窗内数据个数 : " + arrarList.size()
								+ "\n              触发窗起始数据： " + sdf.format(arrarList.get(0))
								+ "\n              触发窗最后（可能是延时）数据：" + sdf
								.format(arrarList.get(arrarList.size() - 1))
								+ "\n              实际窗起始和结束时间： " + sdf.format(window.getStart()) + "《----》" + sdf
								.format(window.getEnd()) + " \n \n ";
						out.collect(result);
					}
				}).print();
		env.execute("test");
	}
}
