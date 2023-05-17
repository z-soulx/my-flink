package com.geekbang.flink.windowing.dynamic;

/**
 * @program: geektime-flink
 * @description: 使用处理函数实现您自己的窗口化
 * @author: soulx
 * @create: 2023-05-17 14:43
 **/

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**Flink's window API does not support dynamically changing window sizes.

		What you'll need to do is to implement your own windowing using a process function. In this case a KeyedBroadcastProcessFunction, where the window configuration is broadcast.

		You can examine [the Flink training](https://ci.apache.org/projects/flink/flink-docs-master/training/event_driven.html#example) for an example of how to implement time windows with a KeyedProcessFunction (copied below):

		**/
public class PseudoWindow extends
		KeyedProcessFunction<String, KeyedDataPoint<Double>, KeyedDataPoint<Integer>> {
	// Keyed, managed state, with an entry for each window.
	// There is a separate MapState object for each sensor.
	private MapState<Long, Integer> countInWindow;

	boolean eventTimeProcessing;
	int durationMsec;

	/**
	 * Create the KeyedProcessFunction.
	 * @param eventTime whether or not to use event time processing
	 * @param durationMsec window length
	 */
	public PseudoWindow(boolean eventTime, int durationMsec) {
		this.eventTimeProcessing = eventTime;
		this.durationMsec = durationMsec;
	}

	@Override
	public void open(Configuration config) {
		MapStateDescriptor<Long, Integer> countDesc =
				new MapStateDescriptor<>("countInWindow", Long.class, Integer.class);
		countInWindow = getRuntimeContext().getMapState(countDesc);
	}

	@Override
	public void processElement(
			KeyedDataPoint<Double> dataPoint,
			Context ctx,
			Collector<KeyedDataPoint<Integer>> out) throws Exception {

		long endOfWindow = setTimer(dataPoint, ctx.timerService());

		Integer count = countInWindow.get(endOfWindow);
		if (count == null) {
			count = 0;
		}
		count += 1;
		countInWindow.put(endOfWindow, count);
	}

	public long setTimer(KeyedDataPoint<Double> dataPoint, TimerService timerService) {
		long time;

		if (eventTimeProcessing) {
			time = dataPoint.getTimeStampMs();
		} else {
			time = System.currentTimeMillis();
		}
		long endOfWindow = (time - (time % durationMsec) + durationMsec - 1);

		if (eventTimeProcessing) {
			timerService.registerEventTimeTimer(endOfWindow);
		} else {
			timerService.registerProcessingTimeTimer(endOfWindow);
		}
		return endOfWindow;
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext context, Collector<KeyedDataPoint<Integer>> out) throws Exception {
		// Get the timestamp for this timer and use it to look up the count for that window
		long ts = context.timestamp();
		KeyedDataPoint<Integer> result = new KeyedDataPoint<>(context.getCurrentKey(), ts, countInWindow.get(ts));
		out.collect(result);
		countInWindow.remove(timestamp);
	}
}

