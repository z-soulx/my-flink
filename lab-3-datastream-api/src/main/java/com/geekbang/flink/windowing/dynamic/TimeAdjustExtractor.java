package com.geekbang.flink.windowing.dynamic;

/**
 * @program: geektime-flink
 * @description:
 * @author: soulx
 * @create: 2023-05-17 15:41
 **/
public interface TimeAdjustExtractor<T>   {
	long apply(T t);

	default long extract(T element) {return apply(element);};
}
