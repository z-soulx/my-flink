package com.geekbang.flink.cep;

/**
 * @program: my-flink
 * @description:
 * @author: soulx
 * @create: 2023-04-14 12:08
 **/

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * 1生成一个datastream或者KeyedStream
 * 2定义一组规则
 * 3将这组规则应用于流，生成PatternStream
 * 4将生成的PatternStream，通过select方法，将符合规则的数据通过自定义的输出形式，生成结果流
 * 5 结果流就是我们最终得到的数据
 *
 *
 * ## 后一条数据 推动前一条计算
 *
 *  这个例子是判断订单是否正常，譬如说，订单的有效时间是30分钟，所以要在30分钟内完成支付，才算一次正常的支付。
 *  如果超过了30分钟，用户依然发起了支付动作，这个时候就是有问题的，要发出一条指令告诉用户该订单已经超时。
 * @See https://blog.csdn.net/lvwenyuan_1/article/details/93080820
 */
public class TestCEP {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/**
		 *  接收source并将数据转换成一个tuple
		 */
		DataStream<Tuple3<String, String, String>> myDataStream = env.addSource(new MySource())
				.map(new MapFunction<String, Tuple3<String, String, String>>() {
					@Override
					public Tuple3<String, String, String> map(String value) throws Exception {

						JSONObject json = JSON.parseObject(value);
						return new Tuple3<>(json.getString("userid"), json.getString("orderid"),
								json.getString("behave"));
					}
				});

		/**
		 * 定义一个规则
		 * 接受到behave是order以后，下一个动作必须是pay才算符合这个需求
		 */
		Pattern<Tuple3<String, String, String>, Tuple3<String, String, String>> myPattern = Pattern.<Tuple3<String, String, String>>begin(
				"start").where(new IterativeCondition<Tuple3<String, String, String>>() {
			@Override
			public boolean filter(Tuple3<String, String, String> value,
					Context<Tuple3<String, String, String>> ctx) throws Exception {
				System.out.println("value:" + value);
				return value.f2.equals("order");
			}
//		}).next("next").where(new IterativeCondition<Tuple3<String, String, String>>() {
			/**
			 * next是严格邻近。其他的是非严格邻近。
			 * next是严格邻近:  那么匹配上每次order后都会重新计时间，来匹配pay， 若连续多个order过来，超时也只会输出最后一条超时的
			 * 非严格邻近： 则允许中间穿插，超时也只会输出所有超时的
			 */
		}).followedByAny("next").where(new IterativeCondition<Tuple3<String, String, String>>() {
			@Override
			public boolean filter(Tuple3<String, String, String> value,
					Context<Tuple3<String, String, String>> ctx) throws Exception {
				return value.f2.equals("pay");
			}
		}).within(Time.seconds(3));

		PatternStream<Tuple3<String, String, String>> pattern = CEP
				.pattern(myDataStream.keyBy(0), myPattern);

		//记录超时的订单
		OutputTag<String> outputTag = new OutputTag<String>("myOutput") {
		};

		SingleOutputStreamOperator<String> resultStream = pattern.select(outputTag,
				/**
				 * 超时的
				 */
				new PatternTimeoutFunction<Tuple3<String, String, String>, String>() {
					@Override
					public String timeout(Map<String, List<Tuple3<String, String, String>>> pattern,
							long timeoutTimestamp) throws Exception {
						System.out.println("pattern:" + pattern);
						List<Tuple3<String, String, String>> startList = pattern.get("start");
						Tuple3<String, String, String> tuple3 = startList.get(0);
						return tuple3.toString() + "迟到的";
					}
				}, new PatternSelectFunction<Tuple3<String, String, String>, String>() {
					@Override
					public String select(Map<String, List<Tuple3<String, String, String>>> pattern)
							throws Exception {
						//匹配上第一个条件的
						List<Tuple3<String, String, String>> startList = pattern.get("start");
						//匹配上第二个条件的
						List<Tuple3<String, String, String>> endList = pattern.get("next");

						Tuple3<String, String, String> tuple3 = endList.get(0);
						return tuple3.toString() +"match";
					}
				}
		);

		//输出匹配上规则的数据
		resultStream.print();

		//输出超时数据的流
		DataStream<String> sideOutput = resultStream.getSideOutput(outputTag);
//		sideOutput.print();

		env.execute("Test CEP");
	}


}

/**
 * 发现点：
 * 1.对于超时数据来说(匹配上了一个条件，但是在规定时间内，下一条数据没有匹配上第二个条件)，他只有等到下一条数据来了，才会判断上一条数据是否超时了。而不是等到时间窗口到了，就立即判断这条数据是否超时。
 *
 * 2.上面例子中的 next("next") 可以替换成followedByAny("next")或者是followedBy("next")。
 *
 * 这里涉及到两个概念 ，next是严格邻近。其他的是非严格邻近。
 *
 * 严格邻近：表示当前数据流中，所匹配的数据必须是严格一前一后的，中间没有其他数据。譬如上述的，来了3条数据，A数据：userid = 1,orderid=2,behave=create，B数据：userid = 1,orderid=2,behave=create，C数据：userid = 1,orderid=2,behave=pay
 *
 * 都是在3秒内来的。这个时候，匹配上规则的是B和C。
 *
 * 非严格邻近：表示在当前数据流中，所匹配的数据可以不用一前一后，中间允许有其他数据。那么这个时候，匹配上规则的数据就有两组，分别是A和C  以及  B和C。
 *
 * followedByAny 和 followedBy的区别：
 *
 * 模式为begin("first").where(_.name='a').followedBy("second").where(.name='b')
 *
 * 当数据为 a,c,b,b的时候，followedBy输出的模式是a,b   而followedByAny输出的模式是{a,b},{a,b}两组。
 *
 */
