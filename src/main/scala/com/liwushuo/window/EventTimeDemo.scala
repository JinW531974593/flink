package com.liwushuo.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object EventTimeDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //修改为eventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //告诉系统从何处获取eventTime
    val stream = env.socketTextStream("hadoop102",11111).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(0)) {  //延时时长
      override def extractTimestamp(t: String): Long = {
        //进来的信息格式:eventTime word
        val eventTime = t.split(" ")(0).toLong
        println(eventTime)
        eventTime
      }
    }).map(t=>(t.split(" ")(1),1L)).keyBy(0)

//    //滚动窗口
//
//    val windowStream = stream.window(TumblingEventTimeWindows.of(Time.seconds(6)))
//
//
//    val reduceStream = windowStream.reduce((t1,t2)=>(t1._1,t1._2+t2._2))
//
//    reduceStream.print()

    stream.window(EventTimeSessionWindows.withGap(Time.seconds(5)))
    env.execute(this.getClass.getSimpleName)
  }
}
