package com.liwushuo.transmation

import org.apache.flink.streaming.api.scala._

object  Demo02 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.readTextFile("input/test00.txt").flatMap(_.split(" ")).map(t=>(t,1L))

    val keyByStream = stream.keyBy(0)

    val reduceStream = keyByStream.reduce((t1,t2)=>(t1._1,t1._2+t2._2))

    reduceStream.print()

    env.execute(this.getClass.getSimpleName)
  }
}
