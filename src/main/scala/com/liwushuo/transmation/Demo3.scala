package com.liwushuo.transmation

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object Demo3 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream01 = env.readTextFile("input/test01.txt")

    val stream02 = env.readTextFile("input/test02.txt")


    val stream = stream01.union(stream02)

    stream.print()

    env.execute(this.getClass.getSimpleName)
  }
}
