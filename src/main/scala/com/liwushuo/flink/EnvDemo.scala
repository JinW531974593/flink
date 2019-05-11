package com.liwushuo.flink

import org.apache.flink.streaming.api.scala._

object EnvDemo {
  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dss = env.readTextFile("input/")

    dss.print()

    env.execute("FirstDemo")
  }
}
