package com.liwushuo.flink

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object SocketDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    val socketDS = env.socketTextStream("hadoop102",11111)

    val socketDS = env.generateSequence(1,10)
    socketDS.print()

    env.execute("SecondDemo")

  }
}
