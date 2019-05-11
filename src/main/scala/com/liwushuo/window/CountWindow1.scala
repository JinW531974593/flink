package com.liwushuo.window

import org.apache.flink.streaming.api.scala._

object CountWindow1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("hadoop102",11111)


    val streamKeyBy = stream.map(t=>(t,1L)).keyBy(0)

    val result = streamKeyBy.countWindow(4).reduce((t1,t2)=>(t1._1,t1._2+t2._2))

    result.print()
    env.execute(this.getClass.getSimpleName)
  }
}
