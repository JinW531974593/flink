package com.liwushuo.transmation

import org.apache.flink.streaming.api.scala._

object Demo01 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //1map

//    val stream = env.generateSequence(1,10)
//
//    val mapStream = stream.map(t=>t*2)
//
//    mapStream.print()

    //2flatMap

//    val stream = env.readTextFile("input/test01.txt")
//
//    stream.flatMap(_.split(" ")).map(t=>(t,1)).print()

    //3 connect
//    val stream01 = env.generateSequence(1,10)
//
//    val stream02 = env.readTextFile("input/test00.txt").flatMap(t=>t.split(" "))
//
//    val connectStream = stream01.connect(stream02)
//
//    val value = connectStream.map(t=>t*2,t=>(t,1L))
//
//    value.print()


    //4 split
    //将流分割为多个
    val stream = env.readTextFile("input/test00.txt")

    val flatMapStream = stream.flatMap(_.split(" "))

    val splitStream = flatMapStream.split(
      word =>
        ("hadoop".equals(word)) match {
          case true => List("hadoop")
          case _ => List("other")
        }
    )


    //根据名字获取相应的流
    val selectStream01 = splitStream.select("hadoop")
    val selectStream02 =splitStream.select("other")

    selectStream02.print()

    env.execute(this.getClass.getSimpleName)
  }
}
