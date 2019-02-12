package com.alex.demo1

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._


object DataStreamDemo {
  def main(args: Array[String]): Unit = {

//    	获得一个执行环境；（Execution Environment）
//    	加载/创建初始数据；（Source）
//    	指定转换这些数据；（Transformation）
//    	指定放置计算结果的位置；（Sink）
//    	触发程序执行。

    //获取环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //source 读取文本
    val stream= env.readTextFile("test00.txt")

    val stream2= env.readTextFile("test01.txt")
//
//    //读取socket
//    val stream2 = env.socketTextStream("hadoop102",4444)

//    val list = List(1,2,3,4)

//   val stream: DataStream[Int] = env.fromCollection(list)
//    val stream2: DataStream[List[Int]] = env.fromElements(list)
//    val stream3: DataStream[Long] = env.generateSequence(1,100000000)


//    val splitWords: DataStream[String] = stream.flatMap(line=> line.split("\\s+"))
//    val wordsLength: DataStream[String] = splitWords.filter(words=> words.length>6)


//    val con: ConnectedStreams[String, String] = stream.connect(stream2)
//    con.map(x=>println(x),y=>println(y))
    val words: DataStream[String] = stream.flatMap(line=> line.split("\\s+"))
    val wordsforcount: DataStream[(String, Int)] = words.map(words=>(words,1))

    val wordsBykey: KeyedStream[(String, Int), Tuple] = wordsforcount.keyBy(0)

    val count: DataStream[(String, Int)] = wordsBykey.reduce( (a1,a2)=> (a1._1,a1._2+a2._2))
    count.print()




    //触发程序执行
    env.execute("demo1")


  }

}
