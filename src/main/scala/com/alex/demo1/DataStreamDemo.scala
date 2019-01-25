package com.alex.demo1

import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

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
    val stream: DataStreamSource[String] = env.readTextFile("test00.txt")

    //读取socket
    val stream2: DataStreamSource[String] = env.socketTextStream("hadoop102",4444)


    stream.print()
    stream2.print()

    //触发程序执行
    env.execute("demo1")


  }

}
