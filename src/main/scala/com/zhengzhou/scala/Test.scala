package com.zhengzhou.scala


object Test {


    def main(args: Array[String]): Unit = {
        println("hello world")

        val helloActor = new HelloActor
        helloActor.start()

        helloActor ! "leo"
    }


}


