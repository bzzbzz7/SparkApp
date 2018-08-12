package com.zhengzhou.scala


import scala.actors.Actor

class HelloActor extends Actor {
    def act() {
        while (true) {
            receive {
                case name: String => println("hello," + name)
            }
        }
    }
}
