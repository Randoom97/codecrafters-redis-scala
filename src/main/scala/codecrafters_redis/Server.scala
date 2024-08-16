package codecrafters_redis

import codecrafters_redis.handlers.ServerHandler

import java.net.{InetSocketAddress, ServerSocket}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


object Server {
  def main(args: Array[String]): Unit = {
    val serverSocket = new ServerSocket()
    serverSocket.bind(new InetSocketAddress("localhost", 6379))
    while (true) {
      val clientSocket = serverSocket.accept() // wait for client
      Future(ServerHandler.socketHandler(clientSocket))
    }
  }
}
