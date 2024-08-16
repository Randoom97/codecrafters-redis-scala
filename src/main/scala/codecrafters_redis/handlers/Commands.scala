package codecrafters_redis.handlers

import codecrafters_redis.utils.RedisType

object Commands {
  def ping(): RedisType = RedisType.SimpleString("PONG")

  def echo(arguments: Array[String]): RedisType = {
    if (arguments.length != 1)
      RedisType.SimpleError("incorrect argument count for echo")
    else
      RedisType.BulkString(Some(arguments.head))
  }
}