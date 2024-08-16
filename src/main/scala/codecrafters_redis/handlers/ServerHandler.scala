package codecrafters_redis.handlers

import codecrafters_redis.utils.{RedisType, RespParser}

import java.io.InputStream
import java.net.{Socket, SocketException}
import java.text.ParseException
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Success, Try}

object ServerHandler {
  private def parseArguments(stream: InputStream): Try[(Array[String], Int)] = Try {
    val (input, bytesRead) = RespParser.decode(stream).get
    (input match {
      case RedisType.Array(array) => array.map {
        case RedisType.BulkString(string) => string.get
        case _ => throw new ParseException("commands should be an array of bulk strings", 0)
      }
      case _ => throw new ParseException("commands should be an array of bulk strings", 0)
    }, bytesRead)
  }

  def socketHandler(socket: Socket): Unit = {
    val in = socket.getInputStream
    val out = socket.getOutputStream
    while (!socket.isClosed) {
      breakable {
        val argumentsTry = parseArguments(in)
        val (arguments, bytesRead) = argumentsTry match {
          case Success(value) => value
          case Failure(exception) =>
            if (socket.isClosed) break
            try {
              out.write(RespParser.encodeSimpleError(exception.getMessage).getBytes)
            } catch {
              case _: SocketException => socket.close()
            }
            break
        }
        out.write(RespParser.encode(handleCommand(arguments)).getBytes)
      }
    }
  }

  private def handleCommand(arguments: Array[String]): RedisType = {
    arguments.head.toLowerCase match {
      case "ping" => Commands.ping()
      case "echo" => Commands.echo(arguments.drop(1))
      case command => RedisType.SimpleError(s"Error, unsupported command: $command")
    }
  }
}
