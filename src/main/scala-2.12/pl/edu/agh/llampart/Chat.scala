package pl.edu.agh.llampart

import java.io.{InputStream, OutputStream}

import org.jgroups.protocols.UDP
import org.jgroups.{JChannel, Message, ReceiverAdapter, View}
import pl.edu.agh.dsrg.sr.chat.protos.ChatOperationProtos.ChatAction.ActionType
import pl.edu.agh.dsrg.sr.chat.protos.ChatOperationProtos.{ChatAction, ChatState}

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag

class Chat(userName: String) extends ReceiverAdapter with StrictLogging[Chat] {

  override protected implicit def classTag: ClassTag[Chat] = ClassTag(classOf[Chat])

  private val chatManagementChannel = "ChatManagement321321"

  override protected val id: String = "GENERAL"

  private var selectedRoom: String = ""
  private val rooms = TrieMap.empty[String, Room]

  private def users = MSet.empty[String]

  private val channel = new JChannel(false).setup { c =>
    val stack = defaultProtocolStack(new UDP)
    c.setName(userName)
    c.setReceiver(this)
    c.setProtocolStackAndInit(stack)
  }

  def joinRoom(roomId: String): Unit = {
    val room = rooms.getOrElseUpdate(roomId, new Room(roomId, userName, onRoomReceive))
    room.connect()
    logger.info(s"Connected to room $roomId")
    val message = buildMessage(ActionType.JOIN, roomId)
    channel.send(message)
  }

  def switchOrJoin(roomId: String): Room = {
    lazy val newRoom = new Room(roomId, userName, onRoomReceive)
    val roomOpt = rooms.putIfAbsent(roomId, newRoom)
    if (roomOpt.isEmpty) {
      newRoom.connect()
      logger.info(s"Connected to room $roomId")
      val message = buildMessage(ActionType.JOIN, roomId)
      channel.send(message)
      selectedRoom = roomId
    }
    for (room <- roomOpt) room.connect()
    roomOpt.getOrElse(newRoom)
  }


  def leaveRoom(roomId: String): Unit = {
    rooms.get(roomId).foreach{ r =>
      r.disconnect()
      logger.info(s"Leaved to room $roomId")
    }
    val message = buildMessage(ActionType.LEAVE, roomId)
    channel.send(message)
    selectedRoom = ""
  }

  override def viewAccepted(view: View): Unit = {
    val members = view.getMembers.asScala.map(channel.getName).toSet
    logger.info(s"View has changed: $members")
    val removed = users diff members
    for {
      user <- removed
      room <- rooms.valuesIterator
    } room.removeUser(user)
    users --= removed
    users ++= members
  }

  override def getState(output: OutputStream): Unit = ChatState
    .newBuilder().addAllState {
    rooms.iterator.flatMap {
      case (n, room) => room.users.map(n -> _)
    }.map {
      case (r, u) =>
        ChatAction.newBuilder().setAction(ActionType.JOIN).setChannel(r).setNickname(u).build()
    }.toIterable.asJava
  }.build().writeTo(output)

  import ActionType._

  override def receive(msg: Message): Unit = ChatAction.parseFrom(msg.getBuffer) match {
    case ChatActionExt(Some(JOIN), Some(ch), Some(n)) =>
      val room = rooms.getOrElseUpdate(ch, new Room(ch, n, onRoomReceive))
      room.addUser(n)
    case ChatActionExt(Some(LEAVE), Some(ch), Some(n)) =>
      rooms.get(ch).foreach(_.removeUser(n))
    case _ =>
      logger.error("Unknown format of message!")
  }

  override def setState(input: InputStream): Unit = ChatState.parseFrom(input) match {
    case ChatStateExt(elems@_*) =>
      elems.groupBy(_.getChannel).iterator
        .map {
          case (ch, actions) => ch -> actions.map(_.getNickname)
        }.foreach {
        case (ch, users) =>
          rooms.put(ch, new Room(ch, userName, onRoomReceive, users.to[MSet]))
      }
  }

  def onRoomReceive(roomId: String)(src: String, msg: String): Unit = if (roomId == selectedRoom) {
      println(s"$src: $msg")
  }


  def close(): Unit = {
    rooms.values.foreach(_.close())
    channel.close()
  }

  private def buildMessage(actionType: ActionType, roomId: String): Message = {
    val action = ChatAction.newBuilder().setAction(actionType).setNickname(userName).setChannel(roomId).build()
    new Message(null, action.toByteArray)
  }

  def connect(): Unit = {
    channel.connect(chatManagementChannel)
  }
}

object Chat extends App {

  System.setProperty("java.net.preferIPv4Stack", "true")
  val name = io.StdIn.readLine("Type your name:\n")
  val chat = new Chat(name)
  chat.connect()
  private val helpMessage =
    """Welcome to jgroup chat example, possible commands:
j <identifier> - join room with identifier
e <identifier> - exit room with identifier
r - list all open rooms
s <identifier> - switch view to the room with identifier (possibly joining it)
l - list users of current room
l a - list all users in chat
p <message> - post message in current room (can't send in general)
h - display this message
q - quit chat
    """
  println(
    helpMessage.stripMargin)
  Iterator.iterate(Option.empty[String]) { cur =>
    io.StdIn.readLine().split(" ") match {
      case Array("j", joinId) =>
        chat.joinRoom(joinId)
        cur
      case Array("e", leaveId) =>
        chat.leaveRoom(leaveId)
        for (roomId <- cur if roomId != leaveId) yield roomId
      case Array("r") =>
        println(chat.rooms.keys.mkString("\n"))
        cur
      case Array("s", switchId) =>
        val room = chat.switchOrJoin(switchId)
        println(room.messages.takeRight(50).map(x => s"${x._1}: ${x._2}").mkString("\n"))
        switchId.option
      case Array("l") =>
        for (roomId <- cur) {
          println(chat.rooms(roomId).users.mkString("Users:\n", "\n", ""))
        }
        cur
      case Array("l", "a") =>
        println(chat.users.mkString("Users:\n", "\n", ""))
        cur
      case Array("p", message@_*) =>
        for (roomId <- cur) {
          chat.rooms(roomId).send(message.mkString(" "))
        }
        cur
      case Array("q") =>
        chat.close()
        println("Shutting down!")
        sys.exit()
        None
      case Array("h") =>
        println(helpMessage)
        cur
      case _ =>
        println("Unknown command!")
        cur
    }
  }.foreach(_ => ())


}
