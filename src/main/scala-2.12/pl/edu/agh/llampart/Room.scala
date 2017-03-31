package pl.edu.agh.llampart

import java.net.InetAddress

import org.jgroups.protocols.UDP
import org.jgroups.{JChannel, Message, ReceiverAdapter}
import pl.edu.agh.dsrg.sr.chat.protos.ChatOperationProtos.ChatMessage

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class Room(protected val id: String, nick: String, onReceive: String => (String, String) => Unit, val users: MSet[String] = MSet.empty) extends ReceiverAdapter with StrictLogging[Room] {
  override protected implicit def classTag: ClassTag[Room] = ClassTag(classOf[Room])

  val messages: ListBuffer[(String, String)] = ListBuffer.empty[(String, String)]
  private val channel = new JChannel(false).setup { c =>
    c.setName(nick)
    c.setReceiver(this)
    val udp = new UDP().setValue("mcast_group_addr", InetAddress.getByName(id))
    c.setProtocolStackAndInit(defaultProtocolStack(udp))
  }

  def addUser(userName: String): Unit = {
    users += userName
    logger.info(s"User $userName joined the channel")
  }

  def removeUser(userName: String): Unit = {
    if (users.remove(nick)) {
      logger.info(s"USer $userName left the channel")
    }
  }

  def send(message: String): Unit = {
    val msg = ChatMessage.newBuilder().setMessage(message).build()
    channel.send(new Message(null, msg.toByteArray))
  }

  override def receive(msg: Message): Unit = {
    val MessageExt(messageString, srcAddress) = msg
    val src = channel.getName(srcAddress)
    messages += src -> messageString
    onReceive(id)(src, messageString)
  }

  def disconnect(): Unit = channel.disconnect()

  def connect(): Unit = channel.connect(id, null, 0)

  def close(): Unit = channel.close()

}
