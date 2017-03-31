package pl.edu.agh
import org.jgroups.{Address, JChannel, Message}
import org.jgroups.protocols._
import org.jgroups.protocols.pbcast._
import org.jgroups.stack.{Protocol, ProtocolStack}
import org.slf4j.{Logger, LoggerFactory}
import pl.edu.agh.dsrg.sr.chat.protos.ChatOperationProtos.{ChatAction, ChatMessage, ChatState}
import pl.edu.agh.dsrg.sr.chat.protos.ChatOperationProtos.ChatAction.ActionType

import scala.collection.mutable.{Set => mSet}
import scala.reflect.ClassTag
import scala.collection.JavaConverters._
import scala.util.Try

package object llampart {
  //type aliases
  val MSet: mSet.type = mSet
  type MSet[A] = mSet[A]
  // logger utility

  trait StrictLogging[T] {
    protected def id: String
    protected implicit def classTag: ClassTag[T]
    private def getLogger(implicit ev: ClassTag[T]): Logger = LoggerFactory.getLogger(s"${ev.getClass.getSimpleName}_$id")
    protected val logger: Logger = getLogger
  }

  implicit class GenericExtensions[T](private val elem: T) extends AnyVal {
    @inline def setup(fun: T => Any): T = {
      fun(elem)
      elem
    }
    @inline def option: Option[T] = Option(elem)

    @inline def setProtocolStackAndInit(stack: ProtocolStack)(implicit ev: T =:= JChannel): Unit = {
      elem.setProtocolStack(stack)
      stack.init()
    }
  }

  object ChatActionExt {
    def unapply(arg: ChatAction): Option[(Option[ActionType], Option[String], Option[String])] = {
      val optType = arg.getAction.option
      val optChannel = arg.getChannel.option
      val optNickname = arg.getNickname.option
      (optType, optChannel, optNickname).option
    }
  }

  object MessageExt {
    def unapply(arg: Message): Option[(String, Address)] = Try(ChatMessage.parseFrom(arg.getBuffer))
      .toOption.flatMap(_.getMessage.option).zip(arg.getSrc.option).headOption
  }

  object ChatStateExt {
    def unapplySeq(arg: ChatState): Option[Seq[ChatAction]] = {
      arg.getStateList.asScala.option
    }
  }

  def defaultProtocolStack(udp: Protocol): ProtocolStack = {
    val stack =  new ProtocolStack()
    stack.addProtocol(udp)
      .addProtocol(new PING)
      .addProtocol(new MERGE3)
      .addProtocol(new FD_SOCK)
      .addProtocol(new FD_ALL().setValue("timeout", 12000).setValue("interval", 3000))
      .addProtocol(new VERIFY_SUSPECT)
      .addProtocol(new BARRIER)
      .addProtocol(new NAKACK2)
      .addProtocol(new UNICAST3)
      .addProtocol(new STABLE)
      .addProtocol(new GMS)
      .addProtocol(new UFC)
      .addProtocol(new MFC)
      .addProtocol(new FRAG2)
      .addProtocol(new STATE_TRANSFER)
      .addProtocol(new FLUSH)
    stack
  }

}
