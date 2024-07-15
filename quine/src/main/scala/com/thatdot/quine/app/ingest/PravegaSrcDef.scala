package com.thatdot.quine.app.ingest

import cats.data.Validated
import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.app.ingest.serialization.ImportFormat
import com.thatdot.quine.app.ingest.serialization.ContentDecoder
import com.thatdot.quine.util.SwitchMode
import com.thatdot.quine.graph.CypherOpsGraph

import org.apache.pekko.stream.connectors.pravega.ReaderSettings
import org.apache.pekko.stream.connectors.pravega.scaladsl.Pravega
import scala.util.Using
import io.pravega.client.admin.ReaderGroupManager
import com.thatdot.quine.app.ShutdownSwitch
import com.thatdot.quine.graph.cypher.Value
import org.apache.pekko.stream.scaladsl.Source
import scala.util.Try
import org.apache.pekko.Done
import scala.concurrent.Future

import cats.implicits.catsSyntaxTry
import org.apache.pekko.stream.KillSwitches
import com.thatdot.quine.app.PekkoKillSwitch
import org.apache.pekko.stream.UniqueKillSwitch
import org.apache.pekko.stream.scaladsl.Keep

object PravegaSrcDef {
  def apply[A](
    name: String,
    intoNamespace: NamespaceId,
    scope: String,
    readerGroupName: String,
    readerSettings: ReaderSettings[A],
    groupId: String,
    format: ImportFormat,
    initialSwitchMode: SwitchMode,
    parallelism: Int = 2,
    decoders: Seq[ContentDecoder]
  )(implicit graph: CypherOpsGraph): Validated[Throwable, IngestSrcDef] =
    Using(ReaderGroupManager.withScope(scope, readerSettings.clientConfig)) { readerGroupManager =>
      readerGroupManager.getReaderGroup(readerGroupName)
    }.map { readerGroup =>
      val source: Source[A, Future[Done]] = Pravega.source(readerGroup, readerSettings).map(_.message)
      new PravegaSrcDef(
        name,
        intoNamespace,
        source,
        readerSettings,
        format,
        initialSwitchMode,
        parallelism,
        None,
        decoders
      )
    }.toValidated

}

class PravegaSrcDef[A](
  override val name: String,
  val intoNamespace: NamespaceId,
  source: Source[A, Future[Done]],
  readerSettings: ReaderSettings[A],
  format: ImportFormat,
  initialSwitchMode: SwitchMode,
  parallelism: Int,
  maxPerSecond: Option[Int],
  decoders: Seq[ContentDecoder]
)(implicit graph: CypherOpsGraph)
    extends IngestSrcDef(format, initialSwitchMode, parallelism, maxPerSecond, s"$name (Pravega ingest)") {

  type InputType = A
  override def sourceWithShutdown(): Source[(Try[Value], InputType), ShutdownSwitch] =
    source
      .wireTap(o => meter.mark(o.toString().size))
      .viaMat(KillSwitches.single)(Keep.right)
      .mapMaterializedValue(PekkoKillSwitch.apply)
      .map(a => (format.importMessageSafeBytes(readerSettings.serializer.serialize(a).array, false), a))

}
