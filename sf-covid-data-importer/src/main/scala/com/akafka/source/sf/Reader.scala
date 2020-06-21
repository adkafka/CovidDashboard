package com.akafka.source.sf

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, Uri}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.akafka.data.CovidRecord
import io.circe
import io.circe.parser.decode

import scala.concurrent.{ExecutionContext, Future}

object Reader {
  def getData(url: String)(implicit mat: ActorMaterializer): Future[Either[circe.Error, List[CovidRecord]]] = {
    implicit val sys: ActorSystem = mat.system
    implicit val ec: ExecutionContext = mat.system.dispatcher

    val request = HttpRequest(uri = Uri(url))

    Source.fromFuture(Http().singleRequest(request))
      .flatMapConcat(_.entity.dataBytes)
      .reduce(_ ++ _)
      .map(_.utf8String)
      .map(decode[List[CovidRecord]])
      .runWith(Sink.head)
  }
}
