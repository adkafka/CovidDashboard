package com.akafka.source.owid.vaccinations

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, Uri}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.akafka.data.{CovidRecord, VaccinationRecord}
import io.circe

import scala.concurrent.{ExecutionContext, Future}

object Reader {
  def getData(url: String)(implicit mat: ActorMaterializer): Future[Either[String, List[VaccinationRecord]]] = {
    implicit val sys: ActorSystem = mat.system
    implicit val ec: ExecutionContext = mat.system.dispatcher

    val request = HttpRequest(uri = Uri(url))

    Source.fromFuture(Http().singleRequest(request))
      .flatMapConcat(_.entity.dataBytes)
      .reduce(_ ++ _)
      .map(_.utf8String)
      .map(VaccinationRecord.fromCsvRows)
      .runWith(Sink.head)
  }
}
