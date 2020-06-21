package com.akafka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cats.data._
import cats.implicits._
import com.akafka.source.sf.Reader
import com.akafka.transformation.SfCovid

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object SfDataImporter extends App {
  implicit val sys: ActorSystem = ActorSystem("sf-data-importer")
  implicit val ec: ExecutionContext = sys.dispatcher
  implicit val mat: ActorMaterializer = ActorMaterializer()

  val client = new influx.Writer()

  val result = for {
    data <- EitherT(Reader.getData(url = "https://data.sfgov.org/resource/tvq9-ec9w.json")).leftMap(_.toString)
    transformed <- EitherT.fromEither[Future](SfCovid.toInfluxProtocol(data))
    write <- EitherT.right[String](client.write(transformed))
  } yield (data, transformed, write)

  result.value.onComplete {
    case Success(Right((data, _, write))) => println(s"Done! Wrote ${data.length} points. Got ${write}.")
    case Success(Left(err)) => println(s"ERROR : $err")
    case Failure(exception) => println(s"EXCEPTION : $exception")
  }
}
