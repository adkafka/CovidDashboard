package com.akafka.transformation

import com.akafka.data.CovidRecord

import scala.util.Try

object SfCovid {
  val CasesMeasurement = "cases"
  val DeathsMeasurement = "deaths"

  def toInfluxProtocol(records: List[CovidRecord]): Either[String, List[String]] = {
    import cats.implicits._
    records.traverse(toInfluxProtocol)
  }

  /** Returns an error string if can't translate */
  def toInfluxProtocol(record: CovidRecord): Either[String, String] = {
    val measurement = record.caseDisposition match {
      case "Confirmed" => Right(CasesMeasurement)
      case "Death" => Right(DeathsMeasurement)
    }

    val caseCount = Try(record.caseCount.toInt).toEither.left.map(_.toString)
    val lastUpdated = "\"" + record.lastUpdatedAt + "\""
    val transmission = record.transmissionCategory.replaceAll("\\s","")

    for {
      m <- measurement
      c <- caseCount
    } yield s"$m,transmission_category=$transmission case_count=$c,last_updated=$lastUpdated ${record.specimenCollectionDate}"
  }
}
