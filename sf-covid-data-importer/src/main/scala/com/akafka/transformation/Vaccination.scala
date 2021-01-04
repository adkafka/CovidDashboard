package com.akafka.transformation

import com.akafka.data.VaccinationRecord

object Vaccination {
  def toInfluxProtocol(records: List[VaccinationRecord]): Either[String, List[String]] = {
    import cats.implicits._
    records.traverse(toInfluxProtocol)
  }

  /** Returns an error string if can't translate */
  def toInfluxProtocol(record: VaccinationRecord): Either[String, String] = {
    val location = record.location.replaceAll(" ", "_")
    val fields =
      record.totalVaccinations.map(x => s"total_vaccinations=$x,") ::
      record.dailyVaccinations.map(x => s"daily_vaccinations=$x,") ::
      record.totalVaccinationsPerHundred.map(x => s"total_vaccinations_per_hundred=$x,") ::
      record.dailyVaccinationsPerMillion.map(x => s"daily_vaccinations_per_million=$x,") :: Nil

    if (fields.forall(_.isEmpty)) {
      Left("All optional fields omitted")
    }
    else {
      val fieldsStr = fields.map(_.getOrElse("")).reduce(_ + _).dropRight(1) // drop trailing ,
      Right(s"vaccinations,location=$location $fieldsStr ${record.epochMs}")
    }
  }
}
