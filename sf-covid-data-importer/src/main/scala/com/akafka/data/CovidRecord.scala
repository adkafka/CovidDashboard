package com.akafka.data

import akka.http.scaladsl.model.DateTime
import com.akafka.data.CovidRecord.EpochMs
import io.circe.{Decoder, DecodingFailure, HCursor}
import io.circe.Decoder.Result
import io.circe.generic.extras.{Configuration, ConfiguredJsonCodec}

object CovidRecord {
  type EpochMs = Long

  def dateToEpochMs(date: String, cursor: HCursor): Result[EpochMs] = {
    DateTime.fromIsoDateTimeString(date + "Z")
      .map(_.clicks)
      .toRight(DecodingFailure(s"Bad date format on $date", cursor.history))
  }

  implicit val epochMsDecoder: Decoder[EpochMs] = Decoder.instance { cursor =>
    cursor.as[String].flatMap(dateToEpochMs(_, cursor))
  }

  implicit val config: Configuration = Configuration.default.withSnakeCaseMemberNames
}

@ConfiguredJsonCodec  final case class CovidRecord(
  specimenCollectionDate: EpochMs,
  caseDisposition: String,
  transmissionCategory: String,
  caseCount: String,
  lastUpdatedAt: String
)
