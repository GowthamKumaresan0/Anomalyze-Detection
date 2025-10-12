// App.scala
// sbt deps (example):
// "com.typesafe.akka" %% "akka-actor-typed" % "2.9.5"
// "com.typesafe.akka" %% "akka-stream" % "2.9.5"
// "com.typesafe.akka" %% "akka-http" % "10.6.3"
// "io.spray"         %% "spray-json" % "1.3.6"
// "org.apache.kafka"  % "kafka-clients" % "3.7.0"

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.Materializer
import akka.http.scaladsl.model.headers.RawHeader
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import spray.json._

import java.time.Instant
import java.util.Properties
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

// --------- Models ----------
case class ProduceRequest(level: Option[String], text: Option[String], key: Option[String])
case class RawLog(events: List[String], label: String, template: String, template_id: Int, timestamp: String)
case class LogEntry(
  timestamp: String,
  raw_log: RawLog,
  template: String,
  score: Double,
  is_anomaly: Boolean,
  seq_len: Int
)

// --------- JSON ----------
object JsonFormats extends DefaultJsonProtocol {
  implicit val produceRequestFormat = jsonFormat3(ProduceRequest)
  implicit val rawLogFormat        = jsonFormat5(RawLog)
  implicit val logEntryFormat      = jsonFormat6(LogEntry)
}

object App extends App {
  import JsonFormats._

  implicit val system: ActorSystem = ActorSystem("scala-kafka-producer")
  implicit val materializer: Materializer = Materializer(system)
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  val kafkaBrokers = sys.env.getOrElse("KAFKA_BROKERS", "localhost:9094")
  val kafkaTopic   = sys.env.getOrElse("KAFKA_TOPIC", "logs")

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
  props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")

  val producer = new KafkaProducer[String, String](props)

  val cors = List(
    RawHeader("Access-Control-Allow-Origin", "*"),
    RawHeader("Access-Control-Allow-Headers", "Content-Type, Authorization"),
    RawHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
  )

  val route =
    respondWithHeaders(cors) {
      concat(
        options { complete(HttpResponse(StatusCodes.OK)) },

        path("produce") {
          post {
            entity(as[String]) { body =>
              val js = body.parseJson.asJsObject
              val req = js.convertTo[ProduceRequest]
              val level = req.level.getOrElse("INFO")

              // Prefer explicit `label` from the body; else derive from `level`
              val labelFromBody: Option[String] =
                js.fields.get("label").collect { case JsString(s) if s.trim.nonEmpty => s.trim }

              val label: String =
                labelFromBody.getOrElse {
                  if (level.equalsIgnoreCase("ERROR")) "Error" else "Success"
                }

              // Prefer explicit `events: string[]` from the body; else use `text` or a default
              val eventsFromBody: List[String] =
                js.fields.get("events") match {
                  case Some(JsArray(arr)) => arr.collect { case JsString(s) => s.trim }.filter(_.nonEmpty).toList
                  case _                  => Nil
                }

              val events: List[String] =
                if (eventsFromBody.nonEmpty) eventsFromBody
                else List(req.text.getOrElse("Sample event from Web UI"))

              val key = req.key.getOrElse("web")
              val now = Instant.now().toString

              val rawLog = RawLog(
                events = events,
                label = label,
                template = "",
                template_id = -1,
                timestamp = now
              )

              val entry = LogEntry(
                timestamp = now,
                raw_log = rawLog,
                template = "",
                score = 0.0,
                is_anomaly = false,
                seq_len = 1
              )

              val jsonStr = entry.toJson.compactPrint
              val record = new ProducerRecord[String, String](kafkaTopic, key, jsonStr)
              val sendF: Future[RecordMetadata] = Future { producer.send(record).get() }

              onComplete(sendF) {
                case Success(_) =>
                  complete(HttpEntity(ContentTypes.`application/json`, s"""{"ok":true,"sent":$jsonStr}"""))
                case Failure(ex) =>
                  complete(HttpResponse(
                    status = StatusCodes.InternalServerError,
                    entity = HttpEntity(ContentTypes.`application/json`,
                      s"""{"ok":false,"error":${JsString(ex.getMessage).compactPrint}}""")
                  ))
              }
            }
          }
        },

        // Serve static files from external public/ directory
        pathSingleSlash {
          get {
            getFromFile("public/index.html")
          }
        },
        path("style.css") {
          get {
            getFromFile("public/style.css")
          }
        },
        path("app.js") {
          get {
            getFromFile("public/app.js")
          }
        },
        pathPrefix("") {
          getFromDirectory("public")
        }
      )
    }

  val binding = Http().newServerAt("0.0.0.0", 3000).bind(route)
  println("Server online at http://localhost:3000/")

  sys.addShutdownHook {
    println("Shutting down...")
    producer.flush(); producer.close()
    binding.flatMap(_.terminate(hardDeadline = scala.concurrent.duration.Duration(5, "seconds")))
      .onComplete(_ => system.terminate())
  }
}
