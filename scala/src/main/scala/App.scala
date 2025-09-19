
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.Materializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import spray.json._

import java.time.Instant
import java.util.Properties
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

// JSON handling using spray-json
case class ProduceRequest(level: Option[String], text: Option[String], key: Option[String])
object JsonFormats extends DefaultJsonProtocol {
  implicit val produceRequestFormat = jsonFormat3(ProduceRequest)
}

object App extends App {
  import JsonFormats._

  implicit val system: ActorSystem = ActorSystem("scala-kafka-producer")
  implicit val materializer: Materializer = Materializer(system)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  // Load environment variables
  val kafkaBrokers = sys.env.getOrElse("KAFKA_BROKERS", "localhost:9094")
  val kafkaTopic = sys.env.getOrElse("KAFKA_TOPIC", "logs")

  // Kafka producer setup
  val props = new Properties()
  props.put("bootstrap.servers", kafkaBrokers)
  props.put("key.serializer", classOf[StringSerializer].getName)
  props.put("value.serializer", classOf[StringSerializer].getName)

  val producer = new KafkaProducer[String, String](props)

  // Akka HTTP Routes
  val route =
    pathSingleSlash {
      get {
        getFromFile("public/index.html")
      }
    } ~
    path("produce") {
      post {
        entity(as[String]) { body =>
          val json = body.parseJson.convertTo[ProduceRequest]
          val level = json.level.getOrElse("INFO")
          val text = json.text.getOrElse("Sample event from Web UI")
          val key = json.key.getOrElse("web")
          val timestamp = Instant.now().toString
          val line = s"$timestamp, $level, $text"

          val record = new ProducerRecord[String, String](kafkaTopic, key, line)

          val sendFuture: Future[RecordMetadata] = Future {
            producer.send(record).get()
          }

          onComplete(sendFuture) {
            case Success(metadata) =>
              complete(HttpEntity(ContentTypes.`application/json`, s"""{"ok":true, "sent":"$line"}"""))
            case Failure(exception) =>
              complete(StatusCodes.InternalServerError, s"""{"ok":false, "error":"${exception.getMessage}"}""")
          }
        }
      }
    } ~
    pathPrefix("") {
      getFromDirectory("public")
    }

  val bindingFuture = Http().newServerAt("0.0.0.0", 3000).bind(route)

  println("Server online at http://localhost:3000/")
}
