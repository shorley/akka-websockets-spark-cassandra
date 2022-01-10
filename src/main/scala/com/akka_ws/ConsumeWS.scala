package com.akka_ws

import akka._
import akka.actor._
import akka.http.scaladsl._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.ws._
import akka.stream.{ActorMaterializer, OverflowStrategy}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import spray.json._

import java.util.Properties
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

case class WSMsgSubRequest(action: String, subs: Seq[String])
//case class WSMsgSubRequestSuccess(TYPE: String, MESSAGE: String, SUB: String)
case class WSMsgStart(TYPE: String, MESSAGE: String, SERVER_UPTIME_SECONDS: Long, SERVER_TIME_MS: Long, CLIENT_ID:Long, SOCKET_ID: String, RATELIMIT_MAX_DAY:Long, RATELIMIT_REMAINING_DAY: Long, RATELIMIT_MAX_MINUTE: Long, RATELIMIT_REMAINING_MINUTE:Long)
case class WSMsgInfo(TYPE: String, MESSAGE: String, TIMEMS: Long)
case class WSMsgWrongApiKey(TYPE: String, MESSAGE: String, PARAMETER: String, INFO: String)
case class WSMsgTicker(TYPE: String, MARKET: String, FROMSYMBOL: String, TOSYMBOL: String, FLAGS: Long, PRICE: Double, LASTUPDATE: Long, LASTVOLUME: Double, VOLUMEDAY: Double)
case class WSMsgTrade(TYPE: String, M: String, FSYM: String, TSYM: String, F: String, ID: String, TS: Long, Q:Double, P: Double, TOTAL: Double, RTS: Long)


object ConsumeWSJsonProtocol extends DefaultJsonProtocol {
  implicit val wsmsgreqFormat = jsonFormat2(WSMsgSubRequest)
  implicit val wsmsgstartFormat = jsonFormat10(WSMsgStart)
  implicit val wsmsginfoFormat = jsonFormat3(WSMsgInfo)
  implicit val wmsgtickerFormat = jsonFormat9(WSMsgTicker)
  implicit val wmsgtradeFormat = jsonFormat11(WSMsgTrade)
  implicit val wmsgwrongapikeyFormat = jsonFormat4(WSMsgWrongApiKey)

}


object ConsumeWS {
  val logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("ConsumeWS")
  implicit val materializer = ActorMaterializer() //ActorMaterializer.create(system)

  import ConsumeWSJsonProtocol._
  import system.dispatcher

  var kafkaProducer: Option[KafkaProducer[String, String]] = None

  def processWSJsonMsg(json: String, actorref: ActorRef, streamerDFActor: ActorRef, timeout: Int) = {
    def asWsMsgStart(json: String) = json.parseJson.convertTo[WSMsgStart]
    def asWsMsgInfo(json: String) = json.parseJson.convertTo[WSMsgInfo]
    def asWsMsgTicker(json: String) = json.parseJson.convertTo[WSMsgTicker]
    def asWsMsgTrade(json: String) = json.parseJson.convertTo[WSMsgTrade]
    def asWsMsgWrongapikey(json: String) = json.parseJson.convertTo[WSMsgWrongApiKey]

    try {
      Try[Any](asWsMsgStart(json)).orElse(
        Try(asWsMsgInfo(json))).orElse(
        Try(asWsMsgTrade(json))).orElse(
        Try(asWsMsgTicker(json))).orElse(
        Try(asWsMsgWrongapikey(json))) match {
        case Success(req: WSMsgStart) =>
          logger.info(s"${req}")
          logger.info(s">>>> Subscribing to TradeMsgs for : $timeout secs")
          //Send a msg to start our subscription here
          actorref ! WSMsgSubRequest("SubAdd", Seq("0~Coinbase~BTC~USD", "0~Binance~BTC~USDT", "0~Kraken~BTC~USD", "0~CoinDeal~BTC~USD", "0~Gemini~BTC~USD")).toJson.prettyPrint
          setScheduler(actorref, timeout)
        case Success(_: WSMsgInfo) =>
        case Success(_: WSMsgTicker) =>
        case Success(req: WSMsgTrade) =>
          logger.debug(s"$req")
          streamerDFActor ! req
        case Success(req: WSMsgWrongApiKey) =>
          val actor = system.actorOf(Props(new WSTimer(actorref)), name = "Scheduler-Terminate")
          logger.error(s"${req.MESSAGE}: ${req.INFO}")
          actor ! Done
        case Success(x) =>
          throw new IllegalArgumentException(s"Unknown request type: $x")
        case Failure(e) =>
          throw e
      }
    } catch {
      case e: DeserializationException =>
        logger.warn(s"Message $json does not conform with start request", e.msg)
      case e: JsonParser.ParsingException =>
        logger.warn(s"Handled invalid message $json", e.summary)
      case e: Throwable =>
        logger.error(s"Error occurred during handling message $json", e)
    }
  }

  def websocketFlow(streamerDFActor: ActorRef, timeout: Int) = {
    /*val completionMatcher: PartialFunction[Any, CompletionStrategy] = { case Status.Success =>  CompletionStrategy.immediately }
    val failureMatcher: PartialFunction[Any, Throwable]             = { case Status.Failure(cause)        => cause }*/

    import akka.stream.scaladsl._

    val (actorRef: ActorRef, publisher: Publisher[TextMessage.Strict]) =
      Source.actorRef[String](bufferSize = 16, overflowStrategy = OverflowStrategy.dropNew)
        .map(msg => TextMessage.Strict(msg))
        .toMat(Sink.asPublisher(false))(Keep.both)
        .run()

    val printSink: Sink[Message, Future[Done]] =
      Sink.foreach {
        case message: TextMessage.Strict => processWSJsonMsg(message.text, actorRef, streamerDFActor, timeout)
        case _ => // ignore other message types
      }



    Flow.fromSinkAndSource(printSink, Source.fromPublisher(publisher))
  }



  def setScheduler(actorRef: ActorRef, timeout: Int): Unit = {
    val actor = system.actorOf(Props(new WSTimer(actorRef)), name = "Scheduler")
    system.scheduler.scheduleOnce(timeout.seconds, actor, "TimerActor")
  }

  def startWebSocket(streamerDFActor: ActorRef, apikey: String, timeout: Int) = {
    logger.info(">>>>> starting websockets....")
    val extraHeaders = Seq(Authorization(BasicHttpCredentials("username", "password")))
    val uri = Uri(s"wss://streamer.cryptocompare.com/v2?api_key=$apikey")
    val (upgradeResponse, notused) =  Http().singleWebSocketRequest(WebSocketRequest(uri), websocketFlow(streamerDFActor, timeout))

    val connected = upgradeResponse.map { upgrade =>
      if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
        logger.info("connected successfully")
        Done
      } else {
        throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
      }
    }
  }

  def streamWebSocketToMemory(msgtradesrc: MemoryStream[WSMsgTrade], apikey: String, timeout: Int) = {
    val msgstreamerDFactor = system.actorOf(Props(new WSTradeMsgMemorySender(msgtradesrc)), name = "MemoryStreamerDFActor")
    startWebSocket(msgstreamerDFactor, apikey, timeout)
  }

  def streamWebSocketToKafka(kafkaBootstrapServer:String, kafkaTopic: String, apikey: String, timeout: Int) = {
    implicit val producer = getKafkaProducer(kafkaBootstrapServer)
    kafkaProducer = Some(producer)
    val msgstreamerDFactor = system.actorOf(Props(new WSTradeMsgKafkaSender(kafkaTopic)), name = "KafkaStreamerDFActor")
    startWebSocket(msgstreamerDFactor, apikey, timeout)
  }

  def getKafkaProducer(kafkaBootstrapServer: String) = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "WSTradeMsgProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    new KafkaProducer[String, String](props)
  }

  class WSTimer(actorRef: ActorRef) extends Actor with ActorLogging {
    override def receive = {
      case _ =>
        actorRef ! Status.Success
        system.terminate()
        println("terminated session")
        kafkaProducer match {
          case Some(kp) => kp.close()
          case _ =>
        }

    }
  }

  class WSTradeMsgMemorySender (msgtradesrc: MemoryStream[WSMsgTrade]) extends Actor{
    override def receive = {
      case w:WSMsgTrade => msgtradesrc.addData(w)
    }
  }

  class WSTradeMsgKafkaSender(kafkaTopic:String) (implicit val kafkaproducer: KafkaProducer[String, String]) extends Actor {
    override def receive = {
      case w:WSMsgTrade =>
        val record = new ProducerRecord[String, String](kafkaTopic, s"WSTradeMsg-${w.F}-${w.M}", s"${w.toJson}")
        kafkaproducer.send(record, new Callback {
            def onCompletion(metadata: RecordMetadata, exception: Exception) =
              if(exception != null) logger.error(s"Kafka Producer record $metadata failed sending... ${exception}")
          }
          )
        kafkaproducer.flush()
    }
  }
}

object SparkProcessMsgs{
  import org.apache.spark.sql.functions._

  def highestTxnsPerVolEvery60Secs(df: DataFrame): DataFrame = {
    //limit to only mode SELL ->1, BUY -> 2
    val df_repartition = df.coalesce(8)

    df_repartition.withColumn("TS", to_timestamp(from_unixtime(col("TS"))))
      .withColumn("Direction", when(col("F") === 1, "SELL").otherwise("BUY"))
      .filter(col("F").isin(Seq(1,2):_*))
      .withWatermark("TS", "5 minutes")
      .groupBy(col("M").as("Market"), col("Direction"), window(col("TS"), "60 seconds").as("window"))
      .agg(round(sum(col("TOTAL")), 8).as("TotalVol"), round(avg(col("P")), 8).as("AvgPrice"), count(col("ID")).as("TotalTrade"))
      .selectExpr("window.start as window_start", "window.end as window_end", "market", "direction", "totalvol", "avgprice", "totaltrade")
      /*.orderBy($"Window_Start", $"direction", $"TotalVol".desc)*/
  }

  def runProcessForMemory()(implicit spark: SparkSession, apikey: String, timeout: Int) = {
    import spark.implicits._
    implicit val sqlcontext = spark.sqlContext

    val msgtradesrc = MemoryStream[WSMsgTrade]
    val msgstream = msgtradesrc.toDF()
    val windowperiod = highestTxnsPerVolEvery60Secs(msgstream)

    val query = windowperiod.writeStream
      .option("checkpointLocation", "checkpoints")
      .format("console")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(15.seconds))
      .start()

    ConsumeWS.streamWebSocketToMemory(msgtradesrc, apikey, timeout)
    query.awaitTermination(timeoutMs=1000*60)
  }

  def wstradeschema() = Encoders.product[WSMsgTrade].schema

  def runProcessForKafka(kafkabootstrap: String, kafkatopic:String, cassandraurl: String)(implicit spark: SparkSession, apikey: String, timeout: Int)= {
    import spark.implicits._

    val kafkamsgstream= spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkabootstrap)
      .option("subscribe", kafkatopic)
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) AS wstradejson")
      .select(from_json($"wstradejson", wstradeschema).as("wstrade")) // composite column (struct)
      .selectExpr("wstrade.*")

    val windowperiod = highestTxnsPerVolEvery60Secs(kafkamsgstream)

    val query = windowperiod.writeStream
      .option("checkpointLocation", "checkpoint-kafka-cassandra")
      .foreachBatch((batch: DataFrame, batchid: Long) => {
        batch.write
          .option("spark.cassandra.connection.host", cassandraurl)
          .cassandraFormat("trademsgs1minutewindow", "cryptocompare")
          .mode(SaveMode.Append)
          .save()
      })
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(15.seconds))
      .start()

    ConsumeWS.streamWebSocketToKafka(kafkabootstrap, kafkatopic, apikey, timeout)

    query.awaitTermination(timeoutMs=1000*60)

  }

  val usage = """Usage: [--mode memory|kafka] [apikey]
                |Additional args for [--mode kafka]: [--kafkabroker localhost:9092] [--cassandraurl localhost:9042]
                |Optional: [--timeout 150] 'in seconds'
                |""".stripMargin

  def extractParams(args: Array[String]) = {
    def nextOption(argList:List[String], map:Map[String, String]) : Map[String, String] = {
      val patternmode         = "--(mode)".r
      val patternkafka        = "--(kafkabroker)".r
      val patternkafkatopic   = "--(kafkatopic)".r
      val patterncassandra    = "--(cassandraurl)".r
      val patterntimeout    = "--(timeout)".r
      val patterntimeoutv    = "(\\d{1,3})".r
      argList match {
        case Nil => map
        case patternmode(opt)         :: value  :: tail => nextOption( tail, map ++ Map(opt->value) )
        case patternkafka(opt)        :: value  :: tail => nextOption( tail, map ++ Map(opt->value) )
        case patternkafkatopic(opt)   :: value  :: tail => nextOption( tail, map ++ Map(opt->value) )
        case patterncassandra(opt)    :: value  :: tail => nextOption( tail, map ++ Map(opt->value) )
        case patterntimeout(opt)      :: value  :: tail if patterntimeoutv.pattern.matcher(value).matches => nextOption( tail, map ++ Map(opt->value) )
        case string                   :: Nil            => map ++ Map("apikey"->string)
        case option                   :: _              =>
          println("Unknown option:"+option)
          println(usage)
          sys.exit(1)
      }
    }
    nextOption(args.toList,Map[String, String]())
  }

  def main(args: Array[String]): Unit = {
    val extractedparammap = extractParams(args)
    if(!extractedparammap.contains("mode")) {
      println("missing parameter: --mode")
      println(usage)
      sys.exit(1)
    }else {
      val mode = extractedparammap("mode")
      if(mode != "memory"){
        if(!extractedparammap.contains("kafkabroker")) {
          println("missing parameter: --kafkabroker")
          println(usage)
          sys.exit(1)
        } else if(!extractedparammap.contains("kafkatopic")) {
          println("missing parameter: --kafkatopic")
          println(usage)
          sys.exit(1)
        } else if(!extractedparammap.contains("cassandraurl")) {
          println("missing parameter: --cassandraurl")
          println(usage)
          sys.exit(1)
        }
      }
    }

    if(!extractedparammap.contains("apikey")) {
      println("missing parameter: [apikey]")
      println(usage)
      sys.exit(1)
    }

    implicit val apikey:String = extractedparammap("apikey")
    val timeoutstr = extractedparammap.getOrElse("timeout", "150")
    implicit val timeout:Int = timeoutstr.toInt

    implicit val spark: SparkSession = SparkSession.builder
      .appName("Websocket-to-Spark-Streaming")
      /*.master("local[*]")*/
      .getOrCreate()


    spark.conf.set("spark.sql.shuffle.partitions",8)
    spark.sparkContext.setLogLevel("ERROR")

    println(s"running in: ${extractedparammap("mode")} mode")

    if(extractedparammap("mode")== "memory")
      runProcessForMemory()
    else {
      runProcessForKafka(extractedparammap("kafkabroker"), extractedparammap("kafkatopic"), extractedparammap("cassandraurl"))
    }


  }
}



