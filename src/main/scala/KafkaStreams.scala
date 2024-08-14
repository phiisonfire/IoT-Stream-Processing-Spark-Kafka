package de.phinguyen.kafkawithscala

import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._ // auto generate Encoder & Decoder for each case class to JSON String
import io.circe.parser._ // decode
import io.circe.syntax._ // extension methods: .asJson
// Scala objects --circe--> JSON Strings --java/scala--> Array[Byte]

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
// import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._

object KafkaStreams {

  object Domain {
    type UserId = String
    type Profile = String
    type Product = String
    type OrderId = String
    type Status = String

    case class Order(orderId: OrderId, userId: UserId, product: List[Product], amount: Double)
    case class Discount(profile: Profile, amount: Double)
    case class Payment(orderId: OrderId, status: Status)
  }

  object Topics {
    final val OrdersByUserTopic = "orders-by-user"
    final val DiscountProfilesByUserTopic = "discount-profiles-by-user"
    final val DiscountsTopic = "discounts"
    final val OrdersTopic = "orders"
    final val PaymentsTopic = "payments"
    final val PaidOrdersTopic = "paid-orders"
  }

  import Domain._
  import Topics._

  // Implicit parameters in Scala allow the compiler to automatically pass parameters to functions
  // or methods when they are not explicitly provided.
  // Scope: The implicit values or parameters must be in scope for them to be used.
  // They are selected based on type and availability.
  implicit def serde[A >: Null : Decoder : Encoder]: Serde[A] = { // >: Null : Decoder : Encoder = "context-bound"
    // this `implicit def` will apply for each scala object A that Circe can find an Encoder & Decoder of it
    // and import io.circe.generic.auto._ automatically creates Encoder & Decoder for each Case Class
    val serializer = (a: A) => a.asJson.noSpaces.getBytes()
    val deserializer = (bytes: Array[Byte]) => {
      val string = new String(bytes) // transform array of bytes into Java String
      decode[A](string).toOption // decode Java String into object of type A
      // decode[Order](string) tells the Circe library to decode JSON string into and instance of the `Order` class
      // decode function automatically finds `Implicit Decoder` for the `Order` type to perform this conversion
      // the `Implicit Decoder of case-class Order` is automatically implemented by io.circe.generic.auto._ under the hood
    }
    Serdes.fromFn[A](serializer, deserializer)
    // return Serdes instance for case class A with corresponding serializer and deserializer
    // Kafka can implicitly use this Serdes instance to automatically convert an object A to & from an array of bytes
  }

  // topology
  val builder = new StreamsBuilder()

  // Sources
  // KStream - is distributed
  val usersOrdersStream: KStream[UserId, Order] = builder.stream[UserId, Order](OrdersByUserTopic)
  // builder.stream[K, V](topic: String)(implicit consumed: kstream.Consumed[K,V])
  // `implicit def serde` function above created a `Serdes` for everything we have including `UserId` and `Order` here
  // `UserId` and `Order` are String so, Scala Kafka Stream's Consumed will use Serdes.stringSerde implicit object
  // org.apache.kafka.streams.scala.ImplicitConversions._ helps
  // converting those implicit Serdes[UserId], Serdes[Order] to kstream.Consumed[UserId, Order]

  // KTable - is distributed, events are kept in the table for a period of time (cleanup.policy=compact)
  val userProfilesTable: KTable[UserId, Profile] = builder.table[UserId, Profile](DiscountProfilesByUserTopic)
  // DiscountProfilesByUserTopic has --config "cleanup.policy=compact"

  // GlobalKTable - similar to KTable but broadcast into memory of each node of the cluster
  val discountProfileGTable: GlobalKTable[Profile, Discount] = builder.globalTable[Profile, Discount](DiscountsTopic)
  // DiscountsTopic has --config "cleanup.policy=compact"

  // 5. Streams Transformations
  // 5.1. Stateless transformations: executes only in memory
  // filter, map, flatMap
  // stateless terminal transformations: sinks - terminal node of stream topology
  val expensiveOrders = usersOrdersStream.filter { (k: UserId, v: Order) =>
    v.amount > 1000
  }
  // usersOrdersStream is streaming data from Kafka's topic OrdersByUserTopic
  // the key and value of messages in OrdersByUserTopic are UserId and Order, respectively

  // 2 examples of sink processors: `foreach` & `to`
  // 5.2. Stateful transformations: requires managing a state to perform



  builder.build()

}
