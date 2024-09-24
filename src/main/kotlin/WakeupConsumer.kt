import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*
import kotlin.concurrent.thread

class WakeupConsumer {

    companion object {
        private val logger = KotlinLogging.logger {}

        @JvmStatic
        fun main(args: Array<String>) {
            val topic = "pizza-topic"

            val props = initConsumerProps(
                keyDeSerClass = StringDeserializer::class.java,
                valueDeSerClass = StringDeserializer::class.java
            )

            val consumer = KafkaConsumer<String, String>(props)
            consumer.subscribe(listOf(topic))

            val mainThread = Thread.currentThread()

            // main thread 종료 시 별도의 thread로 kafka consumer wakeup() 호출
            Runtime.getRuntime().addShutdownHook(thread(start = false) {
                logger.info { "main program starts to exit by calling wakeup" }
                consumer.wakeup()

                // 메인 스레드가 종료될 때까지 대기
                try {
                    mainThread.join()
                } catch (e: InterruptedException) {
                    logger.error { e.message }
                    throw RuntimeException(e)
                }
            })

            try {
                while (true) {
                    val consumerRecords = consumer.poll(Duration.ofMillis(1000))
                    for (record in consumerRecords) {
                        with(record) {
                            logger.info { "key: ${key()}, partition: ${partition()}, offset: ${offset()}, value: ${value()}" }
                        }
                    }
                }
            } catch (e: WakeupException) {
                logger.error { "wakeup exception has been called" }
            } finally {
                consumer.close()
            }
        }

        private fun <K : Any, V : Any> initConsumerProps(
            keyDeSerClass: Class<out Deserializer<K>>,
            valueDeSerClass: Class<out Deserializer<V>>
        ): Properties = Properties().apply {
            put(BOOTSTRAP_SERVERS_CONFIG, "10.211.55.53:9092")
            // put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeSerClass.name)
            put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeSerClass.name)
            // put(GROUP_ID_CONFIG, "group-01")
            // put(GROUP_ID_CONFIG, "group-01-static")
            // put(GROUP_INSTANCE_ID_CONFIG, "3")

//            put(AUTO_OFFSET_RESET_CONFIG, "latest") // default
//            put(AUTO_OFFSET_RESET_CONFIG, "earliest")
        }
    }
}
