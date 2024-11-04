import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties
import kotlin.concurrent.thread

class MultiTopicReBalanceConsumer {

    companion object {
        private val logger = KotlinLogging.logger {}

        @JvmStatic
        fun main(args: Array<String>) {
            val props = initConsumerProps(
                keyDeSerClass = StringDeserializer::class.java,
                valueDeSerClass = StringDeserializer::class.java
            )

            val consumer = KafkaConsumer<String, String>(props)
            consumer.subscribe(listOf(
                "topic-p3-t1",
                "topic-p3-t2"
            ))

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
                            logger.info { "topic: ${topic()} key: ${key()}, partition: ${partition()}, offset: ${offset()}, value: ${value()}" }
                        }
                    }
                    Thread.sleep(10000)
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
            put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeSerClass.name)
            put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeSerClass.name)
            put(GROUP_ID_CONFIG, "group-mtopic")
        }
    }
}
