import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*

class SimpleConsumer {

    companion object {

        private val logger = KotlinLogging.logger { }

        @JvmStatic
        fun main(args: Array<String>) {
            val topic = "simple-topic"

            val props = initConsumerProps(
                keyDeSerClass = StringDeserializer::class.java,
                valueDeSerClass = StringDeserializer::class.java
            )

            val consumer = KafkaConsumer<String, String>(props)
            consumer.subscribe(mutableListOf(topic))

            while (true) {
                val consumerRecord = consumer.poll(Duration.ofMillis(1000))
                for (record in consumerRecord) {
                    with(record) {
                        logger.info { "key: ${key()}, value: ${value()}, partition: ${partition()}" }
                    }
                }
            }

//            consumer.close()
        }

        private fun <K : Deserializer<*>, V : Deserializer<*>> initConsumerProps(
            keyDeSerClass: Class<K>,
            valueDeSerClass: Class<V>
        ): Properties {
            val props = Properties().apply {
                put(BOOTSTRAP_SERVERS_CONFIG, "10.211.55.53:9092")
                put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeSerClass.name)
                put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeSerClass.name)
                put(GROUP_ID_CONFIG, "group_01")
            }
            return props
        }
    }
}
