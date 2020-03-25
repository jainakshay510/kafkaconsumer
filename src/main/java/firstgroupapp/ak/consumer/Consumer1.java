package firstgroupapp.ak.consumer;

import jdk.nashorn.internal.ir.annotations.Immutable;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer1 {

    public static void main(String args[]){

        Logger log= LoggerFactory.getLogger(Consumer1.class);

        Properties properties=new Properties();

        String bootstrapServer="127.0.0.1:9092";
        String topic="my_first_topic";
        String group="my_first_group";
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(topic));

        while(true){
            ConsumerRecords<String,String> consumerRecords=consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String,String> consumer1:consumerRecords){
                log.info("Key: "+ consumer1.key() + ", Value:" +consumer1.value());
                log.info("Partition:" + consumer1.partition()+",Offset:"+consumer1.offset());
            }
        }


    }
}
