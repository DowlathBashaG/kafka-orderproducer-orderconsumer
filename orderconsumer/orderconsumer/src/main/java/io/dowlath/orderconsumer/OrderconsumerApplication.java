package io.dowlath.orderconsumer;

import io.dowlath.orderconsumer.customserializer.Order;
import io.dowlath.orderconsumer.customserializer.OrderDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@SpringBootApplication
public class OrderconsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(OrderconsumerApplication.class, args);

		Properties props = new Properties();
		props.setProperty("bootstrap.servers","localhost:9092");
		//props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		//props.setProperty("value.deserializer","org.apache.kafka.common.serialization.IntegerDeserializer");
		props.setProperty("key.deserializer", StringDeserializer.class.getName());
		props.setProperty("value.deserializer", OrderDeserializer.class.getName());
		props.setProperty("group.id","OrderGroup");

		/*KafkaConsumer<String,Integer> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList("OrderTopic"));*/

		KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
		//consumer.subscribe(Collections.singletonList("OrderTopic"));
		consumer.subscribe(Collections.singletonList("OrderCSTopic"));
		// This poll method is doing lot-of things...  1. re-partitoning re-balacing and check
		// 2. the heart beat and 3. data fetching

		/*ConsumerRecords<String,Integer> orders = consumer.poll(Duration.ofSeconds(20));
		for(ConsumerRecord<String,Integer> order : orders){
			System.out.println("Product Name : "+ order.key());
			System.out.println("Quantity     : "+ order.value());
		}*/

		ConsumerRecords<String,Order> records = consumer.poll(Duration.ofSeconds(20));

		for(ConsumerRecord<String,Order> record : records){
			String customerName = record.key();
			Order order = record.value();
			System.out.println("Customer Name : "+ customerName);
			System.out.println("Product : "+ order.getProduct());
			System.out.println("Quantity : "+ order.getQunatity());
			/*System.out.println("Product Name : "+ record.key());
			System.out.println("Quantity     : "+ record.value());*/
		}

		consumer.close();

	}

}
