package io.dowlath.orderproducer;

import io.dowlath.orderproducer.customserializer.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class OrderproducerApplication {

	public static void main(String[] args) {

		SpringApplication.run(OrderproducerApplication.class, args);
		Properties props = new Properties();
		props.setProperty("bootstrap.servers","localhost:9092");
		props.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		//props.setProperty("value.serializer","org.apache.kafka.common.serialization.IntegerSerializer");
		props.setProperty("value.serializer","io.dowlath.orderproducer.customserializer.OrderSerializer");

		/*KafkaProducer<String,Integer> producer = new KafkaProducer<String, Integer>(props);
		ProducerRecord<String,Integer> record = new ProducerRecord<>("OrderTopic","Mac Book Pro",100); */

		// Custom serializer ....

		//KafkaProducer<String,Integer> producer = new KafkaProducer<String, Integer>(props);
		KafkaProducer<String,Order> producer = new KafkaProducer<String,Order>(props);
		Order order = new Order();
		order.setCustomerName("Dowlath Basha G");
		order.setProduct("IPhone");
		order.setQunatity(1);

		ProducerRecord<String,Order> record = new ProducerRecord<>("OrderCSTopic",order.getCustomerName(),order);

		try{

			//producer.send(record);

			/*

			This is sync call....

			RecordMetadata recordMetadata = producer.send(record).get();
			System.out.println(recordMetadata.partition());
			System.out.println(recordMetadata.offset());
		    System.out.println("Message sent successfully");

			 */
			System.out.println("Try....producer");
			producer.send(record);//,new OrderCallBack());

		}catch(Exception e){
          e.printStackTrace();
		} finally{
			producer.close();
		}
	}

}
