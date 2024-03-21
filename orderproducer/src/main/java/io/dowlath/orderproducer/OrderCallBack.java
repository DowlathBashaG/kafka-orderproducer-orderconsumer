package io.dowlath.orderproducer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;


public class OrderCallBack implements Callback {
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        System.out.println(recordMetadata.partition());
        System.out.println(recordMetadata.offset());
        System.out.println("Message sent successfully");
        if(e != null){
            e.printStackTrace();
        }
    }
}
