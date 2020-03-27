package br.com.consumer.service;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerService {
	
	private final static String TOPIC = "TESTE_TOPIC";

	public static void main(String[] args) throws InterruptedException {
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(getProperties());
		consumer.subscribe(Collections.singletonList(TOPIC));
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			if(!records.isEmpty()) {
				for (ConsumerRecord<String, String> record : records) {
					System.out.println("====================================================");
					System.out.println("Processando Mensagens do topic");
					System.out.println(record.key());
					System.out.println(record.value());
					System.out.println(record.partition());
					System.out.println(record.offset());
					System.out.println("====================================================");
					Thread.sleep(5000);
				}
			}
			Thread.sleep(5000);
			System.out.println("Nenhum registro encontrado");
		}
	}

	private static Properties getProperties() {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ConsumerService.class.getSimpleName());
		return properties;
	}

}
