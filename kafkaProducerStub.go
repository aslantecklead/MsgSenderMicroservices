package main

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	// Замените брокерами Kafka и названием темы на ваши значения
	brokers := "localhost:9092"
	topic := "ваша_тема"

	configMap := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
	}

	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Fatalf("Ошибка создания продюсера: %s", err)
	}

	defer p.Close()

	fmt.Println("Введите ваше имя:")
	nameInput := getUserInput()

	fmt.Println("Введите заголовок сообщения:")
	titleInput := getUserInput()

	fmt.Println("Введите тело сообщения:")
	bodyInput := getUserInput()

	message := fmt.Sprintf(`{"name": "%s", "title": "%s", "body": "%s"}`, nameInput, titleInput, bodyInput)

	deliveryChan := make(chan kafka.Event)

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:         []byte(message),
	}, deliveryChan)

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Printf("Ошибка отправки сообщения: %s", m.TopicPartition.Error)
	} else {
		fmt.Printf("Сообщение отправлено в тему '%s', партиция %d, смещение %d\n", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}
}

func getUserInput() string {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	return scanner.Text()
}
