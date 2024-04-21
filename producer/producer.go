package producer

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type DataSourceMessage interface {
	ToJSON() ([]byte, error)
}

func (m DataSourceRecordsCountMessage) ToJSON() ([]byte, error) {
	return json.Marshal(m)
}

func (m DataSourceVolumeMessage) ToJSON() ([]byte, error) {
	return json.Marshal(m)
}

type DataSourceRecordsCountMessage struct {
	DataStoreId int    `json:"dataStoreId"`
	ResourceId  int    `json:"resourceId"`
	DataType    string `json:"dataType"`
	Payload     int64  `json:"payload"`
}

type DataSourceVolumeMessage struct {
	DataStoreId int     `json:"dataStoreId"`
	ResourceId  int     `json:"resourceId"`
	DataType    string  `json:"dataType"`
	Payload     float64 `json:"payload"`
}

func AddDataSourceInfoToMessageQueue(p *kafka.Producer, data DataSourceMessage) {
	jsonData, err := data.ToJSON()
	if err != nil {
		fmt.Printf("Error marshalling data: %v\n", err)
		return
	}

	topic := "data-source-info-topic"

	fmt.Println("Producing message:")
	fmt.Println(string(jsonData))

	go func() {
		err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          jsonData,
		}, nil)

		if err != nil {
			fmt.Printf("Failed to produce message: %v\n", err)
		}
	}()
}
