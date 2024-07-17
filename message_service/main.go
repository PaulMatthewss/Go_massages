package main

import (
	"context"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/segmentio/kafka-go"
)

var (
	dbpool      *pgxpool.Pool
	kafkaWriter *kafka.Writer
)

func main() {
	var err error

	// Initialize PostgreSQL connection
	dbpool, err = pgxpool.Connect(context.Background(), "postgres://user:password@localhost:5432/mydb")
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer dbpool.Close()

	// Initialize Kafka writer
	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "my-topic",
		Balancer: &kafka.LeastBytes{},
	}
	defer kafkaWriter.Close()

	// Initialize Gin router
	r := gin.Default()
	r.POST("/message", handleMessage)

	// Run the server
	if err := r.Run(":8080"); err != nil {
		log.Fatalf("Failed to run server: %v\n", err)
	}
}

func handleMessage(c *gin.Context) {
	var json struct {
		Message string `json:"message"`
	}

	if err := c.ShouldBindJSON(&json); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Save message to PostgreSQL
	_, err := dbpool.Exec(context.Background(), "INSERT INTO messages (content) VALUES ($1)", json.Message)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save message"})
		return
	}

	// Send message to Kafka
	err = kafkaWriter.WriteMessages(context.Background(), kafka.Message{
		Value: []byte(json.Message),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to send message to Kafka"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "message received"})
}
