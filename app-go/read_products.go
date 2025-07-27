package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	uri = "mongodb://appuser:appuserpassword@127.0.0.1:27034/appdb?replicaSet=rs0"
)

type Product struct {
	ID        primitive.ObjectID `bson:"_id" json:"id"`
	Name      string            `bson:"name" json:"name"`
	CreatedAt time.Time         `bson:"createdAt" json:"createdAt"`
}

func printProducts(client *mongo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	coll := client.Database("appdb").Collection("products")
	cursor, err := coll.Find(ctx, bson.M{})
	if err != nil {
		log.Printf("Error finding products: %v", err)
		return
	}
	defer cursor.Close(ctx)
	fmt.Println("All products:")
	i := 1
	for cursor.Next(ctx) {
		var product Product
		if err := cursor.Decode(&product); err != nil {
			log.Printf("Error decoding product: %v", err)
			continue
		}
		prettyJSON, err := json.MarshalIndent(product, "", "  ")
		if err != nil {
			log.Printf("Error formatting product: %v", err)
			continue
		}
		fmt.Printf("%d.\n%s\n", i, string(prettyJSON))
		i++
	}
	fmt.Println("---")
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)
	for {
		printProducts(client)
		time.Sleep(3 * time.Second)
	}
}
