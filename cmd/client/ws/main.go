package main

import (
	"context"
	"encoding/json"
	"log"
	"net/url"
	"os"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func main() {
	ctx := context.Background()
	wsPort := "8080"
	if len(os.Args) > 1 {
		wsPort = os.Args[1]
	}
	u := url.URL{Scheme: "ws", Host: "localhost:" + wsPort, Path: "/"}
	log.Printf("connecting to %s", u.String())

	conn, _, err := websocket.Dial(ctx, u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	req := map[string]string{
		"query": "SELECT 1 AS id, 'porter' AS name",
	}

	if err := wsjson.Write(ctx, conn, req); err != nil {
		log.Fatal("write:", err)
	}

	log.Println("Sent query, waiting for responses...")

	for {
		msgType, msg, err := conn.Read(ctx)
		if err != nil {
			if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
				log.Println("Stream complete")
				return
			}
			log.Printf("read error: %v", err)
			return
		}

		if msgType == websocket.MessageText {
			var schemaMsg map[string]interface{}
			if err := json.Unmarshal(msg, &schemaMsg); err != nil {
				log.Printf("unmarshal error: %v", err)
				continue
			}
			log.Printf("Schema: %v", schemaMsg)
		} else if msgType == websocket.MessageBinary {
			log.Printf("Received binary IPC frame: %d bytes", len(msg))
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
