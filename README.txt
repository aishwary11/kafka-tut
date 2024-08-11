docker-compose up --build
curl -X POST http://localhost:3000/send -H "Content-Type: application/json" -d '{"message": "Hello, Kafka!"}'
curl http://localhost:3000/consume