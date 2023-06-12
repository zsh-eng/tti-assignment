package main

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server/kitex_gen/rpc"

	"github.com/redis/go-redis/v9"
)

// Redis Client struct to implement methods for IM Service
type RedisClient struct {
	*redis.Client
}

var opts = &redis.Options{
	Addr:     "redis:6379", // Use redis service name
	Password: "",           // no password set
	DB:       0,            // use default DB
}

// Sort the users in the chatID alphabetically to ensure consistent chatID
func normaliseChatID(chatID string) string {
	participants := strings.Split(chatID, ":")
	sort.Strings(participants)
	return strings.Join(participants, ":")
}

func NewRedisClient(ctx context.Context) *RedisClient {
	// Test the Redis Client
	cli := &RedisClient{redis.NewClient(opts)}
	_, err := cli.Ping(ctx).Result()

	if err != nil {
		log.Fatal("Redis Client Error: ", err)
		panic(err)
	}

	return cli
}

// Saves the Message to Redis with the chatID as the key
func (c *RedisClient) SaveMessageToRedis(ctx context.Context, message rpc.Message, chatID string) error {
	// Save message to Redis
	chatID = normaliseChatID(chatID)

	messageJSON, err := json.Marshal(message)
	if err != nil {
		return err
	}

	z := &redis.Z{
		Score:  float64(message.SendTime),
		Member: messageJSON,
	}

	err = c.ZAdd(ctx, chatID, *z).Err()
	if err != nil {
		return err
	}

	return nil
}

// Gets the messages from Redis with the chatID as the key
func (c *RedisClient) GetMessagesFromRedis(ctx context.Context, chatID string, start int64, stop int64, rev bool) ([]*rpc.Message, error) {
	// Get messages from Redis
	chatID = normaliseChatID(chatID)

	var members []string
	var err error
	if rev {
		members, err = c.ZRevRange(ctx, chatID, start, stop).Result()
	} else {
		members, err = c.ZRange(ctx, chatID, start, stop).Result()
	}

	if err != nil {
		return nil, err
	}

	var messages []*rpc.Message
	for _, member := range members {
		var message *rpc.Message
		err := json.Unmarshal([]byte(member), &message)
		if err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}

	return messages, nil
}

// IMServiceImpl implements the last service interface defined in the IDL.
type IMServiceImpl struct{}

var client = NewRedisClient(context.Background())

func (s *IMServiceImpl) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {
	message := req.GetMessage()
	if message.GetSendTime() == 0 {
		message.SendTime = time.Now().Unix()
	}

	err := client.SaveMessageToRedis(ctx, *message, message.GetChat())
	if err != nil {
		return nil, err
	}

	return &rpc.SendResponse{
		Code: 0,
		Msg:  "success",
	}, nil
}

func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {
	start := req.GetCursor()
	stop := start + int64(req.GetLimit())
	rev := req.GetReverse()

	messages, err := client.GetMessagesFromRedis(ctx, req.GetChat(), start, stop, rev)
	if err != nil {
		return nil, err
	}

	hasMore := false
	if len(messages) == int(req.GetLimit())+1 {
		hasMore = true
		messages = messages[:len(messages)-1]
	}

	// Starting position of the next page
	nextCursor := stop
	return &rpc.PullResponse{
		Code:       0,
		Msg:        "success",
		Messages:   messages,
		HasMore:    &hasMore,
		NextCursor: &nextCursor,
	}, nil
}
