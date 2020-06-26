package tirggerstorage

import (
	"strconv"

	"github.com/go-redis/redis"
	log "github.com/sirupsen/logrus"
)

type RedisTriggerStorage struct {
	client redis.Client
}

func NewRedisTriggerStorage(host string, port int, password string, db int) Storage {
	c := redis.NewClient(&redis.Options{
		Addr:     host + ":" + strconv.Itoa(port),
		Password: password,
		DB:       db,
	})

	redisClient := &RedisTriggerStorage{
		client: *c,
	}

	statusCmd := redisClient.client.Ping()
	res, err := statusCmd.Result()
	if err != nil {
		panic(err)
	}

	log.Debugf("[RedisTriggerStorage] %s", res)

	return redisClient
}

func NewRedisTriggerStorageMappedConfig(config map[string]interface{}) Storage {
	host := config["host"].(string)
	port := config["port"].(int)
	password := config["password"].(string)
	db := config["db"].(int)

	rs := NewRedisTriggerStorage(host, port, password, db)
	return rs
}

func (rs RedisTriggerStorage) Get(workspace string, key string) map[string]string {
	blob, err := rs.client.HGetAll(workspace + "-" + key).Result()

	if err != nil {
		log.Fatal(err)
		return make(map[string]string)
	}

	return blob
}

func (rs RedisTriggerStorage) Put(workspace string, key string, field string, value []byte) {
	boolCmd := rs.client.HSet(workspace + "-" + key, field, value)
	result, err := boolCmd.Result()
	if result {
		log.Infof("[RedisTriggerStorage] Updated %s/%s", field, key)
	} else {
		log.Errorf("[RedisTriggerStorage] Error updating %s/%s: %s", field, key, err)
	}
}
