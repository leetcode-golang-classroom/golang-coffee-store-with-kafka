package config

import (
	"log"

	"github.com/leetcode-golang-classroom/golang-coffee-store-with-kafka/internal/util"
	"github.com/spf13/viper"
)

type Config struct {
	Port      int64  `mapstructure:"PORT"`
	BrokerURL string `mapstructure:"KAFKA_URL"`
}

var AppConfig *Config

func init() {
	v := viper.New()
	v.AddConfigPath(".")
	v.SetConfigType("env")
	v.SetConfigName(".env")
	v.AutomaticEnv()
	util.FailOnError(v.BindEnv("PORT", "KAFKA_URL"), "failed to bind PORT, KAFKA_URL")
	err := v.ReadInConfig()
	if err != nil {
		log.Println("Load from environment variable")
	}
	err = v.Unmarshal(&AppConfig)
	if err != nil {
		util.FailOnError(err, "Failed to read enivronment")
	}
}
