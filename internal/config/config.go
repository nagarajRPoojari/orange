package config

import (
	"log"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Directory string `mapstructure:"directory"`

	Memtable struct {
		Threshold           int           `mapstructure:"threshold"`
		QueueHardLimit      int           `mapstructure:"queue_hard_limit"`
		QueueSoftLimit      int           `mapstructure:"queue_soft_limit"`
		FlushTimeInterval   time.Duration `mapstructure:"flush_time_interval"`
		TurnOnWAL           bool          `mapstructure:"turn_on_wal"`
		WALTimeInterval     time.Duration `mapstructure:"wal_time_interval"`
		WALEventChSize      int32         `mapstructure:"wal_event_ch_size"`
		WALWriterBufferSize int           `mapstructure:"wal_writer_buffer_size"`
	} `mapstructure:"memtable"`

	Compaction struct {
		TurnOn                     bool          `mapstructure:"turn_on"`
		Level0MaxSizeInBytes       int64         `mapstructure:"level0_max_size_in_bytes"`
		MaxSizeInBytesGrowthFactor int32         `mapstructure:"max_size_growth_factor"`
		TimeInterval               time.Duration `mapstructure:"time_interval"`
		WALTimeInterval            time.Duration `mapstructure:"wal_time_interval"`
		WALEventChSize             int32         `mapstructure:"wal_event_ch_size"`
		WALWriterBufferSize        int           `mapstructure:"wal_writer_buffer_size"`
	} `mapstructure:"compaction"`
}

func init() {

}

func GetConfig() Config {
	viper.SetConfigName("config")
	viper.SetConfigType("toml")
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Config error: %v", err)
	}
	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		log.Fatalf("Unmarshal error: %v", err)
	}
	return cfg
}
