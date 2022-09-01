package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"pole/internal/conf"
	poled2 "pole/internal/poled"
	"pole/internal/poled/meta"
	poleRaft "pole/internal/raft"
	"pole/internal/server"

	"github.com/fsnotify/fsnotify"
	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	defaultEnvFile = ".env"
)

var (
	// Used for flags.
	cfgFile       string
	envFile       string
	raftId        string
	raftAddress   string
	raftBootstrap bool
	raftDataDir   string
	join          string

	poleCmd = &cobra.Command{
		Use:   "pole",
		Short: "A full text Search engine",
		Long:  `A full text Search engine that use sql to create,update,query,delete index data`,
		RunE: func(cmd *cobra.Command, args []string) error {

			meta := meta.NewMeta()
			ctx := context.Background()
			raft, err := poleRaft.NewRaft(ctx, raftId, raftAddress, raftDataDir, join, raftBootstrap, meta)
			if err != nil {
				return err
			}
			conf := conf.GetConfig()

			poled, err := poled2.NewPoled(conf, meta, raft.Raft)
			if err != nil {
				return err
			}

			grpcService := server.NewNodeService(raft.Raft)

			poleService := server.NewPoleService(poled)

			grpcServer, err := server.NewGrpcServer(grpcService, poleService)
			if err != nil {
				return err
			}

			if err := grpcServer.Start(); err != nil {
				return err
			}

			httpServer, err := server.NewHttpServer(conf.HttpAddr, poled)
			if err != nil {
				return err
			}
			if err := httpServer.Start(); err != nil {
				return err
			}

			quitCh := make(chan os.Signal, 1)
			signal.Notify(quitCh, syscall.SIGINT, syscall.SIGTERM)

			<-quitCh

			raft.Stop(ctx)

			grpcServer.Stop()

			if err := httpServer.Stop(); err != nil {
				return err
			}
			poled.Close()

			return nil
		},
	}
)

// Execute executes the root command.
func Execute() error {
	if err := poleCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return nil
}

func init() {
	cobra.OnInitialize(initConfig)

	poleCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/pole.yaml)")
	poleCmd.PersistentFlags().StringVar(&envFile, "env", defaultEnvFile, "environment file")
	poleCmd.PersistentFlags().StringVar(&raftId, "raft-id", "", "raft id")
	poleCmd.PersistentFlags().StringVar(&raftAddress, "raft-addr", "", "raft address")
	poleCmd.PersistentFlags().StringVar(&raftDataDir, "raft-data-dir", "./", "raft data directory")
	poleCmd.PersistentFlags().BoolVar(&raftBootstrap, "raft-bootstrap", false, "raft bootstrap")
	poleCmd.PersistentFlags().StringVar(&join, "join", "", "join the cluster")
}

func initConfig() {
	godotenv.Load(envFile)
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		if err != nil {
			// Failed to get home directory.
			// Exit the program.
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		// Search config in home directory with name ".cobra" (without extension).
		viper.AddConfigPath(home)
		viper.AddConfigPath("/etc")
		viper.AddConfigPath("./etc")
		viper.AddConfigPath("./")
		viper.SetConfigType("yaml")
		viper.SetConfigName("pole")
	}

	viper.AutomaticEnv()
	viper.SetEnvPrefix("POLE")

	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	} else {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	conf := conf.GetConfig()
	if err := viper.Unmarshal(conf); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	viper.OnConfigChange(func(in fsnotify.Event) {
		viper.Unmarshal(conf)
	})
	viper.WatchConfig()
}
