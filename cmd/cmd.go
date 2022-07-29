package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"pole/internal/server"
)

const (
	defaultEnvFile = ".env"
)

var (
	// Used for flags.
	cfgFile     string
	envFile     string

	poleCmd = &cobra.Command{
		Use:   "pole",
		Short: "A full text Search engine",
		Long:  `A full text Search engine that use sql to create,update,query,delete index data`,
		RunE: func(cmd *cobra.Command, args []string) error {
			httpServer, err := server.NewHttpServer("127.0.0.1:5000")
			if err != nil {
				return err
			}
			if err := httpServer.Start(); err != nil {
				return err
			}

			quitCh := make(chan os.Signal, 1)
			signal.Notify(quitCh, syscall.SIGINT, syscall.SIGTERM)

			<-quitCh

			if err := httpServer.Stop(); err != nil {
				return err
			}
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

	poleCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.cobra.yaml)")
	poleCmd.PersistentFlags().StringVar(&envFile, "envFile", defaultEnvFile, "environment file")
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
		viper.AddConfigPath("./")
		viper.SetConfigType("yaml")
		viper.SetConfigName("pole")
	}

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
