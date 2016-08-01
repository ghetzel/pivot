package pivot

import (
	"fmt"
	"github.com/ghetzel/pivot/client"
	"math/rand"
	"os"
	"testing"
	"time"
)

var server *Server
var client *pivot.Client

func TestMain(m *testing.M) {
	serveErr := make(chan error)

	if config, err := LoadConfigFile(`./test/test.yml`); err == nil {
		if err := config.Initialize(); err == nil {
			server = NewServer()
			server.Address = ``
			server.Port = (rand.Intn(5000) + 60000)

			go func() {
				if err := server.ListenAndServe(); err != nil {
					serveErr <- err
				}
			}()

			select {
			case err := <-serveErr:
				fmt.Println(err)
				os.Exit(4)
			case <-time.After(1 * time.Second):
				break
			}

			client = pivot.NewClient(fmt.Sprintf("http://%s:%d", server.Address, server.Port))

			if err := client.CheckAll(); err != nil {
				fmt.Printf("%v\n", err)
				os.Exit(3)
			}
		} else {
			fmt.Printf("Failed to initialize test configuration: %v\n", err)
			os.Exit(2)
		}
	} else {
		fmt.Printf("Failed to load test configuration: %v\n", err)
		os.Exit(1)
	}

	retCode := m.Run()
	os.Exit(retCode)
}

func TestClientStatus(t *testing.T) {
	if v, err := client.Status(); err != nil {
		t.Error(err)
	} else {
		t.Logf("%+v", v)
	}
}

func TestClientBackends(t *testing.T) {
	if v, err := client.Backends(); err != nil {
		t.Error(err)
	} else {
		t.Logf("%+v", v)
	}
}
