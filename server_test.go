package pivot

import (
	"fmt"
	"github.com/ghetzel/pivot/client"
	"github.com/ghetzel/pivot/util"
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
	if v, err := client.Status(); err == nil {
		if !v.OK {
			t.Errorf("Status.OK is false, expected true")
		}

		if v.Application != util.ApplicationName {
			t.Errorf("Status.Application; expected: %q, got: %q", util.ApplicationName, v.Application)
		}

		if v.Version != util.ApplicationVersion {
			t.Errorf("Status.Version; expected: %q, got: %q", util.ApplicationVersion, v.Version)
		}
	} else {
		t.Error(err)
	}
}

	// Available            bool          `json:"available"`
	// Connected            bool          `json:"connected"`
	// ConnectMaxAttempts   int           `json:"max_connection_attempts"`
	// ConnectTimeout       time.Duration `json:"connect_timeout"`
	// Dataset              dal.Dataset   `json:"configuration"`
	// Name                 string        `json:"name"`
	// SchemaRefresh        time.Duration `json:"schema_refresh_interval"`
	// SchemaRefreshMaxFail int           `json:"schema_refresh_max_failures"`
	// SchemaRefreshTimeout time.Duration `json:"schema_refresh_timeout"`

func TestDummyAllBackends(t *testing.T) {
	if backends, err := client.Backends(); err == nil {
		if len(backends) == 1 {
			backend := backends[0]

			if backend.Name != `dummy1` {
				t.Errorf("backend.Name; expected: %q, got: %q", `dummy1`, backend.Name)
			}

			if backend.Dataset.Name != `test-one` {
				t.Errorf("backend.Name; expected: %q, got: %q", `test-one`, backend.Dataset.Name)
			}

			if len(backend.Dataset.Addresses) == 2 {
				if v := backend.Dataset.Addresses[0]; v != `http://127.0.0.1:0` {
					t.Errorf("backend.Dataset.Addresses[0]; expected: %q, got: %q", `http://127.0.0.1:0`, v)
				}

				if v := backend.Dataset.Addresses[1]; v != `http://127.0.0.1:1` {
					t.Errorf("backend.Dataset.Addresses[1]; expected: %q, got: %q", `http://127.0.0.1:1`, v)
				}

			} else{
				t.Errorf("backend.Dataset.Addresses wrong size; expected: 2, got: %d", len(backend.Dataset.Addresses))
			}

			if opts := backend.Dataset.Options; len(opts) == 3 {
				if v, ok := opts[`first`]; !ok || v != float64(1) {
					t.Errorf("backend.Dataset.Options['first']; expected: %v, got: %v(%T)", 1, v, v)
				}

				if v, ok := opts[`second`]; !ok || v != true {
					t.Errorf("backend.Dataset.Options['second']; expected: %v, got: %v(%T)", true, v, v)
				}

				if v, ok := opts[`third`]; !ok || v != `three` {
					t.Errorf("backend.Dataset.Options['third']; expected: %v, got: %v(%T)", `three`, v, v)
				}
			}else{
				t.Errorf("backend.Dataset.Options wrong size; expected: 3, got: %d", len(opts))
			}
		}else{
			t.Errorf("wrong size; expected: 1, got: %d", len(backends))
		}
	} else {
		t.Error(err)
	}
}


func TestDummyOneBackend(t *testing.T) {
	if backend, err := client.GetBackend(`dummy1`); err == nil {
		if backend.Available {
			t.Errorf("Backend %q started available, should be unavailable", backend.Name)
		}

		if backend.Connected {
			t.Errorf("Backend %q started connected, should be disconnected", backend.Name)
		}

		if err := backend.Connect(); err == nil {
			if !backend.Connected {
				t.Errorf("Backend %q should be connected, but isn't.", backend.Name)
			}
		}else{
			t.Errorf("Backend %q connect failed: %v", backend.Name, err)
		}

		if err := backend.Resume(); err == nil {
			if !backend.Available {
				t.Errorf("Backend %q should be available, but isn't.", backend.Name)
			}
		}else{
			t.Errorf("Backend %q resume failed: %v", backend.Name, err)
		}

		if err := backend.Suspend(); err == nil {
			if backend.Available {
				t.Errorf("Backend %q should be unavailable, but isn't.", backend.Name)
			}
		}else{
			t.Errorf("Backend %q suspend failed: %v", backend.Name, err)
		}

		if err := backend.Disconnect(); err == nil {
			if backend.Connected {
				t.Errorf("Backend %q should be disconnected, but isn't.", backend.Name)
			}
		}else{
			t.Errorf("Backend %q disconnect failed: %v", backend.Name, err)
		}
	} else {
		t.Error(err)
	}
}
