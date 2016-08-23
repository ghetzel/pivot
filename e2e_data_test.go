package pivot

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/client"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/util"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"
)

var server *Server
var client *pivot.Client
var testBackend string
var testSchema dal.Collection = dal.Collection{
	Name: `test`,
	Fields: []dal.Field{
		{
			Name:   `id`,
			Type:   `str`,
			Length: 16,
		}, {
			Name:   `state`,
			Type:   `str`,
			Length: 24,
		}, {
			Name:   `county`,
			Type:   `str`,
			Length: 32,
		},
	},
}

var testSchema2 dal.Collection = dal.Collection{
	Name: `test`,
	Fields: []dal.Field{
		{
			Name:   `id`,
			Type:   `str`,
			Length: 36,
		}, {
			Name:   `state`,
			Type:   `str`,
			Length: 24,
		}, {
			Name:   `county`,
			Type:   `str`,
			Length: 32,
		}, {
			Name:   `fips_state`,
			Type:   `int`,
			Length: 8,
		}, {
			Name:   `fips_county`,
			Type:   `int`,
			Length: 16,
		},
	},
}

func TestMain(m *testing.M) {
	serveErr := make(chan error)
	testConfigPath := `./test/test.yml`
	testBackend = `testing`

	if v := os.Getenv(`CONFIG`); v != `` {
		testConfigPath = v
	}

	if v := os.Getenv(`BACKEND`); v != `` {
		testBackend = v
	}

	if config, err := LoadConfigFile(testConfigPath); err == nil {
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

			// perform initial checks
			RecheckAllBackends()

			client = pivot.NewClient(fmt.Sprintf("http://%s:%d", server.Address, server.Port))

			if err := client.CheckAll(); err != nil {
				fmt.Printf("%v\n", err)
				os.Exit(1)
			}
		} else {
			fmt.Printf("Failed to initialize test configuration: %v\n", err)
			os.Exit(2)
		}
	} else {
		fmt.Printf("Failed to load test configuration: %v\n", err)
		os.Exit(3)
	}

	retCode := m.Run()
	os.Exit(retCode)
}

func TestGetStatus(t *testing.T) {
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

func TestBackendStatus(t *testing.T) {
	if backend, err := client.GetBackend(testBackend); err == nil {
		if err := backend.WaitForAvailable(); err != nil {
			t.Error(err)
		} else {
			t.Logf("Backend %q is available", testBackend)
		}
	} else {
		t.Error(err)
	}
}

func TestBackendSchemaCreate(t *testing.T) {
	if backend, err := client.GetBackend(testBackend); err == nil {
		testSchema.Dataset = backend.GetDataset()

		if err := backend.CreateCollection(testSchema); err == nil {
			if schema, err := backend.GetCollection(testSchema.Name); err == nil {
				if err := testSchema.VerifyEqual(schema); err != nil {
					t.Errorf("Schema readback does not match input schema: %v", err)
				}
			} else {
				t.Error(err)
			}
		} else {
			t.Error(err)
		}
	} else {
		t.Error(err)
	}
}

func TestBackendInsertData(t *testing.T) {
	if backend, err := client.GetBackend(testBackend); err == nil {
		if file, err := os.Open(`./test/us-fips.csv`); err == nil {
			data := csv.NewReader(file)
			rs := dal.NewRecordSet()

			for {
				row, err := data.Read()

				if err != nil {
					if err != io.EOF {
						t.Error(err)
					}

					break
				}

				if len(row) == 4 {
					id := sha256.Sum256(append([]byte(row[2]), []byte(row[3])...))
					idStr := hex.EncodeToString(append([]byte{}, id[:]...))

					record := make(dal.Record)

					record[`id`] = idStr[16:32]
					record[`state`] = row[0]
					record[`county`] = row[1]

					if v, err := stringutil.ConvertToInteger(row[2]); err == nil {
						record[`fips_state`] = v
					}

					if v, err := stringutil.ConvertToInteger(row[3]); err == nil {
						record[`fips_county`] = v
					}

					rs.Push(record)

				}
			}

			if err := backend.InsertRecords(testSchema.Name, rs); err != nil {
				t.Error(err)
			}
		} else {
			t.Error(err)
		}
	} else {
		t.Error(err)
	}
}

// func TestBackendSchemaDelete(t *testing.T) {
// 	if backend, err := client.GetBackend(testBackend); err == nil {
// 		if err := backend.DeleteCollection(testSchema.Name); err != nil {
// 			t.Error(err)
// 		}
// 	} else {
// 		t.Error(err)
// 	}
// }

func TestBackendConnectionClose(t *testing.T) {
	if backend, err := client.GetBackend(testBackend); err == nil {
		if err := backend.Suspend(); err == nil {
			if backend.Available {
				t.Errorf("Backend %q should be unavailable, but isn't.", backend.Name)
			}
		} else {
			t.Errorf("Backend %q suspend failed: %v", backend.Name, err)
		}

		if err := backend.Disconnect(); err == nil {
			if backend.Connected {
				t.Errorf("Backend %q should be disconnected, but isn't.", backend.Name)
			}
		} else {
			t.Errorf("Backend %q disconnect failed: %v", backend.Name, err)
		}

	} else {
		t.Error(err)
	}
}
