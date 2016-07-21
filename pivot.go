package pivot

import (
	"github.com/ghetzel/pivot/backends"
	"github.com/op/go-logging"
	"time"
)

var log = logging.MustGetLogger(`pivot`)
var Backends = make(map[string]backends.IBackend)
var MonitorCheckInterval = time.Duration(10) * time.Second
var backendCheckLock = make(map[string]bool)

func ConnectBackend(name string, backend backends.IBackend) {
	backendCheckLock[name] = true
	defer func() {
		backendCheckLock[name] = false
	}()

	connectSuccess := false
	connectDone := make(chan bool)

	go func() {
		log.Debugf("Connecting to %s backend '%s'", backend.GetDataset().Type, name)

		if err := backend.Connect(); err != nil {
			log.Errorf("Backend %s experienced an error during connect: %s", name, err)
		} else {
			connectDone <- true
		}
	}()

	select {
	case connectSuccess = <-connectDone:
		if connectSuccess {
			// make the backend available
			backend.Resume()
			return
		}
	case <-time.After(backend.GetConnectTimeout()):
		log.Warningf("Timed out connecting to backend '%s'", name)
	}
}

func RecheckAllBackends() {
	for name, backend := range Backends {
		if !backend.IsConnected() {
			if locked, ok := backendCheckLock[name]; !ok || ok && !locked {
				go ConnectBackend(name, backend)
			}
		}
	}
}

func MonitorBackends() {
	//  auto-reconnect ticker
	autoReconnectTicker := time.NewTicker(MonitorCheckInterval)

	RecheckAllBackends()

	for {
		select {
		case <-autoReconnectTicker.C:
			log.Debugf("Re-checking connection status for all backends")
			RecheckAllBackends()
		}
	}
}
