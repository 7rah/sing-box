//go:build darwin

package group

import (
	"io"
	"os"
	"sync"
	"time"

	"github.com/sagernet/sing/common/buf"
	E "github.com/sagernet/sing/common/exceptions"

	"golang.org/x/net/route"
	"golang.org/x/sys/unix"
)

const autoSelectorDarwinDebounce = 200 * time.Millisecond

func newAutoSelectorPlatformWatcher(selector *AutoSelector) (io.Closer, error) {
	watcher, err := newDarwinRouteWatcher(selector)
	if err != nil {
		return nil, err
	}
	return watcher, nil
}

type darwinRouteWatcher struct {
	selector       *AutoSelector
	socketFile     *os.File
	closeOnce      sync.Once
	done           chan struct{}
	debounceAccess sync.Mutex
	debounceTimer  *time.Timer
}

func newDarwinRouteWatcher(selector *AutoSelector) (*darwinRouteWatcher, error) {
	socketFD, err := unix.Socket(unix.AF_ROUTE, unix.SOCK_RAW, 0)
	if err != nil {
		return nil, E.Cause(err, "create route watcher socket")
	}
	routeSocketFile := os.NewFile(uintptr(socketFD), "auto-selector-route")
	watcher := &darwinRouteWatcher{
		selector:   selector,
		socketFile: routeSocketFile,
		done:       make(chan struct{}),
	}
	go watcher.loop()
	return watcher, nil
}

func (w *darwinRouteWatcher) loop() {
	buffer := buf.NewPacket()
	defer buffer.Release()
	for {
		n, err := w.socketFile.Read(buffer.FreeBytes())
		if err != nil {
			select {
			case <-w.done:
				return
			default:
			}
			w.selector.logger.Debug("auto selector ", w.selector.Tag(), " darwin watcher stopped: ", err)
			return
		}
		buffer.Truncate(n)
		messages, err := route.ParseRIB(route.RIBTypeRoute, buffer.Bytes())
		buffer.Reset()
		if err != nil {
			continue
		}
		watchInterfaceIndex := w.watchInterfaceIndex()
		for _, message := range messages {
			if !matchDarwinInterfaceMessage(message, w.selector.watchInterface, watchInterfaceIndex) {
				continue
			}
			w.selector.logger.Info("auto selector ", w.selector.Tag(), " interface changed: ", w.selector.watchInterface)
			w.schedule()
			break
		}
	}
}

func (w *darwinRouteWatcher) watchInterfaceIndex() int {
	if w.selector.networkManager == nil {
		return 0
	}
	if err := w.selector.networkManager.UpdateInterfaces(); err != nil {
		return 0
	}
	networkInterface, err := w.selector.networkManager.InterfaceFinder().ByName(w.selector.watchInterface)
	if err != nil || networkInterface == nil {
		return 0
	}
	return networkInterface.Index
}

func (w *darwinRouteWatcher) schedule() {
	w.debounceAccess.Lock()
	defer w.debounceAccess.Unlock()
	if w.debounceTimer == nil {
		w.debounceTimer = time.AfterFunc(autoSelectorDarwinDebounce, func() {
			w.selector.scheduleRecheck("darwin-interface-event")
		})
		return
	}
	w.debounceTimer.Reset(autoSelectorDarwinDebounce)
}

func (w *darwinRouteWatcher) Close() error {
	w.closeOnce.Do(func() {
		close(w.done)
	})
	w.debounceAccess.Lock()
	if w.debounceTimer != nil {
		w.debounceTimer.Stop()
		w.debounceTimer = nil
	}
	w.debounceAccess.Unlock()
	return w.socketFile.Close()
}

func matchDarwinInterfaceMessage(message route.Message, interfaceName string, interfaceIndex int) bool {
	switch message := message.(type) {
	case *route.InterfaceMessage:
		return message.Name == interfaceName
	case *route.InterfaceAddrMessage:
		return interfaceIndex > 0 && message.Index == interfaceIndex
	default:
		return false
	}
}
