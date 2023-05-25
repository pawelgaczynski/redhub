package redhub

import (
	"bytes"
	"os"
	"sync"
	"time"

	"github.com/IceFireDB/redhub/pkg/resp"
	"github.com/pawelgaczynski/gain"
	gsync "github.com/pawelgaczynski/gain/pkg/pool/sync"
	"github.com/rs/zerolog"
)

type Action int

const (
	// None indicates that no action should occur following an event.
	None Action = iota

	// Close closes the connection.
	Close

	// Shutdown shutdowns the server.
	Shutdown
)

type Conn gain.Conn

type Options struct {
	// Architecture indicates one of the two available architectures: Reactor and SocketSharding.
	//
	// The Reactor design pattern has one input called Acceptor,
	// which demultiplexes the handling of incoming connections to Consumer workers.
	// The load balancing algorithm can be selected via configuration option.
	//
	// The Socket Sharding allows multiple workers to listen on the same address and port combination.
	// In this case the kernel distributes incoming requests across all the sockets.
	Architecture gain.ServerArchitecture
	// AsyncHandler indicates whether the engine should run the OnRead EventHandler method in a separate goroutines.
	AsyncHandler bool
	// GoroutinePool indicates use of pool of bounded goroutines for OnRead calls.
	// Important: Valid only if AsyncHandler is true
	GoroutinePool bool
	// CPUAffinity determines whether each engine worker is locked to the one CPU.
	CPUAffinity bool
	// ProcessPriority sets the prority of the process to high (-19). Requires root privileges.
	ProcessPriority bool
	// Workers indicates the number of consumers or shard workers. The default is runtime.NumCPU().
	Workers int
	// CBPFilter uses custom BPF filter to improve the performance of the Socket Sharding architecture.
	CBPFilter bool
	// LoadBalancing indicates the load-balancing algorithm to use when assigning a new connection.
	// Important: valid only for Reactor architecture.
	LoadBalancing gain.LoadBalancing
	// SocketRecvBufferSize sets the maximum socket receive buffer in bytes.
	SocketRecvBufferSize int
	// SocketSendBufferSize sets the maximum socket send buffer in bytes.
	SocketSendBufferSize int
	// TCPKeepAlive sets the TCP keep-alive for the socket.
	TCPKeepAlive time.Duration
	// LoggerLevel indicates the logging level.
	LoggerLevel zerolog.Level
	// PrettyLogger sets the pretty-printing zerolog mode.
	// Important: it is inefficient so should be used only for debugging.
	PrettyLogger bool
}

func NewRedHub(
	onAccept func(c Conn),
	onClose func(c Conn, err error),
	handler func(cmd resp.Command, out []byte) ([]byte, Action),
) *redHub {
	return &redHub{
		redHubBufMap: make(map[gain.Conn]*connBuffer),
		connSync:     sync.RWMutex{},
		onAccept:     onAccept,
		onClose:      onClose,
		handler:      handler,
		logger:       zerolog.New(os.Stdout).With().Logger().Level(zerolog.ErrorLevel),
	}
}

const bytesliceCap = 2048

type byteSlicePool struct {
	internalPool gsync.Pool[[]byte]
}

func (b *byteSlicePool) Get() []byte {
	slice := b.internalPool.Get()
	if slice == nil {
		return make([]byte, 0, bytesliceCap)
	}
	return slice
}

func (b *byteSlicePool) Put(buf []byte) {
	b.internalPool.Put(buf[:0])
}

var pool = &byteSlicePool{
	internalPool: gsync.NewPool[[]byte](),
}

type redHub struct {
	gain.DefaultEventHandler
	onAccept     func(c Conn)
	onClose      func(c Conn, err error)
	handler      func(cmd resp.Command, out []byte) ([]byte, Action)
	redHubBufMap map[gain.Conn]*connBuffer
	logger       zerolog.Logger
	connSync     sync.RWMutex
}

type connBuffer struct {
	buf     bytes.Buffer
	command []resp.Command
}

func (rs *redHub) OnAccept(c gain.Conn) {
	rs.connSync.Lock()
	defer rs.connSync.Unlock()
	rs.redHubBufMap[c] = new(connBuffer)
	rs.onAccept(c)
	return
}

func (rs *redHub) OnClose(c gain.Conn, err error) {
	rs.connSync.Lock()
	defer rs.connSync.Unlock()
	delete(rs.redHubBufMap, c)
	rs.onClose(c, err)
	return
}

func (rs *redHub) OnRead(c gain.Conn, n int) {
	rs.connSync.RLock()
	defer rs.connSync.RUnlock()
	out := pool.Get()
	defer pool.Put(out)
	cb, ok := rs.redHubBufMap[c]
	if !ok {
		out = resp.AppendError(out, "ERR Client is closed")
		_, err := c.Write(out)
		if err != nil {
			rs.logger.Error().Err(err).Msg("write error")
		}
		return
	}
	frame, err := c.Next(-1)
	if err != nil {
		rs.logger.Error().Err(err).Msg("read error")
		return
	}
	cb.buf.Write(frame)
	cmds, lastbyte, err := resp.ReadCommands(cb.buf.Bytes())
	if err != nil {
		out = resp.AppendError(out, "ERR "+err.Error())
		_, err = c.Write(out)
		if err != nil {
			rs.logger.Error().Err(err).Msg("write error")
		}
		return
	}
	cb.command = append(cb.command, cmds...)
	cb.buf.Reset()
	var status Action
	if len(lastbyte) == 0 {
		for len(cb.command) > 0 {
			cmd := cb.command[0]
			if len(cb.command) == 1 {
				cb.command = nil
			} else {
				cb.command = cb.command[1:]
			}
			out, status = rs.handler(cmd, out)
		}
	} else {
		cb.buf.Write(lastbyte)
	}
	_, err = c.Write(out)
	if err != nil {
		rs.logger.Error().Err(err).Msg("write error")
	}
	if status == Close {
		err = c.Close()
		if err != nil {
			rs.logger.Error().Err(err).Msg("close error")
		}
	}
	return
}

func ListendAndServe(addr string, options Options, rh *redHub) error {
	serveOptions := []gain.ConfigOption{
		gain.WithArchitecture(options.Architecture),
		gain.WithAsyncHandler(options.AsyncHandler),
		gain.WithGoroutinePool(options.GoroutinePool),
		gain.WithCPUAffinity(options.CPUAffinity),
		gain.WithProcessPriority(options.ProcessPriority),
		gain.WithWorkers(options.Workers),
		gain.WithCBPF(options.CBPFilter),
		gain.WithLoadBalancing(options.LoadBalancing),
		gain.WithSocketRecvBufferSize(options.SocketRecvBufferSize),
		gain.WithSocketSendBufferSize(options.SocketSendBufferSize),
		gain.WithTCPKeepAlive(options.TCPKeepAlive),
		gain.WithLoggerLevel(options.LoggerLevel),
		gain.WithPrettyLogger(options.PrettyLogger),
	}

	return gain.ListenAndServe(addr, rh, serveOptions...)
}
