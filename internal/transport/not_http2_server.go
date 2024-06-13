package transport

import (
	"bytes"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/internal/channelz"
	"google.golang.org/grpc/internal/syscall"
	"google.golang.org/grpc/peer"
)

type not_http2Server struct {
	*http2Server
}

type NotnetsListener struct {
	mu              sync.Mutex
	notnets_context *ServerContext
	addr            NotnetsAddr
}

func Listen(addr string) *NotnetsListener {
	var lis NotnetsListener
	log.Info().Msgf("New Listener at: %s", addr)

	lis.notnets_context = RegisterServer(addr)
	lis.addr = NotnetsAddr{basic: addr}
	return &lis
}

func (lis *NotnetsListener) Accept() (conn net.Conn, err error) {
	lis.mu.Lock()
	defer lis.mu.Unlock()
	queue := lis.notnets_context.Accept()

	//TODO
	if queue != nil {
		conn = &NotnetsConn{
			ClientSide:  false, //This is the server implementation
			isConnected: true,
			queues:      queue,
			local_addr:  &lis.addr,
			// remote_addr: ,
		}
	} else {
		conn = nil
	}
	return conn, err
}

func (lis *NotnetsListener) Close() error {
	lis.mu.Lock()
	defer lis.mu.Unlock()
	lis.notnets_context.Shutdown()
	return nil
}

func (lis *NotnetsListener) Addr() net.Addr {
	return &lis.addr
}

func NewNotnetsServerTransport(conn net.Conn, config *ServerConfig) (_ ServerTransport, err error) {
	var authInfo credentials.AuthInfo
	rawConn := conn
	if config.Credentials != nil {
		var err error
		conn, authInfo, err = config.Credentials.ServerHandshake(rawConn)
		if err != nil {
			// ErrConnDispatched means that the connection was dispatched away
			// from gRPC; those connections should be left open. io.EOF means
			// the connection was closed before handshaking completed, which can
			// happen naturally from probers. Return these errors directly.
			if err == credentials.ErrConnDispatched || err == io.EOF {
				return nil, err
			}
			return nil, connectionErrorf(false, err, "ServerHandshake(%q) failed: %v", rawConn.RemoteAddr(), err)
		}
	}
	writeBufSize := config.WriteBufferSize
	readBufSize := config.ReadBufferSize
	maxHeaderListSize := defaultServerMaxHeaderListSize
	if config.MaxHeaderListSize != nil {
		maxHeaderListSize = *config.MaxHeaderListSize
	}
	framer := newFramer(conn, writeBufSize, readBufSize, config.SharedWriteBuffer, maxHeaderListSize)
	// Send initial settings as connection preface to client.
	isettings := []http2.Setting{{
		ID:  http2.SettingMaxFrameSize,
		Val: http2MaxFrameLen,
	}}
	if config.MaxStreams != math.MaxUint32 {
		isettings = append(isettings, http2.Setting{
			ID:  http2.SettingMaxConcurrentStreams,
			Val: config.MaxStreams,
		})
	}
	dynamicWindow := true
	iwz := int32(initialWindowSize)
	if config.InitialWindowSize >= defaultWindowSize {
		iwz = config.InitialWindowSize
		dynamicWindow = false
	}
	icwz := int32(initialWindowSize)
	if config.InitialConnWindowSize >= defaultWindowSize {
		icwz = config.InitialConnWindowSize
		dynamicWindow = false
	}
	if iwz != defaultWindowSize {
		isettings = append(isettings, http2.Setting{
			ID:  http2.SettingInitialWindowSize,
			Val: uint32(iwz)})
	}
	if config.MaxHeaderListSize != nil {
		isettings = append(isettings, http2.Setting{
			ID:  http2.SettingMaxHeaderListSize,
			Val: *config.MaxHeaderListSize,
		})
	}
	if config.HeaderTableSize != nil {
		isettings = append(isettings, http2.Setting{
			ID:  http2.SettingHeaderTableSize,
			Val: *config.HeaderTableSize,
		})
	}
	if err := framer.fr.WriteSettings(isettings...); err != nil {
		return nil, connectionErrorf(false, err, "transport: %v", err)
	}
	// Adjust the connection flow control window if needed.
	if delta := uint32(icwz - defaultWindowSize); delta > 0 {
		if err := framer.fr.WriteWindowUpdate(0, delta); err != nil {
			return nil, connectionErrorf(false, err, "transport: %v", err)
		}
	}
	kp := config.KeepaliveParams
	if kp.MaxConnectionIdle == 0 {
		kp.MaxConnectionIdle = defaultMaxConnectionIdle
	}
	if kp.MaxConnectionAge == 0 {
		kp.MaxConnectionAge = defaultMaxConnectionAge
	}
	// Add a jitter to MaxConnectionAge.
	kp.MaxConnectionAge += getJitter(kp.MaxConnectionAge)
	if kp.MaxConnectionAgeGrace == 0 {
		kp.MaxConnectionAgeGrace = defaultMaxConnectionAgeGrace
	}
	if kp.Time == 0 {
		kp.Time = defaultServerKeepaliveTime
	}
	if kp.Timeout == 0 {
		kp.Timeout = defaultServerKeepaliveTimeout
	}
	if kp.Time != infinity {
		if err = syscall.SetTCPUserTimeout(rawConn, kp.Timeout); err != nil {
			return nil, connectionErrorf(false, err, "transport: failed to set TCP_USER_TIMEOUT: %v", err)
		}
	}
	kep := config.KeepalivePolicy
	if kep.MinTime == 0 {
		kep.MinTime = defaultKeepalivePolicyMinTime
	}

	done := make(chan struct{})
	peer := peer.Peer{
		Addr:      conn.RemoteAddr(),
		LocalAddr: conn.LocalAddr(),
		AuthInfo:  authInfo,
	}
	t := &http2Server{
		done:              done,
		conn:              conn,
		peer:              peer,
		framer:            framer,
		readerDone:        make(chan struct{}),
		loopyWriterDone:   make(chan struct{}),
		maxStreams:        config.MaxStreams,
		inTapHandle:       config.InTapHandle,
		fc:                &trInFlow{limit: uint32(icwz)},
		state:             reachable,
		activeStreams:     make(map[uint32]*Stream),
		stats:             config.StatsHandlers,
		kp:                kp,
		idle:              time.Now(),
		kep:               kep,
		initialWindowSize: iwz,
		bufferPool:        newBufferPool(),
	}
	var czSecurity credentials.ChannelzSecurityValue
	if au, ok := authInfo.(credentials.ChannelzSecurityInfo); ok {
		czSecurity = au.GetSecurityValue()
	}
	t.channelz = channelz.RegisterSocket(
		&channelz.Socket{
			SocketType:       channelz.SocketTypeNormal,
			Parent:           config.ChannelzParent,
			SocketMetrics:    channelz.SocketMetrics{},
			EphemeralMetrics: t.socketMetrics,
			LocalAddr:        t.peer.LocalAddr,
			RemoteAddr:       t.peer.Addr,
			SocketOptions:    channelz.GetSocketOption(t.conn),
			Security:         czSecurity,
		},
	)
	t.logger = prefixLoggerForServerTransport(t)

	t.controlBuf = newControlBuffer(t.done)
	if dynamicWindow {
		t.bdpEst = &bdpEstimator{
			bdp:               initialWindowSize,
			updateFlowControl: t.updateFlowControl,
		}
	}

	t.connectionID = atomic.AddUint64(&serverConnectionCounter, 1)
	t.framer.writer.Flush()

	defer func() {
		if err != nil {
			t.Close(err)
		}
	}()

	// Check the validity of client preface.
	preface := make([]byte, len(clientPreface))
	if _, err := io.ReadFull(t.conn, preface); err != nil {
		// In deployments where a gRPC server runs behind a cloud load balancer
		// which performs regular TCP level health checks, the connection is
		// closed immediately by the latter.  Returning io.EOF here allows the
		// grpc server implementation to recognize this scenario and suppress
		// logging to reduce spam.
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, connectionErrorf(false, err, "transport: http2Server.HandleStreams failed to receive the preface from client: %v", err)
	}
	if !bytes.Equal(preface, clientPreface) {
		return nil, connectionErrorf(false, nil, "transport: http2Server.HandleStreams received bogus greeting from client: %q", preface)
	}

	frame, err := t.framer.fr.ReadFrame()
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	if err != nil {
		return nil, connectionErrorf(false, err, "transport: http2Server.HandleStreams failed to read initial settings frame: %v", err)
	}
	atomic.StoreInt64(&t.lastRead, time.Now().UnixNano())
	sf, ok := frame.(*http2.SettingsFrame)
	if !ok {
		return nil, connectionErrorf(false, nil, "transport: http2Server.HandleStreams saw invalid preface type %T from client", frame)
	}
	t.handleSettings(sf)

	go func() {
		t.loopy = newLoopyWriter(serverSide, t.framer, t.controlBuf, t.bdpEst, t.conn, t.logger, t.outgoingGoAwayHandler)
		err := t.loopy.run()
		close(t.loopyWriterDone)
		if !isIOError(err) {
			// Close the connection if a non-I/O error occurs (for I/O errors
			// the reader will also encounter the error and close).  Wait 1
			// second before closing the connection, or when the reader is done
			// (i.e. the client already closed the connection or a connection
			// error occurred).  This avoids the potential problem where there
			// is unread data on the receive side of the connection, which, if
			// closed, would lead to a TCP RST instead of FIN, and the client
			// encountering errors.  For more info:
			// https://github.com/grpc/grpc-go/issues/5358
			timer := time.NewTimer(time.Second)
			defer timer.Stop()
			select {
			case <-t.readerDone:
			case <-timer.C:
			}
			t.conn.Close()
		}
	}()
	go t.keepalive()

	not_t := &not_http2Server{
		http2Server: t,
	}
	return not_t, nil
}
