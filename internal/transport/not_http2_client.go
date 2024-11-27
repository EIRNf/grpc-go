package transport

import (
	"context"
	"io"
	"net"
	"runtime"
	"sync"

	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/net/http2"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/internal/channelz"
	icredentials "google.golang.org/grpc/internal/credentials"
	"google.golang.org/grpc/internal/grpcsync"
	"google.golang.org/grpc/internal/grpcutil"
	imetadata "google.golang.org/grpc/internal/metadata"
	isyscall "google.golang.org/grpc/internal/syscall"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/stats"
)

type NotnetsAddr struct {
	basic string
	// IP   net.IP
	// Port int
}

func (addr *NotnetsAddr) Network() string {
	return "notnets"
}

func (addr *NotnetsAddr) String() string {
	return "notnets:" + addr.basic
}

type not_http2Client struct {
	*http2Client
}

func notnets_dial(ctx context.Context, fn func(context.Context, string) (net.Conn, error), addr resolver.Address, useProxy bool, grpcUA string) (net.Conn, error) {

	address := addr.Addr

	conn := &NotnetsConn{
		ClientSide:  true,
		local_addr:  &NotnetsAddr{basic: address}, //TODO FIX
		remote_addr: &NotnetsAddr{basic: address},
		connectCtx:  &ctx,
	}
	conn.isConnected = false
	conn.message_size = 1024 //TODO FIX

	var tempDelay time.Duration
	if logger.V(logLevel) {
		logger.Infof("Client: Opening New Channel %s,%s\n", conn.local_addr, conn.remote_addr)
	}
	conn.queues = ClientOpen(conn.local_addr.String(), conn.remote_addr.String(), conn.message_size) //TODO FIX

	if conn.queues == nil { //if null means server doesn't exist yet
		for {
			if logger.V(logLevel) {
				logger.Infof("Client: Opening New Channel Failed: Try Again\n")
			}

			//Reattempt wit backoff
			if tempDelay == 0 {
				tempDelay = 3 * time.Second
			} else {
				tempDelay *= 2
			}
			if max := 25 * time.Second; tempDelay > max {
				tempDelay = max
			}
			timer := time.NewTimer(tempDelay)
			<-timer.C
			conn.queues = ClientOpen(conn.local_addr.String(), conn.remote_addr.String(), conn.message_size)
			if conn.queues != nil {
				break
			}
		}
	}

	if logger.V(logLevel) {
		logger.Infof("Client: New Channel: %v \n ", conn.queues.queues.ClientId)
		logger.Infof("Client: New Channel RequestShmid: %v \n ", conn.queues.queues.RequestShmaddr)
		logger.Infof("Client: New Channel RespomseShmid: %v \n ", conn.queues.queues.ResponseShmaddr)

	}
	conn.isConnected = true
	return conn, nil
}

func newNotHTTP2Client(connectCtx, ctx context.Context, addr resolver.Address, opts ConnectOptions, onClose func(GoAwayReason)) (_ *not_http2Client, err error) {
	scheme := "http"
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		if err != nil {
			cancel()
		}
	}()

	// gRPC, resolver, balancer etc. can specify arbitrary data in the
	// Attributes field of resolver.Address, which is shoved into connectCtx
	// and passed to the dialer and credential handshaker. This makes it possible for
	// address specific arbitrary data to reach custom dialers and credential handshakers.
	connectCtx = icredentials.NewClientHandshakeInfoContext(connectCtx, credentials.ClientHandshakeInfo{Attributes: addr.Attributes})

	conn, err := notnets_dial(connectCtx, opts.Dialer, addr, opts.UseProxy, opts.UserAgent)
	if err != nil {
		if opts.FailOnNonTempDialError {
			return nil, connectionErrorf(isTemporary(err), err, "transport: error while dialing: %v", err)
		}
		return nil, connectionErrorf(true, err, "transport: Error while dialing: %v", err)
	}

	// Any further errors will close the underlying connection
	defer func(conn net.Conn) {
		if err != nil {
			conn.Close()
		}
	}(conn)

	// The following defer and goroutine monitor the connectCtx for cancelation
	// and deadline.  On context expiration, the connection is hard closed and
	// this function will naturally fail as a result.  Otherwise, the defer
	// waits for the goroutine to exit to prevent the context from being
	// monitored (and to prevent the connection from ever being closed) after
	// returning from this function.
	ctxMonitorDone := grpcsync.NewEvent()
	newClientCtx, newClientDone := context.WithCancel(connectCtx)
	defer func() {
		newClientDone()         // Awaken the goroutine below if connectCtx hasn't expired.
		<-ctxMonitorDone.Done() // Wait for the goroutine below to exit.
	}()
	go func(conn net.Conn) {
		defer ctxMonitorDone.Fire() // Signal this goroutine has exited.
		<-newClientCtx.Done()       // Block until connectCtx expires or the defer above executes.
		if err := connectCtx.Err(); err != nil {
			// connectCtx expired before exiting the function.  Hard close the connection.
			if logger.V(logLevel) {
				logger.Infof("Aborting due to connect deadline expiring: %v", err)
			}
			conn.Close()
		}
	}(conn)

	kp := opts.KeepaliveParams
	// Validate keepalive parameters.
	if kp.Time == 0 {
		kp.Time = defaultClientKeepaliveTime
	}
	if kp.Timeout == 0 {
		kp.Timeout = defaultClientKeepaliveTimeout
	}
	keepaliveEnabled := false
	if kp.Time != infinity {
		if err = isyscall.SetTCPUserTimeout(conn, kp.Timeout); err != nil {
			return nil, connectionErrorf(false, err, "transport: failed to set TCP_USER_TIMEOUT: %v", err)
		}
		keepaliveEnabled = true
	}
	var (
		isSecure bool
		authInfo credentials.AuthInfo
	)
	transportCreds := opts.TransportCredentials
	perRPCCreds := opts.PerRPCCredentials

	if b := opts.CredsBundle; b != nil {
		if t := b.TransportCredentials(); t != nil {
			transportCreds = t
		}
		if t := b.PerRPCCredentials(); t != nil {
			perRPCCreds = append(perRPCCreds, t)
		}
	}
	if transportCreds != nil {
		conn, authInfo, err = transportCreds.ClientHandshake(connectCtx, addr.ServerName, conn)
		if err != nil {
			return nil, connectionErrorf(isTemporary(err), err, "transport: authentication handshake failed: %v", err)
		}
		for _, cd := range perRPCCreds {
			if cd.RequireTransportSecurity() {
				if ci, ok := authInfo.(interface {
					GetCommonAuthInfo() credentials.CommonAuthInfo
				}); ok {
					secLevel := ci.GetCommonAuthInfo().SecurityLevel
					if secLevel != credentials.InvalidSecurityLevel && secLevel < credentials.PrivacyAndIntegrity {
						return nil, connectionErrorf(true, nil, "transport: cannot send secure credentials on an insecure connection")
					}
				}
			}
		}
		isSecure = true
		if transportCreds.Info().SecurityProtocol == "tls" {
			scheme = "https"
		}
	}
	dynamicWindow := true
	icwz := int32(initialWindowSize)
	if opts.InitialConnWindowSize >= defaultWindowSize {
		icwz = opts.InitialConnWindowSize
		dynamicWindow = false
	}
	writeBufSize := opts.WriteBufferSize
	readBufSize := opts.ReadBufferSize
	maxHeaderListSize := defaultClientMaxHeaderListSize
	if opts.MaxHeaderListSize != nil {
		maxHeaderListSize = *opts.MaxHeaderListSize
	}

	/// TODO
	t := &http2Client{
		ctx:                   ctx,
		ctxDone:               ctx.Done(), // Cache Done chan.
		cancel:                cancel,
		userAgent:             opts.UserAgent,
		registeredCompressors: grpcutil.RegisteredCompressors(),
		address:               addr,
		conn:                  conn,
		remoteAddr:            conn.RemoteAddr(),
		localAddr:             conn.LocalAddr(),
		authInfo:              authInfo,
		readerDone:            make(chan struct{}),
		writerDone:            make(chan struct{}),
		goAway:                make(chan struct{}),
		framer:                newFramer(conn, writeBufSize, readBufSize, opts.SharedWriteBuffer, maxHeaderListSize),
		fc:                    &trInFlow{limit: uint32(icwz)},
		scheme:                scheme,
		activeStreams:         make(map[uint32]*Stream),
		isSecure:              isSecure,
		perRPCCreds:           perRPCCreds,
		kp:                    kp,
		statsHandlers:         opts.StatsHandlers,
		initialWindowSize:     initialWindowSize,
		nextID:                1,
		maxConcurrentStreams:  defaultMaxStreamsClient,
		streamQuota:           defaultMaxStreamsClient,
		streamsQuotaAvailable: make(chan struct{}, 1),
		keepaliveEnabled:      keepaliveEnabled,
		bufferPool:            newBufferPool(),
		onClose:               onClose,
	}
	var czSecurity credentials.ChannelzSecurityValue
	if au, ok := authInfo.(credentials.ChannelzSecurityInfo); ok {
		czSecurity = au.GetSecurityValue()
	}
	t.channelz = channelz.RegisterSocket(
		&channelz.Socket{
			SocketType:       channelz.SocketTypeNormal,
			Parent:           opts.ChannelzParent,
			SocketMetrics:    channelz.SocketMetrics{},
			EphemeralMetrics: t.socketMetrics,
			LocalAddr:        t.localAddr,
			RemoteAddr:       t.remoteAddr,
			SocketOptions:    channelz.GetSocketOption(t.conn),
			Security:         czSecurity,
		})
	t.logger = prefixLoggerForClientTransport(t)
	// Add peer information to the http2client context.
	t.ctx = peer.NewContext(t.ctx, t.getPeer())

	if md, ok := addr.Metadata.(*metadata.MD); ok {
		t.md = *md
	} else if md := imetadata.Get(addr); md != nil {
		t.md = md
	}
	t.controlBuf = newControlBuffer(t.ctxDone)
	if opts.InitialWindowSize >= defaultWindowSize {
		t.initialWindowSize = opts.InitialWindowSize
		dynamicWindow = false
	}
	if dynamicWindow {
		t.bdpEst = &bdpEstimator{
			bdp:               initialWindowSize,
			updateFlowControl: t.updateFlowControl,
		}
	}
	for _, sh := range t.statsHandlers {
		t.ctx = sh.TagConn(t.ctx, &stats.ConnTagInfo{
			RemoteAddr: t.remoteAddr,
			LocalAddr:  t.localAddr,
		})
		connBegin := &stats.ConnBegin{
			Client: true,
		}
		sh.HandleConn(t.ctx, connBegin)
	}
	if t.keepaliveEnabled {
		t.kpDormancyCond = sync.NewCond(&t.mu)
		go t.keepalive()
	}

	// Start the reader goroutine for incoming messages. Each transport has a
	// dedicated goroutine which reads HTTP2 frames from the network. Then it
	// dispatches the frame to the corresponding stream entity.  When the
	// server preface is received, readerErrCh is closed.  If an error occurs
	// first, an error is pushed to the channel.  This must be checked before
	// returning from this function.
	readerErrCh := make(chan error, 1)
	go t.reader(readerErrCh)
	defer func() {
		if err != nil {
			// writerDone should be closed since the loopy goroutine
			// wouldn't have started in the case this function returns an error.
			close(t.writerDone)
			t.Close(err)
		}
	}()

	// Send connection preface to server.
	n, err := t.conn.Write(clientPreface)
	if err != nil {
		err = connectionErrorf(true, err, "transport: failed to write client preface: %v", err)
		return nil, err
	}
	if n != len(clientPreface) {
		err = connectionErrorf(true, nil, "transport: preface mismatch, wrote %d bytes; want %d", n, len(clientPreface))
		return nil, err
	}
	var ss []http2.Setting

	if t.initialWindowSize != defaultWindowSize {
		ss = append(ss, http2.Setting{
			ID:  http2.SettingInitialWindowSize,
			Val: uint32(t.initialWindowSize),
		})
	}
	if opts.MaxHeaderListSize != nil {
		ss = append(ss, http2.Setting{
			ID:  http2.SettingMaxHeaderListSize,
			Val: *opts.MaxHeaderListSize,
		})
	}
	err = t.framer.fr.WriteSettings(ss...)
	if err != nil {
		err = connectionErrorf(true, err, "transport: failed to write initial settings frame: %v", err)
		return nil, err
	}
	// Adjust the connection flow control window if needed.
	if delta := uint32(icwz - defaultWindowSize); delta > 0 {
		if err := t.framer.fr.WriteWindowUpdate(0, delta); err != nil {
			err = connectionErrorf(true, err, "transport: failed to write window update: %v", err)
			return nil, err
		}
	}

	t.connectionID = atomic.AddUint64(&clientConnectionCounter, 1)

	if err := t.framer.writer.Flush(); err != nil {
		return nil, err
	}
	// Block until the server preface is received successfully or an error occurs.
	if err = <-readerErrCh; err != nil {
		return nil, err
	}
	go func() {
		t.loopy = newLoopyWriter(clientSide, t.framer, t.controlBuf, t.bdpEst, t.conn, t.logger, t.outgoingGoAwayHandler)
		if err := t.loopy.run(); !isIOError(err) {
			// Immediately close the connection, as the loopy writer returns
			// when there are no more active streams and we were draining (the
			// server sent a GOAWAY).  For I/O errors, the reader will hit it
			// after draining any remaining incoming data.
			t.conn.Close()
		}
		close(t.writerDone)
	}()

	not_t := &not_http2Client{
		http2Client: t,
	}

	return not_t, nil
}

type NotnetsConn struct {
	ClientSide  bool
	isConnected bool
	message_size int32


	read_mu        sync.Mutex
	write_mu       sync.Mutex
	queues         *QueueContext
	connectCtx     *context.Context
	local_addr     net.Addr
	remote_addr    net.Addr
	deadline       time.Time
	read_deadline  time.Time
	write_deadline time.Time
}

func (c *NotnetsConn) ok() bool { return c != nil && c.queues != nil }

func (c *NotnetsConn) internalRead(b []byte) (n int, err error) {

	//Supposed to prevent the releasing of resources by certain configured functions? See fd_posix.go
	runtime.KeepAlive(c) //TODO: What does this do?

	//Handle not ready read
	if c.queues == nil {
		return 0, nil
	}

	if len(b) == 0 {
		// If the caller wanted a zero byte read, return immediately
		// without trying (but after acquiring the readLock).
		// Otherwise syscall.Read returns 0, nil which looks like
		// io.EOF.
		return 0, nil
	}

	if c.ClientSide {
		n = c.queues.ClientReceiveBuf(b, len(b))

	} else { //Server read
		n = c.queues.ServerReceiveBuf(b, len(b))
	}

	return n, nil 
}

// TODO: Error handling, timeouts
func (c *NotnetsConn) Read(b []byte) (n int, err error) {
	if !c.ok() {
		return 0, syscall.EINVAL
	}

	c.read_mu.Lock()
	n, err = c.internalRead(b)
	c.read_mu.Unlock()

	if err != nil && err != io.EOF {
		err = &net.OpError{Op: "read", Net: c.local_addr.Network(), Source: c.LocalAddr(), Addr: c.RemoteAddr(), Err: err}
	}
	return n, err
}

// TODO: Error handling, timeouts
func (c *NotnetsConn) Write(b []byte) (n int, err error) {
	c.write_mu.Lock()

	runtime.KeepAlive(c)

	var size int32
	if c.ClientSide {
		size = c.queues.ClientSendRpc(b, len(b))
	} else { //Server write
		size = c.queues.ServerSendRpc(b, len(b))
	}
	if err != nil {
		return int(size), err
	}
	if size == 0 {
		return int(size), io.ErrUnexpectedEOF
	}
	c.write_mu.Unlock()
	return int(size), err
}

// TODO: Error handling, timeouts
func (c *NotnetsConn) Close() error {
	runtime.SetFinalizer(c, nil)
	var err error
	ret := ClientClose(c.local_addr.String(), c.remote_addr.String())
	// Error closing
	if ret == -1 {
		// log.Fatalf()
		return err
	}
	return nil
}

func (c *NotnetsConn) setAddr(laddr, raddr net.Addr) {
	c.local_addr = laddr
	c.remote_addr = raddr
	runtime.SetFinalizer(c, c.Close())
}

func (c *NotnetsConn) LocalAddr() net.Addr {
	return c.local_addr
}

func (c *NotnetsConn) RemoteAddr() net.Addr {
	return c.remote_addr
}

// TODO: Error handling, timeouts
func (c *NotnetsConn) SetDeadline(t time.Time) error {
	c.deadline = t
	return nil
}

// TODO: Error handling, timeouts
func (c *NotnetsConn) SetReadDeadline(t time.Time) error {
	c.read_deadline = t
	return nil
}

// TODO: Error handling, timeouts
func (c *NotnetsConn) SetWriteDeadline(t time.Time) error {
	c.write_deadline = t
	return nil
}
