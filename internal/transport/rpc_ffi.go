package transport

// #cgo CFLAGS: -I${SRCDIR}/notnets_shm/libnotnets/include
// #cgo LDFLAGS: -L${SRCDIR}/notnets_shm/libnotnets/bin  -lnotnets
// #include <stdio.h>
// #include <unistd.h>
// #include <sched.h>
// #include <stdlib.h>
// #include <errno.h>
// #include "rpc.h"
// #include "coord.h"
import "C"
import (
	"runtime"
	"unsafe"

	"github.com/rs/zerolog/log"
	"modernc.org/libc/pthread"
)

const MESSAGE_SIZE = 256

type QueueContext struct {
	queues *QueuePair
	qt     C.QUEUE_TYPE
	fn     unsafe.Pointer
	pinner runtime.Pinner

	ptr_ctx *C.queue_ctx
}

type QueuePair struct {
	ClientId        int
	RequestShmaddr  unsafe.Pointer
	ResponseShmaddr unsafe.Pointer
	Offset          int //Needed for boost queue
}

// server_context
type ServerContext struct {
	CoordShmaddr     uintptr
	ManagePoolThread pthread.Pthread_t
	ManagePoolState  int32
	ManagePoolMutex  pthread.Pthread_mutex_t
}

func ClientOpen(sourceAddr string, destinationAddr string, messageSize int32) (ret *QueueContext) {
	_sourceAddr := C.CString(sourceAddr)
	defer C.free(unsafe.Pointer(_sourceAddr))
	_destinationAddr := C.CString(destinationAddr)
	defer C.free(unsafe.Pointer(_destinationAddr))
	_messageSize := C.int(messageSize)
	_ret := C.client_open(_sourceAddr, _destinationAddr, _messageSize, C.POLL)
	log.Info().Msgf("Client: open response : %v \n ", _ret)
	if _ret == nil {
		return nil //Pass on null for retry
	}

	ret = &QueueContext{
		queues: &QueuePair{
			ClientId:        int(_ret.queues.client_id),
			RequestShmaddr:  _ret.queues.request_shmaddr,
			ResponseShmaddr: _ret.queues.response_shmaddr,
			Offset:          int(_ret.queues.offset),
		},
		qt:      _ret.queue_type,
		fn:      unsafe.Pointer(_ret.fn),
		ptr_ctx: _ret,
	}
	log.Info().Msgf("Client: open response ret : %v \n ", ret)
	C.fflush(C.stdout)
	return
}

// client_send_rpc
func (conn *QueueContext) ClientSendRpc(buf []byte, size int) (ret int32, err error) {

	_buf := unsafe.Pointer(&buf[0])
	_size := C.size_t(size)
	_ret, err := C.client_send_rpc(conn.ptr_ctx, _buf, _size)
	ret = int32(_ret)
	return ret, err
}

// client_receive_buf
// TODO: Make this behave like socket read.
// Return
func (conn *QueueContext) ClientReceiveBuf(buf []byte, size int) (ret int, err error) {
	_buf := unsafe.Pointer(&buf[0])
	_size := C.size_t(size)
	_ret, err := C.client_receive_buf(conn.ptr_ctx, _buf, _size)
	ret = int(_ret)
	return ret, err
}

// client_close
func ClientClose(sourceAddr string, destinationAddr string) (ret int32) {
	_sourceAddr := C.CString(sourceAddr)
	defer C.free(unsafe.Pointer(_sourceAddr))
	_destinationAddr := C.CString(destinationAddr)
	defer C.free(unsafe.Pointer(_destinationAddr))
	_ret := C.client_close(_sourceAddr, _destinationAddr)
	ret = int32(_ret)
	return
}

// register_server
func RegisterServer(sourceAddr string) (ret *ServerContext) {
	_sourceAddr := C.CString(sourceAddr)
	defer C.free(unsafe.Pointer(_sourceAddr))
	_ret := C.register_server(_sourceAddr)
	ret = (*ServerContext)(unsafe.Pointer(_ret))
	return
}

// accept
func (handler *ServerContext) Accept() (ret *QueueContext) {
	_handler := (*C.server_context)(unsafe.Pointer(handler))
	_ret := C.accept(_handler)
	log.Info().Msgf("Server: open response : %v \n ", _ret)
	if _ret == nil {
		//null case, return simple type conversion
		ret = (*QueueContext)(unsafe.Pointer(_ret))
		log.Info().Msgf("Server: open response ret old : %v \n ", ret)
		return ret
	}
	ret = &QueueContext{
		queues: &QueuePair{
			ClientId:        int(_ret.queues.client_id),
			RequestShmaddr:  _ret.queues.request_shmaddr,
			ResponseShmaddr: _ret.queues.response_shmaddr,
			Offset:          int(_ret.queues.offset),
		},
		qt:      _ret.queue_type,
		fn:      _ret.fn,
		ptr_ctx: _ret,
	}
	log.Info().Msgf("Server: open response ret new : %v \n ", ret)
	C.fflush(C.stdout)
	// C.free((unsafe.Pointer(_ret)))

	return ret
}

// manage_pool
func (handler *ServerContext) ManagePool() {
	_handler := (*C.server_context)(unsafe.Pointer(handler))
	C.manage_pool(_handler)
}

// shutdown
func (handler *ServerContext) Shutdown() {
	_handler := (*C.server_context)(unsafe.Pointer(handler))
	C.shutdown(_handler)
}

// server_receive_buf
func (conn *QueueContext) ServerReceiveBuf(buf []byte, size int) (ret int, err error) {
	_buf := unsafe.Pointer(&buf[0])
	_size := C.size_t(size)
	_ret, err := C.server_receive_buf(conn.ptr_ctx, _buf, _size)
	ret = int(_ret)
	return ret, err
}

// server_send_rpc
func (client *QueueContext) ServerSendRpc(buf []byte, size int) (ret int32, err error) {
	_buf := unsafe.Pointer(&buf[0])
	_size := C.size_t(size)
	_ret, err := C.server_send_rpc(client.ptr_ctx, _buf, _size)
	ret = int32(_ret)
	return ret, err
}
