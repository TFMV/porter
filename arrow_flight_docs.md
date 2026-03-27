package flight // import "github.com/apache/arrow-go/v18/arrow/flight"


CONSTANTS

const (
	DescriptorUNKNOWN = flight.FlightDescriptor_UNKNOWN
	DescriptorPATH    = flight.FlightDescriptor_PATH
	DescriptorCMD     = flight.FlightDescriptor_CMD
)
const (
	CancelFlightInfoActionType    = "CancelFlightInfo"
	RenewFlightEndpointActionType = "RenewFlightEndpoint"
	SetSessionOptionsActionType   = "SetSessionOptions"
	GetSessionOptionsActionType   = "GetSessionOptions"
	CloseSessionActionType        = "CloseSession"
)
    Constants for Action types

const (
	// The set option error is unknown. Servers should avoid
	// using this value (send a NOT_FOUND error if the requested
	// FlightInfo is not known). Clients can retry the request.
	SetSessionOptionsResultErrorUnspecified = flight.SetSessionOptionsResult_UNSPECIFIED
	// The given session option name is invalid.
	SetSessionOptionsResultErrorInvalidName = flight.SetSessionOptionsResult_INVALID_NAME
	// The session option value or type is invalid.
	SetSessionOptionsResultErrorInvalidValue = flight.SetSessionOptionsResult_INVALID_VALUE
	// The session option cannot be set.
	SetSessionOptionsResultErrorError = flight.SetSessionOptionsResult_ERROR
)
const (
	// The close session status is unknown. Servers should avoid
	// using this value (send a NOT_FOUND error if the requested
	// FlightInfo is not known). Clients can retry the request.
	CloseSessionResultUnspecified = flight.CloseSessionResult_UNSPECIFIED
	// The session close request is complete.
	CloseSessionResultClosed = flight.CloseSessionResult_CLOSED
	// The session close request is in progress. The client may retry the request.
	CloseSessionResultClosing = flight.CloseSessionResult_CLOSING
	// The session is not closeable.
	CloseSessionResultNotCloseable = flight.CloseSessionResult_NOT_CLOSEABLE
)
const (
	// The cancellation status is unknown. Servers should avoid
	// using this value (send a NOT_FOUND error if the requested
	// FlightInfo is not known). Clients can retry the request.
	CancelStatusUnspecified = flight.CancelStatus_CANCEL_STATUS_UNSPECIFIED
	// The cancellation request is complete. Subsequent requests
	// with the same payload may return CancelStatusCancelled or a
	// arrow.ErrNotFound error.
	CancelStatusCancelled = flight.CancelStatus_CANCEL_STATUS_CANCELLED
	// The cancellation request is in progress. The client may
	// retry the cancellation request.
	CancelStatusCancelling = flight.CancelStatus_CANCEL_STATUS_CANCELLING
	// The FlightInfo is not cancellable. The client should not
	// retry the cancellation request.
	CancelStatusNotCancellable = flight.CancelStatus_CANCEL_STATUS_NOT_CANCELLABLE
)
    Constants for CancelStatus

const (
	// LocationReuseConnection is a special location that tells clients
	// they may fetch the data from the same service that they obtained
	// the FlightEndpoint response from.
	LocationReuseConnection = "arrow-flight-reuse-connection://?"
)
    Constants for Location


VARIABLES

var NewFlightServiceClient = flight.NewFlightServiceClient

FUNCTIONS

func AuthFromContext(ctx context.Context) interface{}
    AuthFromContext will return back whatever object was returned from `IsValid`
    for a given request context allowing handlers to retrieve identifying
    information for the current request for use.

func ConcatenateReaders(rdrs []array.RecordReader, ch chan<- StreamChunk)
func CreateServerBearerTokenAuthInterceptors(validator BasicAuthValidator) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor)
    CreateServerBearerTokenAuthInterceptors returns grpc interceptors for basic
    auth handling via bearer tokens. validator cannot be nil

    Deprecated: use CreateServerBasicAuthMiddleware instead

func DeserializeSchema(info []byte, mem memory.Allocator) (*arrow.Schema, error)
    DeserializeSchema takes the schema bytes from FlightInfo or SchemaResult and
    returns the deserialized arrow schema.

func NewSessionOptionValue(value any) (flight.SessionOptionValue, error)
    NewSessionOptionValue takes any value and constructs a SessionOptionValue
    suitable for setting session values. An error will be returned if the value
    is not one of the types supported by SessionOptionValue.

func NewSessionOptionValues(options map[string]any) (map[string]*flight.SessionOptionValue, error)
    NewSessionOptionValues returns a map with the same keys as the input map,
    but with all values converted to SessionOptionValues. If any values fail
    conversion, an error will be returned.

func ReadUntilEOF(stream FlightService_DoActionClient) error
    ReadUntilEOF will drain a stream until either an error is returned or EOF is
    encountered and nil is returned.

func RegisterFlightServiceServer(s *grpc.Server, srv FlightServer)
    RegisterFlightServiceServer registers an existing flight server onto an
    existing grpc server, or anything that is a grpc service registrar.

func SerializeSchema(rec *arrow.Schema, mem memory.Allocator) []byte
    SerializeSchema returns the serialized schema bytes for use in Arrow Flight
    protobuf messages.

func StreamChunksFromReader(rdr array.RecordReader, ch chan<- StreamChunk)
    StreamChunksFromReader is a convenience function to populate a channel from
    a record reader. It is intended to be run using a separate goroutine by
    calling `go flight.StreamChunksFromReader(rdr, ch)`.

    If the record reader panics, an error chunk will get sent on the channel.

    This will close the channel and release the reader when it completes.


TYPES

type Action = flight.Action

type ActionType = flight.ActionType

type AuthConn interface {
	Read() ([]byte, error)
	Send([]byte) error
}
    AuthConn wraps the stream from grpc for handshakes to simplify handling
    handshake request and response from the flight.proto forwarding just the
    payloads and errors instead of having to deal with the handshake request and
    response protos directly

type BaseFlightServer struct {
	flight.UnimplementedFlightServiceServer
	// Has unexported fields.
}
    BaseFlightServer is the base flight server implementation and must be
    embedded in any server implementation to ensure forward compatibility with
    any modifications of the spec without compiler errors.

func (s *BaseFlightServer) GetAuthHandler() ServerAuthHandler

func (s *BaseFlightServer) Handshake(stream flight.FlightService_HandshakeServer) error

func (s *BaseFlightServer) SetAuthHandler(handler ServerAuthHandler)

type BasicAuth = flight.BasicAuth

type BasicAuthValidator interface {
	Validate(username, password string) (string, error)
	IsValid(bearerToken string) (interface{}, error)
}

type CancelFlightInfoRequest = flight.CancelFlightInfoRequest

type CancelFlightInfoResult = flight.CancelFlightInfoResult

type CancelStatus = flight.CancelStatus

type Client interface {
	// Authenticate uses the ClientAuthHandler that was used when creating the client
	// in order to use the Handshake endpoints of the service.
	Authenticate(context.Context, ...grpc.CallOption) error
	AuthenticateBasicToken(ctx context.Context, username string, password string, opts ...grpc.CallOption) (context.Context, error)
	CancelFlightInfo(ctx context.Context, request *CancelFlightInfoRequest, opts ...grpc.CallOption) (*CancelFlightInfoResult, error)
	Close() error
	RenewFlightEndpoint(ctx context.Context, request *RenewFlightEndpointRequest, opts ...grpc.CallOption) (*FlightEndpoint, error)
	SetSessionOptions(ctx context.Context, request *SetSessionOptionsRequest, opts ...grpc.CallOption) (*SetSessionOptionsResult, error)
	GetSessionOptions(ctx context.Context, request *GetSessionOptionsRequest, opts ...grpc.CallOption) (*GetSessionOptionsResult, error)
	CloseSession(ctx context.Context, request *CloseSessionRequest, opts ...grpc.CallOption) (*CloseSessionResult, error)
	// join the interface from the FlightServiceClient instead of re-defining all
	// the endpoints here.
	FlightServiceClient
}
    Client is an interface wrapped around the generated FlightServiceClient
    which is generated by grpc protobuf definitions. This interface provides a
    useful hiding of the authentication handshake via calling Authenticate and
    using the ClientAuthHandler rather than manually having to implement the
    grpc communication and sending of the auth token.

func NewClientFromConn(cc grpc.ClientConnInterface, auth ClientAuthHandler) Client

func NewClientWithMiddleware(addr string, auth ClientAuthHandler, middleware []ClientMiddleware, opts ...grpc.DialOption) (Client, error)
    NewClientWithMiddleware takes a slice of middleware in addition to the auth
    and address which will be used by grpc and chained, the first middleware
    will be the outer most with the last middleware being the inner most wrapper
    around the actual call. It also passes along the dialoptions passed in such
    as TLS certs and so on.

func NewClientWithMiddlewareCtx(ctx context.Context, addr string, auth ClientAuthHandler, middleware []ClientMiddleware, opts ...grpc.DialOption) (Client, error)

func NewFlightClient(addr string, auth ClientAuthHandler, opts ...grpc.DialOption) (Client, error)
    NewFlightClient takes in the address of the grpc server and an auth
    handler for the application-level handshake. If using TLS or other grpc
    configurations they can still be passed via the grpc.DialOption list just as
    if connecting manually without this helper function.

    Alternatively, a grpc client can be constructed as normal without this
    helper as the grpc generated client code is still exported. This exists to
    add utility and helpers around the authentication and passing the token with
    requests.

    Deprecated: prefer to use NewClientWithMiddleware

type ClientAuthHandler interface {
	Authenticate(context.Context, AuthConn) error
	GetToken(context.Context) (string, error)
}
    ClientAuthHandler defines an interface for the Flight client to perform
    the authentication handshake. The token that is retrieved from GetToken
    will be sent as part of the context metadata in subsequent requests after
    authentication is performed using the key "auth-token-bin".

type ClientHeadersMiddleware interface {
	HeadersReceived(ctx context.Context, md metadata.MD)
}

type ClientMiddleware struct {
	Stream grpc.StreamClientInterceptor
	Unary  grpc.UnaryClientInterceptor
}

func CreateClientMiddleware(middleware CustomClientMiddleware) ClientMiddleware

func NewClientCookieMiddleware() ClientMiddleware
    NewClientCookieMiddleware returns a go-routine safe middleware for flight
    clients which properly handles Set-Cookie headers to store cookies in a
    cookie jar, and then requests are sent with those cookies added as a Cookie
    header.

type ClientPostCallMiddleware interface {
	CallCompleted(ctx context.Context, err error)
}

type CloseSessionRequest = flight.CloseSessionRequest

type CloseSessionResult = flight.CloseSessionResult

type CookieMiddleware interface {
	CustomClientMiddleware
	// Clone creates a new CookieMiddleware that starts out with the same
	// cookies that this one already has. This is useful when creating a
	// new client connection for the same server.
	Clone() CookieMiddleware
}
    CookieMiddleware is a go-routine safe middleware for flight clients which
    properly handles Set-Cookie headers for storing cookies. This can be passed
    into `CreateClientMiddleware` to create a new middleware object. You can
    also clone it to create middleware for a new client which starts with the
    same cookies.

func NewCookieMiddleware() CookieMiddleware

type Criteria = flight.Criteria

type CustomClientMiddleware interface {
	StartCall(ctx context.Context) context.Context
}

type CustomServerMiddleware interface {
	// StartCall will be called with the current context of the call, grpc.SetHeader can be used to add outgoing headers
	// if the returned context is non-nil, then it will be used as the new context being passed through the calls
	StartCall(ctx context.Context) context.Context
	// CallCompleted is a callback which is called with the return from the handler
	// it will be nil if everything was successful or will be the error about to be returned
	// to grpc
	CallCompleted(ctx context.Context, err error)
}
    CustomerServerMiddleware is a helper interface for more easily defining
    custom grpc middleware without having to expose or understand all the grpc
    bells and whistles.

type DataStreamReader interface {
	Recv() (*FlightData, error)
}
    DataStreamReader is an interface for receiving flight data messages on a
    stream such as via grpc with Arrow Flight.

type DataStreamWriter interface {
	Send(*FlightData) error
}
    DataStreamWriter is an interface that represents an Arrow Flight stream
    writer that writes FlightData objects

type DescriptorType = flight.FlightDescriptor_DescriptorType

type Empty = flight.Empty

type FlightData = flight.FlightData

type FlightDescriptor = flight.FlightDescriptor

type FlightEndpoint = flight.FlightEndpoint

type FlightInfo = flight.FlightInfo

type FlightServer = flight.FlightServiceServer

type FlightServiceClient = flight.FlightServiceClient

type FlightService_DoActionClient = flight.FlightService_DoActionClient

type FlightService_DoActionServer = flight.FlightService_DoActionServer

type FlightService_DoExchangeClient = flight.FlightService_DoExchangeClient

type FlightService_DoExchangeServer = flight.FlightService_DoExchangeServer

type FlightService_DoGetClient = flight.FlightService_DoGetClient

type FlightService_DoGetServer = flight.FlightService_DoGetServer

type FlightService_DoPutClient = flight.FlightService_DoPutClient

type FlightService_DoPutServer = flight.FlightService_DoPutServer

type FlightService_HandshakeClient = flight.FlightService_HandshakeClient

type FlightService_HandshakeServer = flight.FlightService_HandshakeServer

type FlightService_ListActionsClient = flight.FlightService_ListActionsClient

type FlightService_ListActionsServer = flight.FlightService_ListActionsServer

type FlightService_ListFlightsClient = flight.FlightService_ListFlightsClient

type FlightService_ListFlightsServer = flight.FlightService_ListFlightsServer

type GetSessionOptionsRequest = flight.GetSessionOptionsRequest

type GetSessionOptionsResult = flight.GetSessionOptionsResult

type HandshakeRequest = flight.HandshakeRequest

type HandshakeResponse = flight.HandshakeResponse

type Location = flight.Location

type MessageReader interface {
	array.RecordReader
	arrio.Reader
	Err() error
	Chunk() StreamChunk
	LatestFlightDescriptor() *FlightDescriptor
	LatestAppMetadata() []byte
}
    MessageReader is an interface representing a RecordReader that also
    provides StreamChunks and/or the ability to retrieve FlightDescriptors and
    AppMetadata from the flight stream

type MetadataWriter interface {
	WriteMetadata([]byte) error
}

type PollInfo = flight.PollInfo

type PutResult = flight.PutResult

type Reader struct {
	*ipc.Reader
	// Has unexported fields.
}
    Reader is an ipc.Reader which also keeps track of the metadata from the
    FlightData messages as they come in, calling LatestAppMetadata will return
    the metadata bytes from the most recently read message.

func NewRecordReader(r DataStreamReader, opts ...ipc.Option) (*Reader, error)
    NewRecordReader constructs an ipc reader using the flight data stream
    reader as the source of the ipc messages, opts passed will be passed to the
    underlying ipc.Reader such as ipc.WithSchema and ipc.WithAllocator

func (r *Reader) Chunk() StreamChunk
    Chunk is a convenience function to return a chunk of the flight stream
    returning the RecordBatch along with the FlightDescriptor and any
    AppMetadata. Each of these can be retrieved separately with their respective
    functions, this is just a convenience to retrieve all three with one
    function call.

func (r *Reader) LatestAppMetadata() []byte
    LatestAppMetadata returns the bytes from the AppMetadata field of the most
    recently read FlightData message that was processed by calling the Next
    function. The metadata returned would correspond to the record retrieved by
    calling RecordBatch().

func (r *Reader) LatestFlightDescriptor() *FlightDescriptor
    LatestFlightDescriptor returns a pointer to the last FlightDescriptor
    object that was received in the most recently read FlightData message that
    was processed by calling the Next function. The descriptor returned would
    correspond to the record batch retrieved by calling RecordBatch().

func (r *Reader) Release()
    Release reduces the reference count for the underlying message reader and
    ipc.Reader, when the reference counts become zero, the allocated memory is
    released for the stored record and metadata.

func (r *Reader) Retain()
    Retain increases the reference count for the underlying message reader and
    ipc.Reader which are utilized by this Reader.

type RenewFlightEndpointRequest = flight.RenewFlightEndpointRequest

type Result = flight.Result

type SchemaResult = flight.SchemaResult

type Server interface {
	// Init takes in the address to bind to and creates the listener. If both this
	// and InitListener are called, then whichever was called last will be used.
	Init(addr string) error
	// InitListener initializes with an already created listener rather than
	// creating a new one like Init does. If both this and Init are called,
	// whichever was called last is what will be used as they both set a listener
	// into the server.
	InitListener(lis net.Listener)
	// Addr will return the address that was bound to for the service to listen on
	Addr() net.Addr
	// SetShutdownOnSignals sets notifications on the given signals to call GracefulStop
	// on the grpc service if any of those signals are received
	SetShutdownOnSignals(sig ...os.Signal)
	// Serve blocks until accepting a connection fails with a fatal error. It will return
	// a non-nil error unless it stopped due to calling Shutdown or receiving one of the
	// signals set in SetShutdownOnSignals
	Serve() error
	// Shutdown will call GracefulStop on the grpc server so that it stops accepting connections
	// and will wait until current methods complete
	Shutdown()
	// RegisterFlightService sets up the handler for the Flight Endpoints as per
	// normal Grpc setups
	RegisterFlightService(FlightServer)
	// ServiceRegistrar wraps a single method that supports service registration.
	// For example, it may be used to register health check provided by grpc-go.
	grpc.ServiceRegistrar

	// Has unexported methods.
}
    Server is an interface for hiding some of the grpc specifics to make it
    slightly easier to manage a flight service, slightly modeled after the C++
    implementation

func NewFlightServer(opt ...grpc.ServerOption) Server
    NewFlightServer takes any grpc Server options desired, such as TLS certs and
    so on which will just be passed through to the underlying grpc server.

    Alternatively, a grpc server can be created normally without this helper as
    the grpc server generated code is still being exported. This only exists to
    allow the utility of the helpers

    Deprecated: prefer to use NewServerWithMiddleware, due to auth handler
    middleware this function will be problematic if any of the grpc options
    specify other middleware.

func NewServerWithMiddleware(middleware []ServerMiddleware, opts ...grpc.ServerOption) Server
    NewServerWithMiddleware takes a slice of middleware which will be used
    by grpc and chained, the first middleware will be the outer most with
    the last middleware being the inner most wrapper around the actual call.
    It also takes any grpc Server options desired, such as TLS certs and so on
    which will just be passed through to the underlying grpc server.

    Because of the usage of `ChainStreamInterceptor` and `ChainUnaryInterceptor`
    do not specify any middleware using the grpc options, use the
    ServerMiddleware slice instead as the auth middleware will be added for
    handling the case that a service handler is registered that uses the
    ServerAuthHandler.

    Alternatively, a grpc server can be created normally without this helper as
    the grpc server generated code is still being exported. This only exists to
    allow the utility of the helpers.

type ServerAuthHandler interface {
	Authenticate(AuthConn) error
	IsValid(token string) (interface{}, error)
}
    ServerAuthHandler defines an interface for the server to perform the
    handshake. The token is expected to be sent as part of the context metadata
    in subsequent requests with a key of "auth-token-bin" which will then call
    IsValid to validate

type ServerMiddleware struct {
	Stream grpc.StreamServerInterceptor
	Unary  grpc.UnaryServerInterceptor
}

func CreateServerBasicAuthMiddleware(validator BasicAuthValidator) ServerMiddleware
    CreateServerBasicAuthMiddleware returns a ServerMiddleware that can be
    passed to NewServerWithMiddleware in order to automatically add interceptors
    which will properly enforce auth validation as per the passed in
    BasicAuthValidator.

    validator cannot be nil.

func CreateServerMiddleware(middleware CustomServerMiddleware) ServerMiddleware
    CreateServerMiddlware constructs a ServerMiddleware object for the passed
    in custom middleware, generating both the Unary and Stream interceptors from
    the interface.

type SessionOptionValue = flight.SessionOptionValue

type SetSessionOptionsRequest = flight.SetSessionOptionsRequest

type SetSessionOptionsResult = flight.SetSessionOptionsResult

type SetSessionOptionsResultError = flight.SetSessionOptionsResult_Error

type StreamChunk struct {
	Data        arrow.RecordBatch
	Desc        *FlightDescriptor
	AppMetadata []byte
	Err         error
}
    StreamChunk represents a single chunk of a FlightData stream

type Ticket = flight.Ticket

type Writer struct {
	*ipc.Writer
	// Has unexported fields.
}
    Writer is an ipc.Writer which also adds a WriteWithAppMetadata function
    in order to allow adding AppMetadata to the FlightData messages which are
    written.

func NewRecordWriter(w DataStreamWriter, opts ...ipc.Option) *Writer
    NewRecordWriter can be used to construct a writer for arrow flight via the
    grpc stream handler to write flight data objects and write record batches to
    the stream. Options passed here will be passed to ipc.NewWriter

func (w *Writer) SetFlightDescriptor(descr *FlightDescriptor)
    SetFlightDescriptor sets the flight descriptor into the next payload that
    will be written by the flight writer. It will only be put into the very next
    payload and afterwards the writer will no longer keep it's pointer to the
    descriptor.

func (w *Writer) Write(rec arrow.RecordBatch) error
    Write writes a recordbatch payload and returns any error, implementing the
    arrio.Writer interface

func (w *Writer) WriteMetadata(appMetadata []byte) error
    WriteMetadata writes a payload message to the stream containing only the
    specified app metadata.

func (w *Writer) WriteWithAppMetadata(rec arrow.RecordBatch, appMeta []byte) error
    WriteWithAppMetadata will write this record batch with the supplied
    application metadata attached in the flightData message.

