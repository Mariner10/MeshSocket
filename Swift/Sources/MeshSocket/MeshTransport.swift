import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

/// The minimal WebSocket surface `MeshSocket` drives. `URLSessionWebSocketTask`
/// is the production implementation; tests inject a fake (see `KeepaliveTests`)
/// to exercise the keepalive and reconnect paths without a network.
public protocol MeshTransport: AnyObject {
    func resume()
    func send(_ text: String) async throws
    func receive() async throws -> URLSessionWebSocketTask.Message
    /// Resolves when the peer's pong arrives; throws if the socket is gone.
    func sendPing() async throws
    func cancel(with closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?)
}

/// Builds a transport for one connection attempt.
public typealias MeshTransportFactory = @Sendable (URL, URLSession) -> any MeshTransport

extension URLSessionWebSocketTask: MeshTransport {
    public func send(_ text: String) async throws {
        try await send(.string(text))
    }

    #if !canImport(FoundationNetworking)
    /// The pong handler can fire more than once (the pong, then an error when the
    /// socket closes or is cancelled), so resume through `pingOnce`: the first
    /// call wins, and cancelling the caller (the keepalive's pong timeout, or
    /// `stop()`) returns at once instead of waiting on a dead socket.
    public func sendPing() async throws {
        try await pingOnce { done in sendPing(pongReceiveHandler: done) }
    }
    #endif
    // On Linux, swift-corelibs-foundation's URLSessionWebSocketTask already has
    // `sendPing() async throws`, which satisfies the requirement. Apple platforms
    // are the shipping target; Linux builds only for tests.
}

public enum MeshTransports {
    /// The default factory: a `URLSessionWebSocketTask` on the socket's own session.
    public static let urlSession: MeshTransportFactory = { url, session in
        session.webSocketTask(with: url)
    }
}
