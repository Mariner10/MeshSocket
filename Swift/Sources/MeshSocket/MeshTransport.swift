import Foundation

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

    public func sendPing() async throws {
        try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
            sendPing { error in
                if let error { cont.resume(throwing: error) } else { cont.resume() }
            }
        }
    }
}

public enum MeshTransports {
    /// The default factory: a `URLSessionWebSocketTask` on the socket's own session.
    public static let urlSession: MeshTransportFactory = { url, session in
        session.webSocketTask(with: url)
    }
}
