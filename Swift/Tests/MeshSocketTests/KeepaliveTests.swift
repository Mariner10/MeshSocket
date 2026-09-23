import XCTest
@testable import MeshSocket

/// A scripted in-memory transport. It welcomes any identify, answers the first
/// `pongsToAnswer` pings and swallows the rest, and behaves like a real socket on
/// cancel: pending receives and pings fail.
final class FakeTransport: MeshTransport, @unchecked Sendable {
    private let lock = NSLock()
    private var inbox: [URLSessionWebSocketTask.Message] = []
    private var receiveWaiter: CheckedContinuation<URLSessionWebSocketTask.Message, Error>?
    private var pingWaiters: [CheckedContinuation<Void, Error>] = []
    private var pongsToAnswer: Int
    private(set) var closed = false
    private(set) var closeCodes: [URLSessionWebSocketTask.CloseCode] = []
    private(set) var sent: [String] = []
    private(set) var pingsSeen = 0

    init(pongsToAnswer: Int) {
        self.pongsToAnswer = pongsToAnswer
    }

    func resume() {}

    func send(_ text: String) async throws {
        lock.lock()
        if closed { lock.unlock(); throw URLError(.networkConnectionLost) }
        sent.append(text)
        var welcome: URLSessionWebSocketTask.Message?
        if let data = text.data(using: .utf8),
           let frame = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
           frame["type"] as? String == "identify" {
            let reply: [String: Any] = ["id": "w", "type": "welcome",
                                        "payload": ["id": "srv-\(UUID().uuidString.prefix(6))", "name": "fake"],
                                        "reply_to": NSNull()]
            let d = try! JSONSerialization.data(withJSONObject: reply)
            welcome = .string(String(data: d, encoding: .utf8)!)
        }
        var waiter: CheckedContinuation<URLSessionWebSocketTask.Message, Error>?
        if let welcome {
            if let w = receiveWaiter { waiter = w; receiveWaiter = nil } else { inbox.append(welcome) }
        }
        lock.unlock()
        if let waiter, let welcome { waiter.resume(returning: welcome) }
    }

    func receive() async throws -> URLSessionWebSocketTask.Message {
        try await withCheckedThrowingContinuation { cont in
            lock.lock()
            if !inbox.isEmpty {
                let m = inbox.removeFirst(); lock.unlock(); cont.resume(returning: m); return
            }
            if closed { lock.unlock(); cont.resume(throwing: URLError(.networkConnectionLost)); return }
            receiveWaiter = cont
            lock.unlock()
        }
    }

    func sendPing() async throws {
        lock.lock()
        pingsSeen += 1
        if closed { lock.unlock(); throw URLError(.networkConnectionLost) }
        if pongsToAnswer > 0 { pongsToAnswer -= 1; lock.unlock(); return }
        lock.unlock()
        // Swallow: park until cancel (or until the caller's own timeout wins).
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
                lock.lock()
                if closed { lock.unlock(); cont.resume(throwing: URLError(.networkConnectionLost)); return }
                pingWaiters.append(cont)
                lock.unlock()
            }
        } onCancel: {
            failPingWaiters(CancellationError())
        }
    }

    private func failPingWaiters(_ error: Error) {
        lock.lock()
        let waiters = pingWaiters
        pingWaiters.removeAll()
        lock.unlock()
        waiters.forEach { $0.resume(throwing: error) }
    }

    func cancel(with closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?) {
        lock.lock()
        closed = true
        closeCodes.append(closeCode)
        let waiter = receiveWaiter
        receiveWaiter = nil
        lock.unlock()
        waiter?.resume(throwing: URLError(.networkConnectionLost))
        failPingWaiters(URLError(.networkConnectionLost))
    }
}

/// Counts events from the socket's callbacks and factory.
actor Tally {
    var transports: [FakeTransport] = []
    var connects = 0
    var disconnects = 0
    func add(_ t: FakeTransport) { transports.append(t) }
    func connected() { connects += 1 }
    func disconnected() { disconnects += 1 }
}

final class KeepaliveTests: XCTestCase {
    private func makeSocket(tally: Tally, pongsPerTransport: @escaping @Sendable (Int) -> Int,
                            pingInterval: TimeInterval = 0.2, pongTimeout: TimeInterval = 0.2) -> MeshSocket {
        let counter = AttemptCounter()
        let factory: MeshTransportFactory = { _, _ in
            let n = counter.next()
            let t = FakeTransport(pongsToAnswer: pongsPerTransport(n))
            Task { await tally.add(t) }
            return t
        }
        return MeshSocket(parsedURL: URL(string: "ws://127.0.0.1:1")!, name: "ka", authToken: "t",
                          rateLimit: 0, channel: nil, role: nil, canBroadcast: nil, canRoute: nil,
                          canCrossChannelRoute: nil, canMonitor: nil, broadcastScope: nil,
                          maxOfflineBuffer: 0, offlineFilePath: nil,
                          onReconnect: { await tally.connected() },
                          onDisconnect: { await tally.disconnected() },
                          pingInterval: pingInterval, pongTimeout: pongTimeout,
                          transportFactory: factory)
    }

    /// Pongs swallowed after the connect probe: onDisconnect fires once within
    /// pingInterval + pongTimeout + slack, and a reconnect attempt follows.
    func testPongTimeoutTearsDownAndReconnects() async throws {
        let tally = Tally()
        // Attempt 1: answer only the connect probe. Attempt 2+: answer everything.
        let socket = makeSocket(tally: tally, pongsPerTransport: { n in n == 1 ? 1 : 1_000 })
        await socket.start()
        await socket.waitUntilReady()
        let v1_ = await tally.connects
        XCTAssertEqual(v1_, 1)

        let deadline = Date().addingTimeInterval(1.5)   // 0.2 + 0.2 + 1 s slack
        while await tally.disconnects < 1, Date() < deadline {
            try await Task.sleep(nanoseconds: 20_000_000)
        }
        let v2_ = await tally.disconnects
        XCTAssertEqual(v2_, 1, "pong timeout must surface as exactly one onDisconnect")
        let first = await tally.transports.first
        XCTAssertEqual(first?.closeCodes, [.abnormalClosure], "keepalive must close the dead transport")

        // Backoff is 2 s; the second attempt must come up and stay up.
        let reconnectDeadline = Date().addingTimeInterval(4)
        while await tally.connects < 2, Date() < reconnectDeadline {
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        let v3_ = await tally.connects
        XCTAssertEqual(v3_, 2, "a reconnect attempt must follow the teardown")
        let v4_ = await tally.transports.count
        XCTAssertEqual(v4_, 2)

        // Healthy pongs on the second transport: no further disconnects.
        try await Task.sleep(nanoseconds: 600_000_000)
        let v5_ = await tally.disconnects
        XCTAssertEqual(v5_, 1)
        let second = await tally.transports.last
        XCTAssertGreaterThan(second?.pingsSeen ?? 0, 1, "keepalive pings continue on the new transport")
        await socket.stop()
    }

    /// While paused, swallowed pongs never tear the connection down.
    func testPauseKeepaliveSuppressesTeardown() async throws {
        let tally = Tally()
        let socket = makeSocket(tally: tally, pongsPerTransport: { _ in 1 })
        await socket.pauseKeepalive()
        await socket.start()
        await socket.waitUntilReady()
        try await Task.sleep(nanoseconds: 900_000_000)
        let v6_ = await tally.disconnects
        XCTAssertEqual(v6_, 0)
        let t = await tally.transports.first
        XCTAssertEqual(t?.pingsSeen, 1, "only the connect probe while paused")

        await socket.resumeKeepalive()
        let deadline = Date().addingTimeInterval(1.5)
        while await tally.disconnects < 1, Date() < deadline {
            try await Task.sleep(nanoseconds: 20_000_000)
        }
        let v7_ = await tally.disconnects
        XCTAssertEqual(v7_, 1, "resuming re-arms the keepalive")
        await socket.stop()
    }

    func testStopCancelsKeepalive() async throws {
        let tally = Tally()
        let socket = makeSocket(tally: tally, pongsPerTransport: { _ in 1_000 }, pingInterval: 0.1)
        await socket.start()
        await socket.waitUntilReady()
        try await Task.sleep(nanoseconds: 350_000_000)
        await socket.stop()
        let t = await tally.transports.first
        let seen = t?.pingsSeen ?? 0
        try await Task.sleep(nanoseconds: 400_000_000)
        XCTAssertEqual(t?.pingsSeen, seen, "no pings after stop()")
        XCTAssertEqual(t?.closeCodes, [.goingAway], "stop() closes with goingAway, not the keepalive's abnormalClosure")
    }

    func testValidatingInitRejectsBadURLs() {
        XCTAssertThrowsError(try MeshSocket(validating: "not a url"))
        XCTAssertThrowsError(try MeshSocket(validating: "http://example.com"))
        XCTAssertThrowsError(try MeshSocket(validating: "ws://"))
        XCTAssertNoThrow(try MeshSocket(validating: "wss://connect.example.net"))
        XCTAssertNoThrow(try MeshSocket(validating: "ws://127.0.0.1:8765/path"))
    }

    func testDefaultsAreThirtyAndTen() async {
        let socket = MeshSocket(url: "ws://127.0.0.1:1", name: "d")
        let interval = await socket.pingInterval
        let timeout = await socket.pongTimeout
        XCTAssertEqual(interval, 30)
        XCTAssertEqual(timeout, 10)
    }
}

final class AttemptCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var n = 0
    func next() -> Int { lock.lock(); defer { lock.unlock() }; n += 1; return n }
}
