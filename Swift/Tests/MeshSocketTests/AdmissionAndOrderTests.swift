import XCTest
@testable import MeshSocket

/// In-memory transport that answers every ping, welcomes an identify after
/// `welcomeDelay`, and lets a test push frames or drop the socket (carter-b1wp).
final class ScriptedTransport: MeshTransport, @unchecked Sendable {
    private let lock = NSLock()
    private var inbox: [URLSessionWebSocketTask.Message] = []
    private var receiveWaiter: CheckedContinuation<URLSessionWebSocketTask.Message, Error>?
    private var closed = false
    private let welcomeDelay: TimeInterval

    init(welcomeDelay: TimeInterval) { self.welcomeDelay = welcomeDelay }

    func resume() {}

    func send(_ text: String) async throws {
        lock.lock()
        let isClosed = closed
        lock.unlock()
        if isClosed { throw URLError(.networkConnectionLost) }
        guard let data = text.data(using: .utf8),
              let frame = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              frame["type"] as? String == "identify" else { return }
        let delay = welcomeDelay
        Task {
            try? await Task.sleep(nanoseconds: MeshSocket.nanoseconds(delay))
            self.push(["id": "w", "type": "welcome", "payload": ["id": "srv"], "reply_to": NSNull()])
        }
    }

    /// Deliver one inbound frame.
    func push(_ frame: [String: Any]) {
        let d = try! JSONSerialization.data(withJSONObject: frame)
        let m = URLSessionWebSocketTask.Message.string(String(data: d, encoding: .utf8)!)
        lock.lock()
        if closed { lock.unlock(); return }
        if let w = receiveWaiter {
            receiveWaiter = nil
            lock.unlock()
            w.resume(returning: m)
        } else {
            inbox.append(m)
            lock.unlock()
        }
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
        let isClosed = closed
        lock.unlock()
        if isClosed { throw URLError(.networkConnectionLost) }
    }

    func cancel(with closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?) {
        lock.lock()
        closed = true
        let w = receiveWaiter
        receiveWaiter = nil
        lock.unlock()
        w?.resume(throwing: URLError(.networkConnectionLost))
    }
}

actor Recorder {
    var transports: [ScriptedTransport] = []
    var connects = 0
    var seen: [Int] = []
    func add(_ t: ScriptedTransport) { transports.append(t) }
    func connected() { connects += 1 }
    func record(_ n: Int) { seen.append(n) }
}

final class AdmissionAndOrderTests: XCTestCase {
    private func makeSocket(_ rec: Recorder, delays: @escaping @Sendable (Int) -> TimeInterval) -> MeshSocket {
        let counter = AttemptCounter()
        let factory: MeshTransportFactory = { _, _ in
            let t = ScriptedTransport(welcomeDelay: delays(counter.next()))
            Task { await rec.add(t) }
            return t
        }
        return MeshSocket(parsedURL: URL(string: "ws://127.0.0.1:1")!, name: "b1wp", authToken: "t",
                          rateLimit: 0, channel: nil, role: nil, canBroadcast: nil, canRoute: nil,
                          canCrossChannelRoute: nil, canMonitor: nil, broadcastScope: nil,
                          maxOfflineBuffer: 0, offlineFilePath: nil,
                          onReconnect: { await rec.connected() }, onDisconnect: nil,
                          pingInterval: 0, pongTimeout: 10, transportFactory: factory)
    }

    /// M1: attempt 1 is admitted at once and drops; attempt 2 (after the 2 s
    /// backoff) gets a slow welcome. Attempt 1's admission timeout (3 s) lands
    /// while attempt 2 waits; it must not fail attempt 2.
    func testStaleAdmissionTimeoutDoesNotFailNextAttempt() async throws {
        let rec = Recorder()
        let socket = makeSocket(rec, delays: { n in n == 1 ? 0 : 1.6 })
        await socket.setAdmissionTimeoutForTesting(3.0)
        await socket.start()
        await socket.waitUntilReady()
        let first = await rec.transports.first
        first?.cancel(with: .abnormalClosure, reason: nil)   // drop right after admission

        // Attempt 2 starts ~2 s later; its welcome lands ~3.6 s, after the stale 3 s timer.
        let deadline = Date().addingTimeInterval(6)
        while await rec.connects < 2, Date() < deadline {
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        let connects = await rec.connects
        let attempts = await rec.transports.count
        XCTAssertEqual(connects, 2, "the retry must be admitted, not failed by attempt 1's timer")
        XCTAssertEqual(attempts, 2, "no spurious third attempt")
        await socket.stop()
    }

    /// M2: frames of one type reach their handler in arrival order even when the
    /// handler suspends for varying lengths.
    func testHandlerFramesKeepArrivalOrder() async throws {
        let rec = Recorder()
        let socket = makeSocket(rec, delays: { _ in 0 })
        await socket.on("seq") { payload in
            let n = (payload as? [String: Any])?["n"] as? Int ?? -1
            try? await Task.sleep(nanoseconds: UInt64.random(in: 0...2_000_000))
            await rec.record(n)
            return nil
        }
        await socket.start()
        await socket.waitUntilReady()
        let t = await rec.transports.first
        let total = 60
        for n in 0..<total {
            t?.push(["id": "m\(n)", "type": "seq", "payload": ["n": n], "reply_to": NSNull()])
        }
        let deadline = Date().addingTimeInterval(5)
        while await rec.seen.count < total, Date() < deadline {
            try await Task.sleep(nanoseconds: 20_000_000)
        }
        let seen = await rec.seen
        XCTAssertEqual(seen, Array(0..<total), "handler runs must follow frame order")
        await socket.stop()
    }
}
