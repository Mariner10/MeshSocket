import XCTest
@testable import MeshSocket

/// In-memory transport (carter-b1wp). It answers every ping, welcomes an
/// identify after `welcomeDelay` (never when nil), records what is sent, and
/// lets a test push frames or drop the socket. `lingerOnCancel` delays the
/// failure of a pending receive after a cancel, so the listen task's close hook
/// can be made to land late, during the next attempt.
final class ScriptedTransport: MeshTransport, @unchecked Sendable {
    private let lock = NSLock()
    private var inbox: [URLSessionWebSocketTask.Message] = []
    private var receiveWaiter: CheckedContinuation<URLSessionWebSocketTask.Message, Error>?
    private var closed = false
    private var sentFrames: [String] = []
    private let welcomeDelay: TimeInterval?
    private let lingerOnCancel: TimeInterval
    private let lingerDeliversWelcome: Bool

    init(welcomeDelay: TimeInterval?, lingerOnCancel: TimeInterval = 0,
         lingerDeliversWelcome: Bool = false) {
        self.welcomeDelay = welcomeDelay
        self.lingerOnCancel = lingerOnCancel
        self.lingerDeliversWelcome = lingerDeliversWelcome
    }

    func resume() {}

    func send(_ text: String) async throws {
        lock.lock()
        if closed { lock.unlock(); throw URLError(.networkConnectionLost) }
        sentFrames.append(text)
        lock.unlock()
        guard let delay = welcomeDelay,
              let data = text.data(using: .utf8),
              let frame = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              frame["type"] as? String == "identify" else { return }
        Task {
            try? await Task.sleep(nanoseconds: MeshSocket.nanoseconds(delay))
            self.push(["id": "w", "type": "welcome", "payload": ["id": "srv"], "reply_to": NSNull()])
        }
    }

    /// Every `reply_to` this transport has sent.
    var sentReplyTos: [String] {
        lock.lock()
        let frames = sentFrames
        lock.unlock()
        return frames.compactMap { text in
            guard let data = text.data(using: .utf8),
                  let frame = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else { return nil }
            return frame["reply_to"] as? String
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
        guard let w else { return }
        let linger = lingerOnCancel
        let welcome = lingerDeliversWelcome
        if linger <= 0 {
            w.resume(throwing: URLError(.networkConnectionLost))
        } else {
            Task {
                try? await Task.sleep(nanoseconds: MeshSocket.nanoseconds(linger))
                if welcome {
                    // A frame that straggles in after the cancel.
                    let d = try! JSONSerialization.data(withJSONObject:
                        ["id": "w", "type": "welcome", "payload": ["id": "old"], "reply_to": NSNull()] as [String: Any])
                    w.resume(returning: .string(String(data: d, encoding: .utf8)!))
                } else {
                    w.resume(throwing: URLError(.networkConnectionLost))
                }
            }
        }
    }
}

/// Thread-safe record of transports made, admissions and handler calls. The
/// factory registers synchronously, so `transports` is never behind.
final class Recorder: @unchecked Sendable {
    private let lock = NSLock()
    private var _transports: [ScriptedTransport] = []
    private var _connects = 0
    private var _seen: [Int] = []

    func add(_ t: ScriptedTransport) { lock.lock(); _transports.append(t); lock.unlock() }
    func connected() { lock.lock(); _connects += 1; lock.unlock() }
    func record(_ n: Int) { lock.lock(); _seen.append(n); lock.unlock() }

    var transports: [ScriptedTransport] { lock.lock(); defer { lock.unlock() }; return _transports }
    var connects: Int { lock.lock(); defer { lock.unlock() }; return _connects }
    var seen: [Int] { lock.lock(); defer { lock.unlock() }; return _seen }
}

/// A one-shot latch a handler can park on, like an unanswered approval sheet.
actor Gate {
    private var opened = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        if opened { return }
        await withCheckedContinuation { waiters.append($0) }
    }

    func open() {
        opened = true
        let pending = waiters
        waiters.removeAll()
        for w in pending { w.resume() }
    }
}

/// Poll `condition` every 20 ms until it holds or `seconds` pass.
func waitFor(_ seconds: TimeInterval, _ condition: () -> Bool) async -> Bool {
    let deadline = Date().addingTimeInterval(seconds)
    while Date() < deadline {
        if condition() { return true }
        try? await Task.sleep(nanoseconds: 20_000_000)
    }
    return condition()
}

/// One connection attempt's script: when `welcome` comes (never when nil) and
/// how long a pending receive lingers after a cancel.
struct AttemptScript: Sendable {
    var welcomeDelay: TimeInterval?
    var lingerOnCancel: TimeInterval = 0
    var lingerDeliversWelcome = false
}

final class AdmissionAndOrderTests: XCTestCase {
    private func makeSocket(_ rec: Recorder, maxOfflineBuffer: Int = 0,
                            script: @escaping @Sendable (Int) -> AttemptScript) -> MeshSocket {
        let counter = AttemptCounter()
        let factory: MeshTransportFactory = { _, _ in
            let s = script(counter.next())
            let t = ScriptedTransport(welcomeDelay: s.welcomeDelay, lingerOnCancel: s.lingerOnCancel,
                                  lingerDeliversWelcome: s.lingerDeliversWelcome)
            rec.add(t)
            return t
        }
        return MeshSocket(parsedURL: URL(string: "ws://127.0.0.1:1")!, name: "b1wp", authToken: "t",
                          rateLimit: 0, channel: nil, role: nil, canBroadcast: nil, canRoute: nil,
                          canCrossChannelRoute: nil, canMonitor: nil, broadcastScope: nil,
                          maxOfflineBuffer: maxOfflineBuffer, offlineFilePath: nil,
                          onReconnect: { rec.connected() }, onDisconnect: nil,
                          pingInterval: 0, pongTimeout: 10, transportFactory: factory)
    }

    private func frame(_ type: String, id: String, n: Int) -> [String: Any] {
        ["id": id, "type": type, "payload": ["n": n], "reply_to": NSNull()]
    }

    /// M1, stale timer. Attempt 1 arms its 4 s admission timer at ~0 s, is
    /// welcomed at 0.2 s and drops at once. Attempt 2 starts after the 2 s
    /// backoff (~2.2 s) and is welcomed 3 s later (~5.2 s). Without the fix
    /// attempt 1's timer fires at ~4 s, mid-wait, and fails attempt 2. Every
    /// edge has 1 s of slack (attempt 2's own timeout is at ~6.2 s).
    func testStaleAdmissionTimeoutDoesNotFailNextAttempt() async throws {
        let rec = Recorder()
        let socket = makeSocket(rec) { n in AttemptScript(welcomeDelay: n == 1 ? 0.2 : 3.0) }
        await socket.setAdmissionTimeoutForTesting(4.0)
        await socket.start()
        await socket.waitUntilReady()
        rec.transports.first?.cancel(with: .abnormalClosure, reason: nil)

        _ = await waitFor(8) { rec.connects >= 2 }
        XCTAssertEqual(rec.connects, 2, "the retry must be admitted, not failed by attempt 1's timer")
        XCTAssertEqual(rec.transports.count, 2, "no spurious third attempt")
        await socket.stop()
    }

    /// M1, stale close hook. Attempt 1 is never welcomed and times out at 3 s;
    /// its socket's pending receive lingers 3 s after the cancel, so its listen
    /// task ends (and runs its close hook) at ~6 s. Attempt 2 starts after the
    /// 2 s backoff (~5 s) and is welcomed at ~7 s. The late hook from attempt 1
    /// must not fail attempt 2. 1 s of slack on every edge.
    func testStaleListenCloseHookDoesNotFailNextAttempt() async throws {
        let rec = Recorder()
        let socket = makeSocket(rec) { n in
            n == 1 ? AttemptScript(welcomeDelay: nil, lingerOnCancel: 3.0)
                   : AttemptScript(welcomeDelay: 2.0)
        }
        await socket.setAdmissionTimeoutForTesting(3.0)
        await socket.start()

        _ = await waitFor(10) { rec.connects >= 1 }
        XCTAssertEqual(rec.connects, 1, "attempt 2 must be admitted despite attempt 1's late close")
        XCTAssertEqual(rec.transports.count, 2, "admitted on the second attempt, no third")
        await socket.stop()
    }

    /// A lingering old listen loop must not route frames under the next attempt.
    /// Attempt 1 is never welcomed, times out at 3 s, and its pending receive
    /// delivers a stray `welcome` 3 s after the cancel (~6 s). Attempt 2 starts
    /// ~5 s and is never welcomed (times out ~8 s). The old welcome must not
    /// admit attempt 2, so nothing is admitted by 7 s.
    func testStaleLoopFrameDoesNotAdmitNextAttempt() async throws {
        let rec = Recorder()
        let socket = makeSocket(rec) { n in
            n == 1 ? AttemptScript(welcomeDelay: nil, lingerOnCancel: 3.0, lingerDeliversWelcome: true)
                   : AttemptScript(welcomeDelay: nil)
        }
        await socket.setAdmissionTimeoutForTesting(3.0)
        await socket.start()
        _ = await waitFor(7) { rec.connects >= 1 }
        XCTAssertEqual(rec.connects, 0, "a stale welcome from attempt 1 must not admit attempt 2")
        await socket.stop()
    }

    /// M2: frames of one type reach their handler in arrival order even when the
    /// handler suspends for varying lengths.
    func testHandlerFramesKeepArrivalOrder() async throws {
        let rec = Recorder()
        let socket = makeSocket(rec) { _ in AttemptScript(welcomeDelay: 0) }
        await socket.on("seq") { payload in
            let n = (payload as? [String: Any])?["n"] as? Int ?? -1
            try? await Task.sleep(nanoseconds: UInt64.random(in: 0...2_000_000))
            rec.record(n)
            return nil
        }
        await socket.start()
        await socket.waitUntilReady()
        let t = rec.transports.first
        let total = 60
        for n in 0..<total {
            t?.push(frame("seq", id: "m\(n)", n: n))
        }
        _ = await waitFor(5) { rec.seen.count >= total }
        XCTAssertEqual(rec.seen, Array(0..<total), "handler runs must follow frame order")
        await socket.stop()
    }

    /// An "ask" handler that parks on `gate` for n == 1, like an unanswered
    /// approval sheet, and records every n it sees.
    private func installAsk(on socket: MeshSocket, _ rec: Recorder, _ gate: Gate) async {
        await socket.on("ask") { payload in
            let n = (payload as? [String: Any])?["n"] as? Int ?? -1
            rec.record(n)
            if n == 1 { await gate.wait() }
            return ["answer": n]
        }
    }

    /// Teardown with a parked handler. a1 parks its handler and the connection
    /// drops. On the new connection a2 (same type) must run at once, and when
    /// a1's handler finally returns its reply is dropped: not sent on the new
    /// connection, not buffered for it (the offline buffer is on).
    func testParkedHandlerDoesNotHoldBackNextConnection() async throws {
        let rec = Recorder()
        let gate = Gate()
        let socket = makeSocket(rec, maxOfflineBuffer: 10) { _ in AttemptScript(welcomeDelay: 0.1) }
        await installAsk(on: socket, rec, gate)
        await socket.start()
        await socket.waitUntilReady()
        let first = rec.transports[0]
        first.push(frame("ask", id: "a1", n: 1))
        let parked = await waitFor(2) { rec.seen == [1] }
        XCTAssertTrue(parked, "a1's handler should be running (parked)")

        first.cancel(with: .abnormalClosure, reason: nil)
        _ = await waitFor(6) { rec.connects >= 2 }
        let second = try XCTUnwrap(rec.transports.dropFirst().first, "should reconnect after the drop")
        second.push(frame("ask", id: "a2", n: 2))
        let ran = await waitFor(2) { rec.seen == [1, 2] }
        XCTAssertTrue(ran, "a2 must not wait behind a1's handler parked on the dead connection")
        let replied = await waitFor(2) { second.sentReplyTos.contains("a2") }
        XCTAssertTrue(replied, "a2's reply goes out on the new connection")

        await gate.open()
        try await Task.sleep(nanoseconds: 500_000_000)
        XCTAssertFalse(second.sentReplyTos.contains("a1"), "a1's stale reply must not reach the new connection")
        XCTAssertFalse(first.sentReplyTos.contains("a1"), "nor the dead one")
        await socket.stop()
    }

    /// stop() with a parked handler that returns while stopped, then start()
    /// again: its reply is neither sent nor flushed from the offline buffer
    /// onto the restarted connection, and that type's frames run normally.
    func testStopWithParkedHandlerThenRestart() async throws {
        let rec = Recorder()
        let gate = Gate()
        let socket = makeSocket(rec, maxOfflineBuffer: 10) { _ in AttemptScript(welcomeDelay: 0.1) }
        await installAsk(on: socket, rec, gate)
        await socket.start()
        await socket.waitUntilReady()
        rec.transports[0].push(frame("ask", id: "a1", n: 1))
        let parked = await waitFor(2) { rec.seen == [1] }
        XCTAssertTrue(parked, "a1's handler should be running (parked)")

        await socket.stop()
        await gate.open()                                    // a1 returns while stopped
        try await Task.sleep(nanoseconds: 300_000_000)       // let the old loop exit
        await socket.start()
        _ = await waitFor(4) { rec.connects >= 2 }
        let second = try XCTUnwrap(rec.transports.dropFirst().first, "should connect again after start()")
        second.push(frame("ask", id: "a2", n: 2))
        let replied = await waitFor(2) { second.sentReplyTos.contains("a2") }
        XCTAssertTrue(replied, "the restarted connection handles and answers a2")
        XCTAssertFalse(second.sentReplyTos.contains("a1"), "a1's stale reply must not be flushed to the new connection")
        await socket.stop()
    }

    /// The per-type contract `on()` documents: while a "slow" handler is parked,
    /// a "fast" frame runs at once, but the next "slow" frame waits its turn.
    func testParkedHandlerHoldsOnlyItsOwnType() async throws {
        let rec = Recorder()
        let gate = Gate()
        let socket = makeSocket(rec) { _ in AttemptScript(welcomeDelay: 0) }
        await socket.on("slow") { payload in
            let n = (payload as? [String: Any])?["n"] as? Int ?? -1
            rec.record(n)
            if n == 1 { await gate.wait() }
            return nil
        }
        await socket.on("fast") { payload in
            rec.record((payload as? [String: Any])?["n"] as? Int ?? -1)
            return nil
        }
        await socket.start()
        await socket.waitUntilReady()
        let t = rec.transports[0]
        t.push(frame("slow", id: "s1", n: 1))
        t.push(frame("fast", id: "f1", n: 100))
        t.push(frame("slow", id: "s2", n: 2))

        let fastRan = await waitFor(2) { rec.seen.contains(100) }
        XCTAssertTrue(fastRan, "another type must not wait behind a parked handler")
        try await Task.sleep(nanoseconds: 300_000_000)
        XCTAssertFalse(rec.seen.contains(2), "the next frame of the parked type waits its turn")

        await gate.open()
        let slowRan = await waitFor(2) { rec.seen.contains(2) }
        XCTAssertTrue(slowRan, "it runs once the parked handler returns")
        XCTAssertEqual(rec.seen.last, 2)
        XCTAssertEqual(Set(rec.seen), [1, 2, 100])
        await socket.stop()
    }
}
