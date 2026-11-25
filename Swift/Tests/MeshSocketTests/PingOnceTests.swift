import XCTest
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
@testable import MeshSocket

private struct PongError: Error, Equatable { let n: Int }

/// Holds the handler a fake `sendPing(pongReceiveHandler:)` was given, so a test
/// can fire it later, as often as it likes, from any thread.
private final class HandlerBox: @unchecked Sendable {
    private let lock = NSLock()
    private var handlers: [@Sendable (Error?) -> Void] = []
    private var sent = 0
    var sends: Int { lock.lock(); defer { lock.unlock() }; return sent }
    func add(_ h: @escaping @Sendable (Error?) -> Void) { lock.lock(); handlers.append(h); sent += 1; lock.unlock() }
    var count: Int { lock.lock(); defer { lock.unlock() }; return handlers.count }
    func fire(_ error: Error?) {
        lock.lock(); let hs = handlers; lock.unlock()
        hs.forEach { $0(error) }
    }
}

/// `pingOnce` and `ResumeOnce`: the pong handler may fire any number of times;
/// the continuation resumes exactly once and never traps. These are the crash
/// in CAR-TER Beta 2.0.1 (`CheckedContinuation.resume(throwing:)` from
/// `URLSessionWebSocketTask.sendPing()`), reproduced without a network.
final class PingOnceTests: XCTestCase {
    func testSuccessThenErrorResolvesAsSuccess() async throws {
        try await pingOnce { done in
            done(nil)
            done(PongError(n: 1))
        }
    }

    func testErrorThenSuccessThrowsTheFirstError() async {
        do {
            try await pingOnce { done in
                done(PongError(n: 1))
                done(nil)
                done(PongError(n: 2))
            }
            XCTFail("the first outcome (an error) must win")
        } catch {
            XCTAssertEqual(error as? PongError, PongError(n: 1))
        }
    }

    /// The handler fires later, after the continuation is parked: the pong, then
    /// the close error, as URLSessionWebSocketTask does on cancel.
    func testLateSecondCallAfterResumeIsIgnored() async throws {
        let box = HandlerBox()
        try await pingOnce { done in
            box.add(done)
            DispatchQueue.global().async { box.fire(nil) }
        }
        box.fire(URLError(.cancelled))
        box.fire(nil)
        XCTAssertEqual(box.sends, 1)
    }

    /// Many calls racing from different threads: one resumes, none traps.
    func testConcurrentCallsFromManyThreadsResumeOnce() async throws {
        for round in 0..<200 {
            do {
                try await pingOnce { done in
                    for i in 0..<8 {
                        DispatchQueue.global().async { done(i % 2 == 0 ? nil : PongError(n: i)) }
                    }
                }
            } catch is PongError {
                // An error call won the race: fine, it resumed once.
            } catch {
                XCTFail("round \(round): unexpected \(error)")
            }
        }
    }

    func testResumeOnceCountsOneWinnerUnderContention() async throws {
        for _ in 0..<200 {
            let once = ResumeOnce()
            let winCount = LockedInt()
            try? await withCheckedThrowingContinuation { (c: CheckedContinuation<Void, Error>) in
                once.arm(c)
                DispatchQueue.concurrentPerform(iterations: 16) { i in
                    if once.finish(i % 3 == 0 ? PongError(n: i) : nil) { winCount.add() }
                }
            }
            XCTAssertEqual(winCount.value, 1)
        }
    }

    /// An outcome that lands before the continuation is handed over (the task was
    /// cancelled first) resumes it on `arm`, once.
    func testFinishBeforeArmResumesOnArm() async {
        let once = ResumeOnce()
        XCTAssertTrue(once.finish(CancellationError()))
        XCTAssertFalse(once.finish(nil))
        do {
            try await withCheckedThrowingContinuation { (c: CheckedContinuation<Void, Error>) in
                XCTAssertFalse(once.arm(c))
            }
            XCTFail("expected the recorded CancellationError")
        } catch {
            XCTAssertTrue(error is CancellationError)
        }
    }

    /// Cancelling the waiter resolves it at once, even though the transport
    /// ignores cancellation (as URLSessionWebSocketTask does); the pong and the
    /// close error that come after are ignored.
    func testCancelResolvesAtOnceAndLaterCallsAreIgnored() async throws {
        let box = HandlerBox()
        let task = Task { try await pingOnce { done in box.add(done) } }
        let deadline = Date().addingTimeInterval(2)
        while box.count == 0, Date() < deadline { try await Task.sleep(nanoseconds: 1_000_000) }
        XCTAssertEqual(box.count, 1)
        task.cancel()
        let result = await task.result
        switch result {
        case .success: XCTFail("a cancelled ping must not report a pong")
        case .failure(let error): XCTAssertTrue(error is CancellationError, "\(error)")
        }
        box.fire(nil)
        box.fire(URLError(.cancelled))
    }

    func testAlreadyCancelledTaskNeverSends() async {
        let box = HandlerBox()
        let task = Task { () async -> Error? in
            while !Task.isCancelled { await Task.yield() }
            do { try await pingOnce { done in box.add(done) }; return nil } catch { return error }
        }
        task.cancel()
        let error = await task.value
        XCTAssertTrue(error is CancellationError)
        XCTAssertEqual(box.sends, 0)
    }
}

final class LockedInt: @unchecked Sendable {
    private let lock = NSLock()
    private var n = 0
    func add() { lock.lock(); n += 1; lock.unlock() }
    var value: Int { lock.lock(); defer { lock.unlock() }; return n }
}

/// Behaves like `URLSessionWebSocketTask` around pings: `sendPing` goes through
/// the same `pingOnce` the production extension uses, the underlying callback
/// ignores task cancellation, answered pings get their handler called a second
/// time with an error when the socket is cancelled, and pings it swallows only
/// ever hear back on cancel. Send/receive/welcome come from `FakeTransport`.
final class URLSessionLikeTransport: MeshTransport, @unchecked Sendable {
    private let inner = FakeTransport(pongsToAnswer: 0)
    private let lock = NSLock()
    private var handlers: [@Sendable (Error?) -> Void] = []
    private var pongsToAnswer: Int
    private var closed = false
    private(set) var pingsSeen = 0
    private(set) var secondCalls = 0
    var closeCodes: [URLSessionWebSocketTask.CloseCode] { inner.closeCodes }

    init(pongsToAnswer: Int) { self.pongsToAnswer = pongsToAnswer }

    func resume() {}
    func send(_ text: String) async throws { try await inner.send(text) }
    func receive() async throws -> URLSessionWebSocketTask.Message { try await inner.receive() }

    func sendPing() async throws {
        try await pingOnce { done in self.ping(pongReceiveHandler: done) }
    }

    private func ping(pongReceiveHandler handler: @escaping @Sendable (Error?) -> Void) {
        lock.lock()
        pingsSeen += 1
        let isClosed = closed
        let answer = !isClosed && pongsToAnswer > 0
        if answer { pongsToAnswer -= 1 }
        handlers.append(handler)
        lock.unlock()
        if isClosed {
            DispatchQueue.global().async { handler(URLError(.networkConnectionLost)) }
        } else if answer {
            DispatchQueue.global().async { handler(nil) }
        }
    }

    func cancel(with closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?) {
        inner.cancel(with: closeCode, reason: reason)
        lock.lock()
        closed = true
        let hs = handlers
        handlers.removeAll()
        secondCalls += hs.count
        lock.unlock()
        // Every ping ever sent hears about the close, answered or not.
        for h in hs { DispatchQueue.global().async { h(URLError(.cancelled)) } }
    }
}

actor LikeTally {
    var transports: [URLSessionLikeTransport] = []
    var connects = 0
    var disconnects = 0
    func add(_ t: URLSessionLikeTransport) { transports.append(t) }
    func connected() { connects += 1 }
    func disconnected() { disconnects += 1 }
}

/// The keepalive against a transport that, like URLSessionWebSocketTask, fires
/// pong handlers twice and ignores task cancellation.
final class PingKeepaliveTests: XCTestCase {
    private func makeSocket(tally: LikeTally, pongsPerTransport: @escaping @Sendable (Int) -> Int,
                            pingInterval: TimeInterval, pongTimeout: TimeInterval) -> MeshSocket {
        let counter = AttemptCounter()
        let factory: MeshTransportFactory = { _, _ in
            let t = URLSessionLikeTransport(pongsToAnswer: pongsPerTransport(counter.next()))
            Task { await tally.add(t) }
            return t
        }
        return MeshSocket(parsedURL: URL(string: "ws://127.0.0.1:1")!, name: "ping", authToken: "t",
                          rateLimit: 0, channel: nil, role: nil, canBroadcast: nil, canRoute: nil,
                          canCrossChannelRoute: nil, canMonitor: nil, broadcastScope: nil,
                          maxOfflineBuffer: 0, offlineFilePath: nil,
                          onReconnect: { await tally.connected() },
                          onDisconnect: { await tally.disconnected() },
                          pingInterval: pingInterval, pongTimeout: pongTimeout,
                          transportFactory: factory)
    }

    /// Healthy pongs, then stop(): every answered ping's handler fires again with
    /// an error on cancel. Before 0.4.4 this trapped.
    func testHandlersFiringAgainOnStopNeverTrap() async throws {
        let tally = LikeTally()
        let socket = makeSocket(tally: tally, pongsPerTransport: { _ in 1_000 },
                                pingInterval: 0.05, pongTimeout: 0.5)
        await socket.start()
        await socket.waitUntilReady()
        try await Task.sleep(nanoseconds: 400_000_000)
        let t = await tally.transports.first
        XCTAssertGreaterThan(t?.pingsSeen ?? 0, 3, "keepalive pings must flow")
        let downs = await tally.disconnects
        XCTAssertEqual(downs, 0)
        await socket.stop()
        // Let the second handler calls land.
        try await Task.sleep(nanoseconds: 200_000_000)
        XCTAssertGreaterThan(t?.secondCalls ?? 0, 3, "the fake must have fired handlers a second time")
    }

    /// Pongs swallowed after the connect probe, and the transport ignores task
    /// cancellation: the pong timeout must still tear the socket down (before
    /// 0.4.4 the probe waited on the ping forever), exactly one onDisconnect, and
    /// the second handler calls on cancel must not trap.
    func testPongTimeoutTearsDownATransportThatIgnoresCancellation() async throws {
        let tally = LikeTally()
        let socket = makeSocket(tally: tally, pongsPerTransport: { n in n == 1 ? 1 : 1_000 },
                                pingInterval: 0.2, pongTimeout: 0.2)
        await socket.start()
        await socket.waitUntilReady()
        let deadline = Date().addingTimeInterval(1.5)
        while await tally.disconnects < 1, Date() < deadline {
            try await Task.sleep(nanoseconds: 20_000_000)
        }
        let downs = await tally.disconnects
        XCTAssertEqual(downs, 1, "pong timeout must surface as exactly one onDisconnect")
        let first = await tally.transports.first
        XCTAssertEqual(first?.closeCodes, [.abnormalClosure])
        try await Task.sleep(nanoseconds: 200_000_000)
        XCTAssertGreaterThanOrEqual(first?.secondCalls ?? 0, 2, "probe ping and swallowed ping both hear the close")
        await socket.stop()
    }

    /// stop() while the connect probe waits on a pong that never comes: the
    /// connection task stops waiting and closes the half-open attempt (before
    /// 0.4.4 it waited on the dead socket and never closed it).
    func testStopDuringConnectProbeClosesTheAttempt() async throws {
        let tally = LikeTally()
        let socket = makeSocket(tally: tally, pongsPerTransport: { _ in 0 },
                                pingInterval: 0, pongTimeout: 0.2)
        await socket.start()
        let deadline = Date().addingTimeInterval(2)
        while await tally.transports.isEmpty, Date() < deadline {
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        try await Task.sleep(nanoseconds: 50_000_000)
        await socket.stop()
        let first = await tally.transports.first
        let closeDeadline = Date().addingTimeInterval(1)
        while first?.closeCodes.isEmpty ?? true, Date() < closeDeadline {
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        XCTAssertEqual(first?.pingsSeen, 1)
        XCTAssertEqual(first?.closeCodes, [.goingAway], "stop() must close the attempt that was mid-probe")
        let ups = await tally.connects
        XCTAssertEqual(ups, 0)
    }
}
