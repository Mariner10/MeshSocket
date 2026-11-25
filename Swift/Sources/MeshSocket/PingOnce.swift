import Foundation

/// Resumes a throwing `Void` continuation exactly once. The first outcome wins;
/// every later one is dropped. Thread-safe: outcomes may arrive from any thread,
/// before or after the continuation is handed over.
///
/// `URLSessionWebSocketTask.sendPing(pongReceiveHandler:)` can call its handler
/// more than once (the pong, then again with an error when the socket closes or
/// is cancelled), and a `CheckedContinuation` resumed twice traps. CAR-TER Beta
/// 2.0.1 crashed exactly there (`CheckedContinuation.resume(throwing:)` from
/// `sendPing()`).
final class ResumeOnce: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<Void, Error>?
    private var outcome: Result<Void, Error>?

    /// Hand over the continuation. If an outcome already arrived (the waiting task
    /// was cancelled first), the continuation is resumed with it now and this
    /// returns false: the caller should not start the operation.
    @discardableResult
    func arm(_ c: CheckedContinuation<Void, Error>) -> Bool {
        lock.lock()
        if let outcome {
            lock.unlock()
            c.resume(with: outcome)
            return false
        }
        continuation = c
        lock.unlock()
        return true
    }

    /// Record an outcome (nil = success). Only the first call counts; it returns
    /// true, later calls return false and do nothing.
    @discardableResult
    func finish(_ error: Error?) -> Bool {
        lock.lock()
        if outcome != nil {
            lock.unlock()
            return false
        }
        let result: Result<Void, Error> = error.map { .failure($0) } ?? .success(())
        outcome = result
        let c = continuation
        continuation = nil
        lock.unlock()
        c?.resume(with: result)
        return true
    }
}

/// One ping through a callback API whose handler may fire any number of times,
/// from any thread. Resolves on the first call (nil = pong, error = failure) and
/// ignores the rest. Cancelling the waiting task resolves it at once with
/// `CancellationError`, so a caller racing the pong against a timeout (the
/// keepalive probe) is never held up by a socket that never answers; a pong
/// that arrives after that is ignored. An already-cancelled task never sends.
func pingOnce(_ send: (@escaping @Sendable (Error?) -> Void) -> Void) async throws {
    let once = ResumeOnce()
    try await withTaskCancellationHandler {
        try await withCheckedThrowingContinuation { (c: CheckedContinuation<Void, Error>) in
            guard once.arm(c) else { return }
            send { error in once.finish(error) }
        }
    } onCancel: {
        once.finish(CancellationError())
    }
}
