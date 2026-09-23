import XCTest
@testable import MeshSocket

/// Reply frames are only ever replies (security review 2026-09-22, M1).
final class ReplyMatchingTests: XCTestCase {
    func testUnmatchedReplyIsDroppedNotDispatched() async {
        let socket = MeshSocket(url: "ws://127.0.0.1:0", name: "ReplyTest")
        let calls = Counter()
        await socket.on("ping") { _ in
            await calls.bump()
            return "pong"
        }

        // Looks like a reply to a request we never made: must not reach the handler.
        await socket.processPacket(["id": "z", "type": "ping", "payload": "pong", "reply_to": "never-pending"])
        // A genuine request still does.
        await socket.processPacket(["id": "q", "type": "ping", "payload": NSNull(), "reply_to": NSNull()])

        let n = await calls.value
        XCTAssertEqual(n, 1, "only the real request may be dispatched")
    }

    func testNanosecondsClampsBadIntervals() {
        XCTAssertEqual(MeshSocket.nanoseconds(-1), 0)
        XCTAssertEqual(MeshSocket.nanoseconds(.nan), 0)
        XCTAssertEqual(MeshSocket.nanoseconds(.infinity), UInt64(86_400.0 * 365 * 1_000_000_000))
        XCTAssertEqual(MeshSocket.nanoseconds(1.5), 1_500_000_000)
    }
}

actor Counter {
    var value = 0
    func bump() { value += 1 }
}
