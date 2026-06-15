import XCTest
@testable import MeshSocket

final class HandlerLifecycleTests: XCTestCase {
    /// `off` actually removes the handler for an event (no live server needed).
    func testOffRemovesHandler() async {
        let socket = MeshSocket(url: "ws://localhost:0", name: "UnitTest")
        await socket.on("demo") { _ in nil }
        let present = await socket.handlers.keys.contains("demo")
        XCTAssertTrue(present, "handler should be registered after on()")

        await socket.off("demo")
        let absent = await !socket.handlers.keys.contains("demo")
        XCTAssertTrue(absent, "handler should be gone after off()")
    }
}
