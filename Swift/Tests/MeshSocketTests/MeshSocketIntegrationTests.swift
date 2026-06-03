import XCTest
@testable import MeshSocket

final class MeshSocketIntegrationTests: XCTestCase {
    static let serverURL = ProcessInfo.processInfo.environment["MESH_SERVER_URL"] ?? "ws://localhost:8765"
    static let authToken = ProcessInfo.processInfo.environment["MESH_AUTH_TOKEN"] ?? "test-token"

    private func makeSocket(
        name: String = "SwiftTest",
        channel: String? = nil,
        role: String = "node",
        maxOfflineBuffer: Int = 0,
        offlineFilePath: String? = nil,
        onReconnect: (@Sendable () async -> Void)? = nil,
        onDisconnect: (@Sendable () async -> Void)? = nil
    ) -> MeshSocket {
        MeshSocket(
            url: Self.serverURL,
            name: name,
            authToken: Self.authToken,
            channel: channel,
            role: role,
            canBroadcast: true,
            canRoute: true,
            canCrossChannelRoute: false,
            canMonitor: false,
            maxOfflineBuffer: maxOfflineBuffer,
            offlineFilePath: offlineFilePath,
            onReconnect: onReconnect,
            onDisconnect: onDisconnect
        )
    }

    // MARK: - Connection & Auth

    func testConnectionAndAuth() async throws {
        let socket = makeSocket()
        await socket.start()
        await socket.waitUntilReady()

        let pong = await socket.request("ping")
        XCTAssertEqual(pong as? String, "pong")

        await socket.stop()
    }

    func testBadTokenRejection() async throws {
        let badSocket = MeshSocket(
            url: Self.serverURL,
            name: "BadAuth",
            authToken: "wrong-token",
            role: "node",
            canBroadcast: true
        )

        let receiver = makeSocket(name: "AuthReceiver")
        let shouldNotReceive = expectation(description: "Bad client broadcast should not arrive")
        shouldNotReceive.isInverted = true

        await receiver.on("broadcast") { payload in
            if let dict = payload as? [String: Any], let msg = dict["msg"] as? String, msg == "from-bad-auth" {
                shouldNotReceive.fulfill()
            }
            return nil
        }

        await receiver.start()
        await receiver.waitUntilReady()

        await badSocket.start()
        await badSocket.waitUntilReady()

        // Bad-token client is not added to server's client set,
        // so broadcast_request won't reach other mesh members
        _ = await badSocket.request("broadcast_request", payload: ["msg": "from-bad-auth"], timeout: 2.0)

        await fulfillment(of: [shouldNotReceive], timeout: 3.0)

        await badSocket.stop()
        await receiver.stop()
    }

    // MARK: - Ping/Pong

    func testPingPong() async throws {
        let socket = makeSocket()
        await socket.start()
        await socket.waitUntilReady()

        let result = await socket.request("ping")
        XCTAssertEqual(result as? String, "pong")

        await socket.stop()
    }

    // MARK: - Handshake

    func testHandshakeLatency() async throws {
        let socket = makeSocket()
        await socket.start()
        await socket.waitUntilReady()

        let t = Date().timeIntervalSince1970
        let result = await socket.request("handshake", payload: ["t": t])
        let dict = result as? [String: Any]
        XCTAssertNotNil(dict)
        XCTAssertNotNil(dict?["l"])

        if let latency = dict?["l"] as? Double {
            XCTAssertLessThan(latency, 5.0, "Latency should be reasonable")
        }

        await socket.stop()
    }

    // MARK: - Broadcast

    func testSendAndBroadcast() async throws {
        let sender = makeSocket(name: "Sender", channel: "test-broadcast")
        let receiver = makeSocket(name: "Receiver", channel: "test-broadcast")

        let received = expectation(description: "Receiver gets broadcast")

        await receiver.start()
        await receiver.waitUntilReady()

        await receiver.on("broadcast") { payload in
            if let dict = payload as? [String: Any],
               let msg = dict["msg"] as? String, msg == "hello-from-swift" {
                received.fulfill()
            }
            return nil
        }

        await sender.start()
        await sender.waitUntilReady()

        _ = await sender.request("broadcast_request", payload: ["msg": "hello-from-swift"])

        await fulfillment(of: [received], timeout: 5.0)

        await sender.stop()
        await receiver.stop()
    }

    // MARK: - Request/Response

    func testRequestResponse() async throws {
        let socket = makeSocket()
        await socket.start()
        await socket.waitUntilReady()

        let result = await socket.request("ping", timeout: 5.0)
        XCTAssertEqual(result as? String, "pong")

        await socket.stop()
    }

    func testRequestTimeout() async throws {
        let socket = makeSocket()
        await socket.start()
        await socket.waitUntilReady()

        // Register a handler on echo-client that deliberately doesn't respond
        // Instead, request a type that nobody handles — it should time out
        let result = await socket.request("nonexistent_type_that_nobody_handles", timeout: 2.0)
        XCTAssertNil(result, "Request to unhandled type should time out and return nil")

        await socket.stop()
    }

    // MARK: - Direct Routing via Echo Client

    func testDirectRouting() async throws {
        let clientA = makeSocket(name: "RouterA", channel: "test-route")
        let clientB = makeSocket(name: "RouterB", channel: "test-route")

        await clientB.start()
        await clientB.waitUntilReady()

        await clientB.on("echo_test") { payload in
            guard let dict = payload as? [String: Any] else { return nil }
            return ["echoed": dict["data"] ?? "none"] as [String: Any]
        }

        await clientA.start()
        await clientA.waitUntilReady()

        let clientBID = await clientB.id
        let result = await clientA.request("route_msg", payload: [
            "target_id": clientBID,
            "type": "echo_test",
            "payload": ["data": "hello-routed"],
        ])

        let dict = result as? [String: Any]
        XCTAssertEqual(dict?["echoed"] as? String, "hello-routed")

        await clientA.stop()
        await clientB.stop()
    }

    // MARK: - Offline Buffering

    func testOfflineBuffering() async throws {
        let bufferFile = FileManager.default.temporaryDirectory.appendingPathComponent("mesh_buffer_test.jsonl").path
        try? FileManager.default.removeItem(atPath: bufferFile)

        let socket = makeSocket(
            name: "BufferTest",
            maxOfflineBuffer: 5,
            offlineFilePath: bufferFile
        )

        // Send while not connected — should buffer without throwing
        try await socket.send("buffered_msg", payload: ["seq": 1])
        try await socket.send("buffered_msg", payload: ["seq": 2])
        try await socket.send("buffered_msg", payload: ["seq": 3])

        // Connect — buffered messages flush on connect
        await socket.start()
        await socket.waitUntilReady()

        // Verify the socket is functional after buffering + flush
        let pong = await socket.request("ping")
        XCTAssertEqual(pong as? String, "pong", "Socket should work after flushing buffered messages")

        await socket.stop()
        try? FileManager.default.removeItem(atPath: bufferFile)
    }

    // MARK: - Channel Isolation

    func testChannelIsolation() async throws {
        let clientA = makeSocket(name: "ChannelA", channel: "alpha")
        let clientB = makeSocket(name: "ChannelB", channel: "beta")

        let shouldNotReceive = expectation(description: "B should NOT get A's broadcast")
        shouldNotReceive.isInverted = true

        await clientB.on("broadcast") { payload in
            if let dict = payload as? [String: Any], let msg = dict["msg"] as? String, msg == "alpha-only" {
                shouldNotReceive.fulfill()
            }
            return nil
        }

        await clientA.start()
        await clientA.waitUntilReady()
        await clientB.start()
        await clientB.waitUntilReady()

        _ = await clientA.request("broadcast_request", payload: ["msg": "alpha-only"])

        await fulfillment(of: [shouldNotReceive], timeout: 3.0)

        await clientA.stop()
        await clientB.stop()
    }

    // MARK: - Reconnection

    func testReconnectionCallback() async throws {
        let reconnected = expectation(description: "onReconnect fires")
        reconnected.assertForOverFulfill = false

        var connectCount = 0
        let socket = makeSocket(
            name: "ReconnectTest",
            onReconnect: {
                connectCount += 1
                if connectCount >= 1 {
                    reconnected.fulfill()
                }
            }
        )

        await socket.start()
        await socket.waitUntilReady()

        await fulfillment(of: [reconnected], timeout: 5.0)
        await socket.stop()
    }
}
