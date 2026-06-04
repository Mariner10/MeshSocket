import Foundation

public enum MeshSocketError: Error, Sendable {
    case notConnected
    case invalidURL
    case timeout
}

public actor MeshSocket {
    public let url: URL
    public let name: String
    public private(set) var id: String

    private let authToken: String?
    private let channel: String?
    private let role: String?
    private let canBroadcast: Bool?
    private let canRoute: Bool?
    private let canCrossChannelRoute: Bool?
    private let canMonitor: Bool?
    private let broadcastScope: String?

    private let maxOfflineBuffer: Int
    private let offlineFilePath: String?
    private let onReconnect: (@Sendable () async -> Void)?
    private let onDisconnect: (@Sendable () async -> Void)?

    private let rateLimit: Int
    private var msgTimes: [Date] = []

    private var webSocketTask: URLSessionWebSocketTask?
    private var isRunning = false
    private var startTime: Date?

    private var handlers: [String: @Sendable (Any?) async -> Any?] = [:]
    private var pendingRequests: [String: CheckedContinuation<Any?, Error>] = [:]

    private var ramBuffer: [String] = []

    private var readyContinuations: [CheckedContinuation<Void, Never>] = []
    private var _isConnected = false

    private var connectionTask: Task<Void, Never>?
    private var listenTask: Task<Void, Never>?

    public init(
        url: String,
        name: String = "Node",
        authToken: String? = nil,
        rateLimit: Int = 0,
        channel: String? = nil,
        role: String? = nil,
        canBroadcast: Bool? = nil,
        canRoute: Bool? = nil,
        canCrossChannelRoute: Bool? = nil,
        canMonitor: Bool? = nil,
        broadcastScope: String? = nil,
        maxOfflineBuffer: Int = 0,
        offlineFilePath: String? = nil,
        onReconnect: (@Sendable () async -> Void)? = nil,
        onDisconnect: (@Sendable () async -> Void)? = nil
    ) {
        guard let parsed = URL(string: url) else {
            fatalError("Invalid MeshSocket URL: \(url)")
        }
        self.url = parsed
        self.name = name
        self.id = UUID().uuidString
        self.authToken = authToken ?? ProcessInfo.processInfo.environment["MESH_AUTH_TOKEN"]
        self.channel = channel ?? ProcessInfo.processInfo.environment["MESH_CHANNEL"]
        self.role = role ?? ProcessInfo.processInfo.environment["MESH_ROLE"]
        self.canBroadcast = canBroadcast
        self.canRoute = canRoute
        self.canCrossChannelRoute = canCrossChannelRoute
        self.canMonitor = canMonitor
        self.broadcastScope = broadcastScope ?? ProcessInfo.processInfo.environment["MESH_BROADCAST_SCOPE"]
        self.maxOfflineBuffer = maxOfflineBuffer
        self.offlineFilePath = offlineFilePath
        self.rateLimit = rateLimit
        self.onReconnect = onReconnect
        self.onDisconnect = onDisconnect

        handlers["handshake"] = { [weak self] payload in
            guard let dict = payload as? [String: Any],
                  let t = dict["t"] as? Double ?? (dict["t"] as? String).flatMap(Double.init) else {
                return nil
            }
            let currentID = await self?.id ?? ""
            return ["server_id": currentID, "l": Date().timeIntervalSince1970 - t] as [String: Any]
        }
        handlers["ping"] = { _ in "pong" }
        handlers["status_request"] = { [weak self] _ in
            let currentName = await self?.name ?? ""
            let currentID = await self?.id ?? ""
            return [
                "name": currentName,
                "id": currentID,
                "status": "online",
                "memory_usage": "unknown",
            ] as [String: Any]
        }
    }

    // MARK: - Public API

    public func start() async {
        guard !isRunning else { return }
        isRunning = true
        startTime = Date()
        connectionTask = Task { [weak self] in
            await self?.maintainConnection()
        }
    }

    public func stop() async {
        isRunning = false
        listenTask?.cancel()
        listenTask = nil
        webSocketTask?.cancel(with: .goingAway, reason: nil)
        webSocketTask = nil
        connectionTask?.cancel()
        connectionTask = nil
        setDisconnected()
    }

    public func waitUntilReady() async {
        if _isConnected { return }
        await withCheckedContinuation { continuation in
            readyContinuations.append(continuation)
        }
    }

    public func on(_ type: String, handler: @escaping @Sendable (Any?) async -> Any?) {
        handlers[type] = handler
    }

    @discardableResult
    public func send(_ type: String, payload: Any? = nil, replyTo: String? = nil) async throws -> String {
        let msgID = UUID().uuidString
        let packet = buildPacket(id: msgID, type: type, payload: payload, replyTo: replyTo)

        if _isConnected, let ws = webSocketTask {
            do {
                try await ws.send(.string(packet))
                return msgID
            } catch {
                // Fall through to buffering
            }
        }

        if maxOfflineBuffer > 0 {
            await handleOfflineBuffering(packet)
            return msgID
        }

        throw MeshSocketError.notConnected
    }

    @discardableResult
    public func emit(_ type: String, payload: Any? = nil, replyTo: String? = nil) async throws -> String {
        try await send(type, payload: payload, replyTo: replyTo)
    }

    public func request(_ type: String, payload: Any? = nil, timeout seconds: TimeInterval = 5.0) async -> Any? {
        if !_isConnected {
            await waitUntilReady()
        }

        do {
            let msgID = try await send(type, payload: payload)
            return try await withThrowingTaskGroup(of: Any?.self) { group in
                group.addTask { [self] in
                    try await withCheckedThrowingContinuation { continuation in
                        Task { await self.storeContinuation(continuation, for: msgID) }
                    }
                }
                group.addTask {
                    try await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
                    return nil
                }

                guard let first = try await group.next() else { return nil }
                group.cancelAll()

                if first == nil {
                    await removePending(msgID)
                }
                return first
            }
        } catch {
            return nil
        }
    }

    public func reportStatus(metrics: [String: Any] = [:]) async throws {
        let payload: [String: Any] = [
            "name": name,
            "id": id,
            "status": "online",
            "uptime": startTime.map { Date().timeIntervalSince($0) } ?? 0,
            "metrics": metrics,
        ]
        try await send("node_status", payload: payload)
    }

    // MARK: - Connection Management

    private func maintainConnection() async {
        var retryDelay: UInt64 = 2

        while isRunning {
            do {
                let session = URLSession(configuration: .default)
                let ws = session.webSocketTask(with: url)
                ws.resume()

                try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
                    ws.sendPing { error in
                        if let error { cont.resume(throwing: error) }
                        else { cont.resume() }
                    }
                }

                webSocketTask = ws
                _isConnected = true
                retryDelay = 2

                try await send("identify", payload: buildIdentityPayload())
                await flushOfflineQueue()

                signalReady()

                if let onReconnect {
                    await onReconnect()
                }

                await listenLoop(ws: ws)

            } catch is CancellationError {
                break
            } catch {
                if let onDisconnect {
                    await onDisconnect()
                }
            }

            setDisconnected()
            webSocketTask = nil
            failAllPendingRequests()

            guard isRunning else { break }

            do {
                try await Task.sleep(nanoseconds: retryDelay * 1_000_000_000)
            } catch {
                break
            }
            retryDelay = min(retryDelay * 2, 30)
        }
    }

    private func listenLoop(ws: URLSessionWebSocketTask) async {
        while isRunning {
            do {
                let message = try await ws.receive()

                if rateLimit > 0 {
                    let now = Date()
                    msgTimes.append(now)
                    if msgTimes.count > rateLimit {
                        msgTimes.removeFirst(msgTimes.count - rateLimit)
                    }
                    if msgTimes.count == rateLimit,
                       now.timeIntervalSince(msgTimes[0]) < 1.0 {
                        ws.cancel(with: .policyViolation, reason: "Rate limit exceeded".data(using: .utf8))
                        return
                    }
                }

                switch message {
                case .string(let text):
                    guard let data = text.data(using: .utf8),
                          let packet = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
                        continue
                    }
                    Task { [weak self] in await self?.processPacket(packet) }
                case .data(let data):
                    guard let packet = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
                        continue
                    }
                    Task { [weak self] in await self?.processPacket(packet) }
                @unknown default:
                    continue
                }
            } catch {
                break
            }
        }
    }

    private func processPacket(_ packet: [String: Any]) async {
        let msgID = packet["id"] as? String
        let msgType = packet["type"] as? String
        let payload = packet["payload"]
        let replyTo = packet["reply_to"] as? String

        if let replyTo, let continuation = pendingRequests.removeValue(forKey: replyTo) {
            continuation.resume(returning: payload)
            return
        }

        if msgType == "welcome" {
            if let dict = payload as? [String: Any],
               let serverID = dict["id"] as? String {
                self.id = serverID
            }
            return
        }

        guard let msgType, let handler = handlers[msgType] else { return }

        let response = await handler(payload)
        if let response, let msgID {
            _ = try? await send(msgType, payload: response, replyTo: msgID)
        }
    }

    // MARK: - Ready State

    private func signalReady() {
        let waiters = readyContinuations
        readyContinuations.removeAll()
        for c in waiters { c.resume() }
    }

    private func setDisconnected() {
        _isConnected = false
    }

    // MARK: - Pending Requests

    private func storeContinuation(_ continuation: CheckedContinuation<Any?, Error>, for id: String) {
        pendingRequests[id] = continuation
    }

    private func removePending(_ id: String) {
        if let c = pendingRequests.removeValue(forKey: id) {
            c.resume(returning: nil)
        }
    }

    private func failAllPendingRequests() {
        for (_, continuation) in pendingRequests {
            continuation.resume(throwing: CancellationError())
        }
        pendingRequests.removeAll()
    }

    // MARK: - Offline Buffering

    private func handleOfflineBuffering(_ packetStr: String) async {
        if ramBuffer.count >= maxOfflineBuffer {
            if offlineFilePath != nil {
                await dumpRAMToDisk()
                await appendToDisk(packetStr)
            } else {
                ramBuffer.removeFirst()
                ramBuffer.append(packetStr)
            }
        } else {
            ramBuffer.append(packetStr)
        }
    }

    private func dumpRAMToDisk() async {
        guard !ramBuffer.isEmpty, let path = offlineFilePath else { return }
        let lines = ramBuffer
        ramBuffer.removeAll()
        await writeLinesToFile(lines, path: path)
    }

    private func appendToDisk(_ packetStr: String) async {
        guard let path = offlineFilePath else { return }
        await writeLinesToFile([packetStr], path: path)
    }

    private nonisolated func writeLinesToFile(_ lines: [String], path: String) async {
        let content = lines.map { $0 + "\n" }.joined()
        let url = URL(fileURLWithPath: path)
        do {
            if FileManager.default.fileExists(atPath: path) {
                let handle = try FileHandle(forWritingTo: url)
                handle.seekToEndOfFile()
                if let data = content.data(using: .utf8) {
                    handle.write(data)
                }
                handle.closeFile()
            } else {
                try content.write(toFile: path, atomically: true, encoding: .utf8)
            }
        } catch {
            // Mirrors Python: logs error but doesn't throw
        }
    }

    private func flushOfflineQueue() async {
        if let path = offlineFilePath, FileManager.default.fileExists(atPath: path) {
            do {
                let content = try String(contentsOfFile: path, encoding: .utf8)
                let lines = content.components(separatedBy: "\n").filter { !$0.isEmpty }
                for line in lines {
                    try? await webSocketTask?.send(.string(line))
                }
                try FileManager.default.removeItem(atPath: path)
            } catch {
                // Best effort
            }
        }

        if !ramBuffer.isEmpty {
            for packet in ramBuffer {
                try? await webSocketTask?.send(.string(packet))
            }
            ramBuffer.removeAll()
        }
    }

    // MARK: - Packet Building

    private func buildPacket(id: String, type: String, payload: Any?, replyTo: String?) -> String {
        var dict: [String: Any] = [
            "id": id,
            "type": type,
        ]
        dict["payload"] = payload is NSNull ? NSNull() : (payload ?? NSNull())
        dict["reply_to"] = replyTo ?? NSNull()

        if let data = try? JSONSerialization.data(withJSONObject: dict),
           let str = String(data: data, encoding: .utf8) {
            return str
        }
        return "{}"
    }

    private func buildIdentityPayload() -> [String: Any] {
        var payload: [String: Any] = [
            "name": name,
            "token": authToken as Any,
        ]
        if let channel { payload["channel"] = channel }
        if let role { payload["role"] = role }
        if let canBroadcast { payload["can_broadcast"] = canBroadcast }
        if let canRoute { payload["can_route"] = canRoute }
        if let canCrossChannelRoute { payload["can_cross_channel_route"] = canCrossChannelRoute }
        if let canMonitor { payload["can_monitor"] = canMonitor }
        if let broadcastScope { payload["broadcast_scope"] = broadcastScope }
        return payload
    }
}
