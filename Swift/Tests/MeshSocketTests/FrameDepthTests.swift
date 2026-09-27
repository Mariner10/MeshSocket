import XCTest
@testable import MeshSocket

/// Deeply nested inbound frames are dropped before `JSONSerialization` sees them
/// (it overflows a 512 KB concurrency-thread stack on objects ~400-511 deep).
final class FrameDepthTests: XCTestCase {
    private func nestedObject(_ depth: Int) -> Data {
        Data((String(repeating: "{\"a\":", count: depth) + "1" + String(repeating: "}", count: depth)).utf8)
    }

    private func nestedArray(_ depth: Int) -> Data {
        Data((String(repeating: "[", count: depth) + "1" + String(repeating: "]", count: depth)).utf8)
    }

    /// Runs `body` on a thread with the same 512 KB stack as the receive task.
    private func onSmallStack(_ body: @escaping @Sendable () -> Void) {
        let done = expectation(description: "small-stack thread finished")
        let thread = Thread {
            body()
            done.fulfill()
        }
        thread.stackSize = 512 * 1024
        thread.start()
        wait(for: [done], timeout: 30)
    }

    func testDeepObjectFrameIsDroppedOnSmallStack() {
        let frame = nestedObject(511)
        onSmallStack {
            XCTAssertNil(MeshSocket.decodeFrame(frame, maxDepth: MeshSocket.defaultMaxFrameDepth))
        }
    }

    func testDeepArrayFrameIsDroppedOnSmallStack() {
        let frame = Data(("{\"type\":\"x\",\"payload\":".utf8)) + nestedArray(511) + Data("}".utf8)
        onSmallStack {
            XCTAssertNil(MeshSocket.decodeFrame(frame, maxDepth: MeshSocket.defaultMaxFrameDepth))
        }
    }

    func testDepthAtCapPassesAndOneOverIsDropped() {
        XCTAssertNotNil(MeshSocket.decodeFrame(nestedObject(128), maxDepth: 128))
        XCTAssertNil(MeshSocket.decodeFrame(nestedObject(129), maxDepth: 128))
        XCTAssertNil(MeshSocket.decodeFrame(nestedObject(9), maxDepth: 8), "cap is configurable")
    }

    func testBracketsInsideStringsDoNotCount() {
        let deepText = String(repeating: "{[", count: 400)
        let frame = Data("{\"type\":\"msg\",\"payload\":\"\(deepText)\"}".utf8)
        XCTAssertEqual(MeshSocket.jsonDepth(of: frame), 1)
        XCTAssertEqual(MeshSocket.decodeFrame(frame, maxDepth: 128)?["payload"] as? String, deepText)
    }

    func testEscapedQuotesKeepStringState() {
        // `\"` stays inside the string, `\\` ends the escape before the closing quote.
        let frame = Data(#"{"a":"x\"{{[[","b":"y\\","c":[{"d":1}]}"#.utf8)
        XCTAssertEqual(MeshSocket.jsonDepth(of: frame), 3)
        let packet = MeshSocket.decodeFrame(frame, maxDepth: 128)
        XCTAssertEqual(packet?["a"] as? String, "x\"{{[[")
        XCTAssertEqual(packet?["b"] as? String, "y\\")
    }

    func testNormalFramePassesUnchanged() {
        let frame = Data(#"{"id":"1","type":"sync","payload":{"k":[1,2,{"v":"}]"}]},"reply_to":null}"#.utf8)
        let packet = MeshSocket.decodeFrame(frame, maxDepth: MeshSocket.defaultMaxFrameDepth)
        let expected = try? JSONSerialization.jsonObject(with: frame) as? [String: Any]
        XCTAssertNotNil(packet)
        XCTAssertEqual(packet.map { NSDictionary(dictionary: $0) }, expected.map { NSDictionary(dictionary: $0) })
    }

    func testMalformedAndNonObjectFramesAreDropped() {
        XCTAssertNil(MeshSocket.decodeFrame(Data("{\"a\":".utf8), maxDepth: 128))
        XCTAssertNil(MeshSocket.decodeFrame(Data("[1,2]".utf8), maxDepth: 128))
        XCTAssertEqual(MeshSocket.jsonDepth(of: Data("42".utf8)), 0)
    }

    /// `{"Ģ":{"ĢĢ":{"ĢĢ":…1}}}`: U+0122 carries a `0x22` byte in UTF-16/32, a fake
    /// quote. The odd first level leaves a byte scanner "inside a string" and the
    /// even levels after it keep it there, so every deeper bracket is hidden.
    private func deepWideFrame(_ depth: Int, _ encoding: String.Encoding) -> Data {
        let text = "{\"\u{0122}\":" + String(repeating: "{\"\u{0122}\u{0122}\":", count: depth - 1)
            + "1" + String(repeating: "}", count: depth)
        return text.data(using: encoding)!
    }

    func testDeepUTF16And32FramesAreDroppedOnSmallStack() {
        let encodings: [String.Encoding] = [.utf16LittleEndian, .utf16BigEndian, .utf16,
                                            .utf32LittleEndian, .utf32BigEndian, .utf32]
        for encoding in encodings {
            let frame = deepWideFrame(510, encoding)
            onSmallStack {
                XCTAssertNil(MeshSocket.decodeFrame(frame, maxDepth: MeshSocket.defaultMaxFrameDepth),
                             "\(encoding)")
            }
        }
    }

    func testNulBytesAndUTF16BOMsCountAsTooDeep() {
        // Shallow wide-encoded frames parse fine in JSONSerialization, but are refused.
        let shallow = deepWideFrame(2, .utf16LittleEndian)
        XCTAssertNotNil(try? JSONSerialization.jsonObject(with: shallow))
        XCTAssertEqual(MeshSocket.jsonDepth(of: shallow, limit: 128), 129)
        XCTAssertNil(MeshSocket.decodeFrame(shallow, maxDepth: 128))
        XCTAssertNil(MeshSocket.decodeFrame(Data([0xFF, 0xFE]) + Data("{}".utf8), maxDepth: 128))
        XCTAssertNil(MeshSocket.decodeFrame(Data([0xFE, 0xFF]) + Data("{}".utf8), maxDepth: 128))
        XCTAssertNil(MeshSocket.decodeFrame(Data("{\"a\":1}\u{0}".utf8), maxDepth: 128))
        XCTAssertEqual(MeshSocket.jsonDepth(of: Data([0x7B, 0x00, 0x7D])), .max)
        // A UTF-8 BOM is still UTF-8, so the scan stays valid.
        XCTAssertEqual(MeshSocket.jsonDepth(of: Data([0xEF, 0xBB, 0xBF]) + Data("{}".utf8)), 1)
    }

    func testSocketExposesConfiguredCap() {
        XCTAssertEqual(MeshSocket(url: "ws://localhost:0").maxFrameDepth, 128)
        XCTAssertEqual(MeshSocket(url: "ws://localhost:0", maxFrameDepth: 32).maxFrameDepth, 32)
    }
}
