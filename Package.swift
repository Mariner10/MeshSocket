// swift-tools-version: 5.9

import PackageDescription

let package = Package(
    name: "MeshSocket",
    platforms: [
        .macOS(.v13),
        .iOS(.v16),
    ],
    products: [
        .library(name: "MeshSocket", targets: ["MeshSocket"]),
    ],
    targets: [
        .target(name: "MeshSocket", path: "Swift/Sources/MeshSocket"),
        .testTarget(name: "MeshSocketTests", dependencies: ["MeshSocket"], path: "Swift/Tests/MeshSocketTests"),
    ]
)
