"""MeshSocket — lightweight WebSocket mesh networking.

`from meshsocket import MeshSocket` is the supported public import. The historical
`from socketCore import MeshSocket` continues to work (the module is published alongside)
so existing code keeps running while it migrates to the package name.
"""
from socketCore import MeshSocket, LogColors

__all__ = ["MeshSocket", "LogColors"]
__version__ = "0.1.0"
