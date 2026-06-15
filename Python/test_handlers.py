import unittest
from socketCore import MeshSocket


class HandlerLifecycleTests(unittest.TestCase):
    def test_off_removes_handler(self):
        s = MeshSocket(url="ws://localhost:0", name="UnitTest")

        def demo(payload):
            return None

        s.on("demo", demo)
        self.assertIn("demo", s.handlers)

        s.off("demo")
        self.assertNotIn("demo", s.handlers)


if __name__ == "__main__":
    unittest.main()
