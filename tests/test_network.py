"""Real process/socket test. Run locally or in CORE; skipped if sockets are blocked."""

import errno
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]


class NetworkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
        except OSError as exc:
            if exc.errno in (errno.EPERM, errno.EACCES):
                raise unittest.SkipTest("this environment forbids local sockets") from exc
            raise

    def test_run_script_fanout_and_selectivity_from_another_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            with socket.socket() as tcp, socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp:
                tcp.bind(("127.0.0.1", 0))
                udp.bind(("127.0.0.1", 0))
                control_port, data_port = tcp.getsockname()[1], udp.getsockname()[1]
            endpoint = f"broker_control=127.0.0.1:{control_port}\n"
            configs = [
                ("subscriber", "sub1", endpoint + f"log={folder}/sub1.log\nduration=2.5\n"
                 "subscribe=north/* qos=2\nsubscribe=north/station7/* qos=2\n"),
                ("subscriber", "sub2", endpoint + f"log={folder}/sub2.log\nduration=2.5\n"
                 "subscribe=north/* qos=1\n"),
                ("subscriber", "sub3", endpoint + f"log={folder}/sub3.log\nduration=2.5\n"
                 "subscribe=south/* qos=1\n"),
                ("publisher", "pub1", endpoint + f"log={folder}/pub1.log\nstart_after=0.7\ndrain=1\n"
                 "publish=north/station7/temperature qos=2 count=8 rate=20\n"),
                ("broker", "broker1", f"bind=127.0.0.1\ncontrol_port={control_port}\n"
                 f"data_port={data_port}\nduration=3\nevict_after=1\n"),
            ]
            processes = []
            started_ms = time.time_ns() // 1_000_000
            try:
                for role, identity, contents in configs:
                    path = folder / (identity + ".conf")
                    path.write_text(contents)
                    processes.append(subprocess.Popen(
                        [str(ROOT / "run.sh"), role, "--id", identity, "--config", str(path)],
                        cwd=directory, stdout=subprocess.PIPE, stderr=subprocess.PIPE))
                for process in processes:
                    stdout, stderr = process.communicate(timeout=8)
                    self.assertEqual(process.returncode, 0, stderr.decode())
                    self.assertEqual(stdout, b"")
                logs = {identity: [row.split() for row in (folder / (identity + ".log")).read_text().splitlines()]
                        for identity in ("pub1", "sub1", "sub2", "sub3")}
                expected = {(row[2], row[3]) for row in logs["pub1"]}
                self.assertEqual(len(expected), 8)
                self.assertEqual(len(logs["sub1"]), 8)
                self.assertEqual(logs["sub3"], [])
                for identity in ("sub1", "sub2"):
                    self.assertEqual({(row[2], row[3]) for row in logs[identity]}, expected)
                for identity, rows in logs.items():
                    for row in rows:
                        self.assertEqual(len(row), 4)
                        self.assertEqual(row[1], identity)
                        self.assertGreaterEqual(int(row[0]), started_ms)
                        self.assertLessEqual(int(row[0]), time.time_ns() // 1_000_000)
            finally:
                for process in processes:
                    if process.poll() is None:
                        process.kill()
                    process.communicate()


if __name__ == "__main__":
    unittest.main()
