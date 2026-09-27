"""Point the app at a throwaway SQLite database before any app import."""
import os
import tempfile

_dir = tempfile.mkdtemp(prefix="contract-registry-test-")
os.environ["DATABASE_URL"] = f"sqlite:///{_dir}/test.db"
