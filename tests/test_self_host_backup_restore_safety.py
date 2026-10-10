from __future__ import annotations

import gzip
import os
import shutil
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _fixture_repo(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "repo"
    scripts = root / "deploy" / "self-host"
    scripts.mkdir(parents=True)
    for name in ("backup-postgres.sh", "restore-postgres-test.sh"):
        shutil.copy2(REPO_ROOT / "deploy" / "self-host" / name, scripts / name)
    (scripts / ".env").write_text(
        "POSTGRES_DB=tradescanner\nPOSTGRES_USER=tradescanner\n",
        encoding="utf-8",
    )
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    docker_log = tmp_path / "docker-called.log"
    docker = bin_dir / "docker"
    docker.write_text(
        "#!/bin/sh\nprintf 'called\\n' >> \"$MOCK_DOCKER_LOG\"\nexit 99\n",
        encoding="utf-8",
    )
    docker.chmod(0o755)
    return root, bin_dir


def _run(script: Path, *, bin_dir: Path, docker_log: Path, **extra_env: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.update(extra_env)
    env["PATH"] = f"{bin_dir}{os.pathsep}{env.get('PATH', '')}"
    env["MOCK_DOCKER_LOG"] = str(docker_log)
    return subprocess.run(
        ["bash", str(script)],
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )


def test_backup_refuses_directory_inside_repository(tmp_path: Path) -> None:
    root, bin_dir = _fixture_repo(tmp_path)
    docker_log = tmp_path / "docker-called.log"

    result = _run(
        root / "deploy" / "self-host" / "backup-postgres.sh",
        bin_dir=bin_dir,
        docker_log=docker_log,
        BACKUP_DIR=str(root / "backups"),
    )

    assert result.returncode != 0
    assert "Refusing to place database backups inside the Git repository" in result.stderr
    assert not docker_log.exists()


def test_restore_refuses_production_database_name(tmp_path: Path) -> None:
    root, bin_dir = _fixture_repo(tmp_path)
    docker_log = tmp_path / "docker-called.log"
    backup = tmp_path / "valid.sql.gz"
    with gzip.open(backup, "wb") as handle:
        handle.write(b"-- synthetic database dump\n")

    result = _run(
        root / "deploy" / "self-host" / "restore-postgres-test.sh",
        bin_dir=bin_dir,
        docker_log=docker_log,
        BACKUP_FILE=str(backup),
        RESTORE_DATABASE="tradescanner",
    )

    assert result.returncode != 0
    assert "Refusing to restore into the production database" in result.stderr
    assert not docker_log.exists()


def test_restore_rejects_unsafe_database_name_before_docker(tmp_path: Path) -> None:
    root, bin_dir = _fixture_repo(tmp_path)
    docker_log = tmp_path / "docker-called.log"
    backup = tmp_path / "valid.sql.gz"
    with gzip.open(backup, "wb") as handle:
        handle.write(b"-- synthetic database dump\n")

    result = _run(
        root / "deploy" / "self-host" / "restore-postgres-test.sh",
        bin_dir=bin_dir,
        docker_log=docker_log,
        BACKUP_FILE=str(backup),
        RESTORE_DATABASE="bad-name;DROP_DATABASE",
    )

    assert result.returncode != 0
    assert "may contain only letters, numbers, and underscores" in result.stderr
    assert not docker_log.exists()


def test_restore_rejects_corrupt_compressed_backup_before_docker(tmp_path: Path) -> None:
    root, bin_dir = _fixture_repo(tmp_path)
    docker_log = tmp_path / "docker-called.log"
    backup = tmp_path / "corrupt.sql.gz"
    backup.write_bytes(b"not a gzip file")

    result = _run(
        root / "deploy" / "self-host" / "restore-postgres-test.sh",
        bin_dir=bin_dir,
        docker_log=docker_log,
        BACKUP_FILE=str(backup),
        RESTORE_DATABASE="tradescanner_restore_test",
    )

    assert result.returncode != 0
    assert not docker_log.exists()
