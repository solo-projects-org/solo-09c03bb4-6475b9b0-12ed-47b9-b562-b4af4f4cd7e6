"""pytest 引导：隔离模式（python -I）下补回仓库根目录，并确保样表存在。

网页层测试会对仓库内置样表真实发布提交，这里在每个测试后幂等重建样表，
保证测试之间以及测试与手工验收之间互不污染。
"""

import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
SAMPLES = ROOT / "samples"


def _generate_samples() -> None:
    sys.path.insert(0, str(ROOT / "scripts"))
    from generate_samples import main as generate
    generate()


def _ensure_samples() -> None:
    for name in ("healthy_table", "incident_table"):
        log = SAMPLES / name / "_delta_log"
        if not log.is_dir() or not list(log.glob("*.json")):
            _generate_samples()
            return


_ensure_samples()


@pytest.fixture(autouse=True)
def _reset_builtin_samples():
    """每个测试前后都把内置样表还原成未启用约束的初始布局。"""
    if SAMPLES.exists():
        shutil.rmtree(SAMPLES)
    _generate_samples()
    yield
    shutil.rmtree(SAMPLES, ignore_errors=True)
    _generate_samples()

