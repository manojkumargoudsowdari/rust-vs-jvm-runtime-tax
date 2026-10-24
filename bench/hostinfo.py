"""Host validation and environment capture for manifest.json."""

from __future__ import annotations

import glob
import os
import platform
import re
import shutil
import subprocess
from pathlib import Path


def _read(path: str) -> str | None:
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def _run(*cmd: str) -> str | None:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def cgroup_fs_type() -> str | None:
    return _run("stat", "-fc", "%T", "/sys/fs/cgroup")


def hypervisor_flag() -> bool:
    cpuinfo = _read("/proc/cpuinfo") or ""
    return bool(re.search(r"^flags\s*:.*\bhypervisor\b", cpuinfo, re.M))


def timing_blockers() -> list[str]:
    """Reasons this host must not record benchmark timings (empty = OK)."""
    reasons = []
    if platform.system() != "Linux":
        reasons.append(f"host OS is {platform.system()}, not Linux")
        return reasons
    fs = cgroup_fs_type()
    if fs != "cgroup2fs":
        reasons.append(f"/sys/fs/cgroup is {fs!r}, not cgroup2fs (cgroup v2 required)")
    if hypervisor_flag():
        reasons.append("CPU reports the 'hypervisor' flag: this is a VM, not bare metal")
    return reasons


def _lscpu() -> dict[str, str]:
    out = _run("lscpu") or ""
    return {k.strip(): v.strip() for k, _, v in (line.partition(":") for line in out.splitlines()) if v}


def _distinct(paths: str) -> list[str]:
    return sorted({v for p in glob.glob(paths) if (v := _read(p)) is not None})


def boost_state() -> dict[str, str | None]:
    return {
        "cpufreq_boost": _read("/sys/devices/system/cpu/cpufreq/boost"),
        "intel_pstate_no_turbo": _read("/sys/devices/system/cpu/intel_pstate/no_turbo"),
        "amd_pstate_status": _read("/sys/devices/system/cpu/amd_pstate/status"),
    }


def os_release() -> str | None:
    text = _read("/etc/os-release") or ""
    m = re.search(r'^PRETTY_NAME="?([^"\n]*)"?', text, re.M)
    return m.group(1) if m else None


def host_section() -> dict:
    lscpu = _lscpu()
    meminfo = _read("/proc/meminfo") or ""
    mem_kb = re.search(r"MemTotal:\s+(\d+)", meminfo)
    return {
        "os": platform.system(),
        "os_release": os_release(),
        "kernel": platform.release(),
        "arch": platform.machine(),
        "cpu_model": lscpu.get("Model name"),
        "cpus_logical": os.cpu_count(),
        "threads_per_core": lscpu.get("Thread(s) per core"),
        "sockets": lscpu.get("Socket(s)"),
        "numa_nodes": lscpu.get("NUMA node(s)"),
        "mem_total_bytes": int(mem_kb.group(1)) * 1024 if mem_kb else None,
        "cpu_governors": _distinct("/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor"),
        "cpu_drivers": _distinct("/sys/devices/system/cpu/cpu*/cpufreq/scaling_driver"),
        "boost": boost_state(),
        "smt_control": _read("/sys/devices/system/cpu/smt/control"),
        "smt_active": _read("/sys/devices/system/cpu/smt/active"),
        "cgroup_fs": cgroup_fs_type(),
        "hypervisor_flag": hypervisor_flag(),
        "systemd_detect_virt": _run("systemd-detect-virt") if shutil.which("systemd-detect-virt") else None,
        "kernel_cmdline": _read("/proc/cmdline"),
        "transparent_hugepage": _read("/sys/kernel/mm/transparent_hugepage/enabled"),
        "python": platform.python_version(),
    }


def docker_section(image_refs: dict[str, str]) -> dict:
    info = _run("docker", "info", "--format",
                "{{.ServerVersion}}|{{.CgroupDriver}}|{{.CgroupVersion}}|{{.KernelVersion}}") or "|||"
    server, driver, cgv, kernel = info.split("|")
    images = {}
    for name, ref in image_refs.items():
        fmt = "{{.Id}}|{{json .RepoDigests}}|{{json .Config.Labels}}"
        raw = _run("docker", "image", "inspect", "--format", fmt, ref)
        if raw:
            image_id, digests, labels = raw.split("|", 2)
            images[name] = {"ref": ref, "id": image_id, "repo_digests": digests, "labels": labels}
        else:
            images[name] = {"ref": ref, "id": None}
    return {
        "server_version": server or None,
        "client_version": _run("docker", "version", "--format", "{{.Client.Version}}"),
        "cgroup_driver": driver or None,
        "cgroup_version": cgv or None,
        "kernel_version": kernel or None,
        "images": images,
    }


def git_section(repo: Path) -> dict:
    sha = _run("git", "-C", str(repo), "rev-parse", "HEAD")
    status = _run("git", "-C", str(repo), "status", "--porcelain")
    return {
        "sha": sha,
        "dirty": bool(status) if status is not None else None,
        "describe": _run("git", "-C", str(repo), "describe", "--tags", "--always", "--dirty"),
    }


def can_drop_caches() -> bool:
    if platform.system() != "Linux":
        return False
    if os.geteuid() == 0:
        return os.access("/proc/sys/vm/drop_caches", os.W_OK)
    return subprocess.run(["sudo", "-n", "true"], capture_output=True).returncode == 0


def drop_caches() -> None:
    os.sync()
    if os.geteuid() == 0:
        Path("/proc/sys/vm/drop_caches").write_text("3\n")
    else:
        subprocess.run(["sudo", "-n", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"], check=True)
