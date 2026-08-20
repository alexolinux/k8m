#!/usr/bin/env python3
"""
k8m.py - Real-time terminal monitor for Kubernetes pods.

Shows, per pod (refreshed live), the standard "get pods" info PLUS:
  - CPU / Memory usage (from metrics-server) vs requests/limits, with %
  - Restart counts, container states (waiting/terminated reasons, e.g. CrashLoopBackOff, OOMKilled)
  - Readiness / liveness probe status
  - Pod age, node, QoS class
  - A computed health flag (Healthy / Warning / Critical) to help spot
    unresponsive or unhealthy components at a glance
  - A per-refresh health summary (counts by status) in the footer

Requirements:
    pip install kubernetes rich

Usage:
    python k8m.py -n my-namespace
    python k8m.py -n my-namespace --interval 3
    python k8m.py -n my-namespace --kubeconfig ~/.kube/config --context prod
    python k8m.py -A                     # all namespaces
    python k8m.py -n my-namespace --sort cpu   # sort by cpu/mem/restarts/name
    python k8m.py -n my-namespace --debug --log-file k8m.log

Notes:
    - CPU/Memory usage requires metrics-server to be installed in the cluster.
      If it's not available, the script still runs and shows "n/a" for usage columns.
    - Read-only: this script never mutates cluster state.
    - While the live view is active, nothing is printed to stdout/stderr —
      logs go to a file instead. Mixing console writes with Rich's Live
      alternate-screen buffer is what causes the display to flicker/shake.
"""

import argparse
import logging
import os
import queue
import select
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

__version__ = "1.2.0"

try:
    import tty
    import termios
    _TTY_AVAILABLE = True
except ImportError:
    _TTY_AVAILABLE = False

log = logging.getLogger("k8m")

CPU_WARN_THRESHOLD = 90
MEM_WARN_THRESHOLD = 90

try:
    from kubernetes.utils import parse_quantity
except Exception:
    parse_quantity = None

try:
    from kubernetes import client, config
    from kubernetes.client.rest import ApiException
except ImportError:
    print("Missing dependency: pip install kubernetes", file=sys.stderr)
    sys.exit(1)

try:
    from rich.console import Console, Group
    from rich.live import Live
    from rich.table import Table
    from rich.text import Text
    from rich.panel import Panel
    from rich.align import Align
except ImportError:
    print("Missing dependency: pip install rich", file=sys.stderr)
    sys.exit(1)


# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #

def setup_logging(log_file: Optional[str], debug: bool, mirror_console: bool) -> None:
    """Configure logging.

    IMPORTANT: while the Rich `Live` alternate-screen view is running, nothing
    should be written straight to stdout/stderr — it corrupts/collides with
    the live redraw and is the main cause of a "shaking" table. So logs are
    routed to a file by default, and only mirrored to the console when we are
    NOT about to enter Live mode (e.g. --export snapshots).
    """
    handlers: List[logging.Handler] = []
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")

    if log_file:
        fh = logging.FileHandler(log_file)
        fh.setFormatter(fmt)
        handlers.append(fh)

    if mirror_console:
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        handlers.append(sh)

    if not handlers:
        handlers.append(logging.NullHandler())

    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        handlers=handlers,
        force=True,
    )


# --------------------------------------------------------------------------- #
# Helpers: unit parsing / formatting
# --------------------------------------------------------------------------- #

def parse_cpu(cpu_str: Optional[str]) -> float:
    """Parse a Kubernetes CPU quantity into millicores (float)."""
    if not cpu_str or not parse_quantity:
        return 0.0
    try:
        cores = float(parse_quantity(str(cpu_str)))
        return cores * 1000.0
    except Exception:
        log.debug("Failed to parse cpu %s", cpu_str)
        return 0.0


def parse_mem(mem_str: Optional[str]) -> float:
    """Parse a Kubernetes memory quantity into MiB (float)."""
    if not mem_str or not parse_quantity:
        return 0.0
    try:
        bytes_val = float(parse_quantity(str(mem_str)))
        return bytes_val / (1024 * 1024)
    except Exception:
        log.debug("Failed to parse memory %s", mem_str)
        return 0.0


def fmt_cpu(millicores: float) -> str:
    if millicores <= 0:
        return "-"
    if millicores >= 1000:
        return f"{millicores / 1000:.2f} core"
    return f"{millicores:.0f}m"


def fmt_mem(mib: float) -> str:
    if mib <= 0:
        return "-"
    if mib >= 1024:
        return f"{mib / 1024:.2f}Gi"
    return f"{mib:.0f}Mi"


def fmt_age(started: Optional[datetime], now: Optional[datetime] = None) -> str:
    if not started:
        return "n/a"
    now = now or datetime.now(timezone.utc)
    delta = now - started
    secs = int(delta.total_seconds())
    if secs < 0:
        secs = 0
    days, rem = divmod(secs, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    if days > 0:
        return f"{days}d{hours}h"
    if hours > 0:
        return f"{hours}h{minutes}m"
    return f"{minutes}m{rem}s" if minutes else f"{secs}s"


def pct(used: float, total: float) -> Optional[float]:
    if total <= 0:
        return None
    return (used / total) * 100


def pct_color(p: Optional[float]) -> str:
    if p is None:
        return "dim"
    if p >= 90:
        return "bold red"
    if p >= 75:
        return "yellow"
    return "green"


# --------------------------------------------------------------------------- #
# Data model
# --------------------------------------------------------------------------- #

@dataclass
class ContainerInfo:
    name: str
    ready: bool = False
    restarts: int = 0
    state: str = "unknown"          # running / waiting / terminated
    reason: str = ""                # e.g. CrashLoopBackOff, OOMKilled, Completed
    cpu_request_m: float = 0.0
    cpu_limit_m: float = 0.0
    mem_request_mi: float = 0.0
    mem_limit_mi: float = 0.0
    cpu_usage_m: float = 0.0
    mem_usage_mi: float = 0.0


@dataclass
class PodInfo:
    name: str
    namespace: str
    node: str = "-"
    phase: str = "Unknown"
    pod_ip: str = "-"
    qos: str = "-"
    started_at: Optional[datetime] = None
    containers: List[ContainerInfo] = field(default_factory=list)
    conditions: Dict[str, str] = field(default_factory=dict)

    @property
    def ready_count(self) -> Tuple[int, int]:
        total = len(self.containers)
        ready = sum(1 for c in self.containers if c.ready)
        return ready, total

    @property
    def total_restarts(self) -> int:
        return sum(c.restarts for c in self.containers)

    @property
    def cpu_usage_m(self) -> float:
        return sum(c.cpu_usage_m for c in self.containers)

    @property
    def mem_usage_mi(self) -> float:
        return sum(c.mem_usage_mi for c in self.containers)

    @property
    def cpu_request_m(self) -> float:
        return sum(c.cpu_request_m for c in self.containers)

    @property
    def cpu_limit_m(self) -> float:
        return sum(c.cpu_limit_m for c in self.containers)

    @property
    def mem_request_mi(self) -> float:
        return sum(c.mem_request_mi for c in self.containers)

    @property
    def mem_limit_mi(self) -> float:
        return sum(c.mem_limit_mi for c in self.containers)

    def health(self) -> Tuple[str, str]:
        """Returns (label, rich_style) — a coarse health verdict."""
        reasons_critical = {"CrashLoopBackOff", "OOMKilled", "Error", "ImagePullBackOff",
                             "ErrImagePull", "CreateContainerConfigError"}
        for c in self.containers:
            if c.reason in reasons_critical:
                return f"CRITICAL ({c.reason})", "bold white on red"

        if self.phase in ("Failed", "Unknown"):
            return f"CRITICAL ({self.phase})", "bold white on red"

        ready, total = self.ready_count
        if total > 0 and ready < total and self.phase == "Running":
            return "WARNING (not ready)", "bold black on yellow"

        if self.total_restarts >= 5:
            return f"WARNING ({self.total_restarts} restarts)", "bold black on yellow"

        cpu_p = pct(self.cpu_usage_m, self.cpu_limit_m)
        mem_p = pct(self.mem_usage_mi, self.mem_limit_mi)
        if (cpu_p and cpu_p >= CPU_WARN_THRESHOLD) or (mem_p and mem_p >= MEM_WARN_THRESHOLD):
            return "WARNING (near limit)", "bold black on yellow"

        if self.phase == "Pending":
            return "PENDING", "bold black on cyan"

        if self.phase == "Succeeded":
            return "COMPLETED", "dim"

        return "Healthy", "bold green"

    def health_bucket(self) -> str:
        """Coarse bucket used for the footer summary counts."""
        label, _ = self.health()
        if label.startswith("CRITICAL"):
            return "critical"
        if label.startswith("WARNING"):
            return "warning"
        if label == "PENDING":
            return "pending"
        if label == "COMPLETED":
            return "completed"
        return "healthy"


@dataclass
class DeploymentInfo:
    name: str
    namespace: str
    replicas_desired: int = 0
    replicas_current: int = 0
    replicas_ready: int = 0
    replicas_updated: int = 0
    replicas_available: int = 0
    age: str = "-"
    strategy: str = "-"
    labels: Dict[str, str] = field(default_factory=dict)
    selector: Dict[str, str] = field(default_factory=dict)
    conditions: Dict[str, str] = field(default_factory=dict)

    def health(self) -> Tuple[str, str]:
        """Returns (label, rich_style) — a coarse health verdict."""
        if self.replicas_desired == 0:
            return "SCALED TO 0", "dim"
        if self.replicas_ready < self.replicas_desired:
            if self.replicas_current == 0:
                return "CRITICAL (no pods)", "bold white on red"
            return f"WARNING ({self.replicas_ready}/{self.replicas_desired} ready)", "bold black on yellow"
        if self.replicas_updated < self.replicas_desired:
            return "UPDATING", "bold cyan"
        return "Healthy", "bold green"


@dataclass
class HPAInfo:
    name: str
    namespace: str
    target_kind: str = ""
    target_name: str = ""
    min_replicas: int = 0
    max_replicas: int = 0
    current_replicas: int = 0
    desired_replicas: int = 0
    cpu_target: Optional[int] = None
    mem_target: Optional[int] = None
    cpu_current: Optional[int] = None
    mem_current: Optional[int] = None
    age: str = "-"
    conditions: Dict[str, str] = field(default_factory=dict)

    def health(self) -> Tuple[str, str]:
        """Returns (label, rich_style) — a coarse health verdict."""
        if self.desired_replicas == 0 and self.min_replicas == 0:
            return "SCALED TO 0", "dim"
        if self.current_replicas == 0 and self.desired_replicas > 0:
            return "CRITICAL (no pods)", "bold white on red"
        if self.current_replicas < self.desired_replicas:
            return f"SCALING UP ({self.current_replicas}/{self.desired_replicas})", "bold cyan"
        if self.current_replicas > self.desired_replicas:
            return f"SCALING DOWN ({self.current_replicas}/{self.desired_replicas})", "bold yellow"
        if self.current_replicas >= self.max_replicas and self.desired_replicas >= self.max_replicas:
            return "AT MAX REPLICAS", "bold black on yellow"
        return "Healthy", "bold green"


# --------------------------------------------------------------------------- #
# Interactive display state & keyboard input
# --------------------------------------------------------------------------- #

@dataclass
class UIState:
    """Mutable display state updated by keyboard input and consumed by the render loop."""
    scroll_offset: int = 0
    sort_by: str = "name"
    sort_reverse: bool = False
    filter_text: str = ""
    filter_mode: bool = False


class KeyReader:
    """Background thread that reads keypresses from stdin and queues them for the render loop.

    Uses ``tty.setcbreak`` so Ctrl-C still raises KeyboardInterrupt while all
    other keys are delivered one-by-one without waiting for Enter.
    """

    _ESC_SEQ: Dict[bytes, str] = {
        b"\x1b[A": "up",    b"\x1b[B": "down",
        b"\x1b[C": "right", b"\x1b[D": "left",
        b"\x1b[5~": "pageup",  b"\x1b[6~": "pagedown",
        b"\x1b[H": "home",    b"\x1b[F": "end",
        b"\x1b[1~": "home",   b"\x1b[4~": "end",
        b"\x1b": "escape",
        b"\x7f": "backspace", b"\x08": "backspace",
        b"\r": "enter",       b"\n": "enter",
    }

    def __init__(self) -> None:
        self._queue: queue.Queue = queue.Queue()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._fd = sys.stdin.fileno()
        self._old_settings = None
        self._active = False

    def start(self) -> bool:
        """Put stdin into cbreak mode and start the reader thread.

        Returns True only when stdin is a real TTY and tty/termios are available.
        """
        if not _TTY_AVAILABLE or not sys.stdin.isatty():
            return False
        try:
            self._old_settings = termios.tcgetattr(self._fd)
            tty.setcbreak(self._fd)
            self._thread.start()
            self._active = True
            return True
        except (termios.error, AttributeError, OSError):
            return False

    def stop(self) -> None:
        """Stop the reader thread and restore the original terminal settings."""
        self._stop.set()
        if self._old_settings is not None:
            try:
                termios.tcsetattr(self._fd, termios.TCSADRAIN, self._old_settings)
            except (termios.error, OSError):
                pass

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                ready, _, _ = select.select([self._fd], [], [], 0.05)
                if ready:
                    data = os.read(self._fd, 32)
                    if not data:
                        break
                    key = self._decode(data)
                    if key:
                        self._queue.put(key)
            except (OSError, ValueError):
                break

    def _decode(self, data: bytes) -> Optional[str]:
        if data in self._ESC_SEQ:
            return self._ESC_SEQ[data]
        if len(data) == 1 and 0x20 <= data[0] <= 0x7E:
            return chr(data[0])
        return None

    def get_keys(self) -> List[str]:
        """Drain and return all pending keystrokes (non-blocking)."""
        keys: List[str] = []
        while True:
            try:
                keys.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return keys

    @property
    def active(self) -> bool:
        return self._active


# --------------------------------------------------------------------------- #
# Kubernetes data collection
# --------------------------------------------------------------------------- #

class ClusterCollector:
    def __init__(self, namespace: Optional[str], all_namespaces: bool, label_selector: Optional[str] = None):
        self.namespace = namespace
        self.all_namespaces = all_namespaces
        self.label_selector = label_selector
        self.core = client.CoreV1Api()
        self.custom = client.CustomObjectsApi()
        self._metrics_available = True

    def fetch_pods(self) -> List[PodInfo]:
        pods: List[PodInfo] = []
        continue_token = None
        while True:
            try:
                kwargs = {"watch": False, "limit": 250, "_continue": continue_token}
                if self.label_selector:
                    kwargs["label_selector"] = self.label_selector
                if self.all_namespaces:
                    resp = self.core.list_pod_for_all_namespaces(**kwargs)
                else:
                    resp = self.core.list_namespaced_pod(namespace=self.namespace, **kwargs)
            except ApiException as e:
                log.warning("Failed to list pods: %s", e)
                break

            for p in resp.items:
                pi = PodInfo(
                    name=p.metadata.name,
                    namespace=p.metadata.namespace,
                    node=p.spec.node_name or "-",
                    phase=p.status.phase or "Unknown",
                    pod_ip=p.status.pod_ip or "-",
                    qos=p.status.qos_class or "-",
                    started_at=p.status.start_time,
                )

                # container requests/limits from spec
                spec_containers = {c.name: c for c in (p.spec.containers or [])}

                statuses = p.status.container_statuses or []
                for cs in statuses:
                    ci = ContainerInfo(name=cs.name, ready=cs.ready, restarts=cs.restart_count)

                    if cs.state.running is not None:
                        ci.state = "running"
                    elif cs.state.waiting is not None:
                        ci.state = "waiting"
                        ci.reason = cs.state.waiting.reason or ""
                    elif cs.state.terminated is not None:
                        ci.state = "terminated"
                        ci.reason = cs.state.terminated.reason or ""

                    spec_c = spec_containers.get(cs.name)
                    if spec_c and spec_c.resources:
                        req = spec_c.resources.requests or {}
                        lim = spec_c.resources.limits or {}
                        ci.cpu_request_m = parse_cpu(req.get("cpu"))
                        ci.cpu_limit_m = parse_cpu(lim.get("cpu"))
                        ci.mem_request_mi = parse_mem(req.get("memory"))
                        ci.mem_limit_mi = parse_mem(lim.get("memory"))

                    pi.containers.append(ci)

                # pods with no container_statuses yet (e.g. Pending) -> synth from spec
                if not statuses and spec_containers:
                    for name, spec_c in spec_containers.items():
                        ci = ContainerInfo(name=name, ready=False, state="waiting", reason=pi.phase)
                        if spec_c.resources:
                            req = spec_c.resources.requests or {}
                            lim = spec_c.resources.limits or {}
                            ci.cpu_request_m = parse_cpu(req.get("cpu"))
                            ci.cpu_limit_m = parse_cpu(lim.get("cpu"))
                            ci.mem_request_mi = parse_mem(req.get("memory"))
                            ci.mem_limit_mi = parse_mem(lim.get("memory"))
                        pi.containers.append(ci)

                pods.append(pi)

            continue_token = getattr(resp.metadata, "_continue", None) if resp.metadata else None
            if not continue_token:
                break

        self._attach_metrics(pods)
        return pods

    def _attach_metrics(self, pods: List[PodInfo]) -> None:
        try:
            if self.all_namespaces:
                items = self.custom.list_cluster_custom_object(
                    "metrics.k8s.io", "v1beta1", "pods"
                ).get("items", [])
            else:
                items = self.custom.list_namespaced_custom_object(
                    "metrics.k8s.io", "v1beta1", self.namespace, "pods"
                ).get("items", [])
        except ApiException:
            # metrics-server not installed / not reachable — degrade gracefully
            self._metrics_available = False
            return
        except Exception:
            self._metrics_available = False
            return

        usage_by_pod: Dict[Tuple[str, str], Dict[str, Tuple[float, float]]] = {}
        for item in items:
            meta = item.get("metadata", {})
            key = (meta.get("namespace", ""), meta.get("name", ""))
            per_container = {}
            for c in item.get("containers", []):
                usage = c.get("usage", {})
                per_container[c.get("name")] = (
                    parse_cpu(usage.get("cpu")),
                    parse_mem(usage.get("memory")),
                )
            usage_by_pod[key] = per_container

        for pod in pods:
            per_container = usage_by_pod.get((pod.namespace, pod.name), {})
            for c in pod.containers:
                if c.name in per_container:
                    c.cpu_usage_m, c.mem_usage_mi = per_container[c.name]
        self._metrics_available = True

    def fetch_deployments(self) -> List[DeploymentInfo]:
        deployments: List[DeploymentInfo] = []
        continue_token = None
        apps = client.AppsV1Api()
        while True:
            try:
                kwargs = {"watch": False, "limit": 250, "_continue": continue_token}
                if self.label_selector:
                    kwargs["label_selector"] = self.label_selector
                if self.all_namespaces:
                    resp = apps.list_deployment_for_all_namespaces(**kwargs)
                else:
                    resp = apps.list_namespaced_deployment(namespace=self.namespace, **kwargs)
            except ApiException as e:
                log.warning("Failed to list deployments: %s", e)
                break

            for d in resp.items:
                started = d.metadata.creation_timestamp
                di = DeploymentInfo(
                    name=d.metadata.name,
                    namespace=d.metadata.namespace,
                    replicas_desired=d.spec.replicas or 0,
                    replicas_current=d.status.replicas or 0,
                    replicas_ready=d.status.ready_replicas or 0,
                    replicas_updated=d.status.updated_replicas or 0,
                    replicas_available=d.status.available_replicas or 0,
                    age=fmt_age(started) if started else "-",
                    strategy=d.spec.strategy.type if d.spec.strategy else "-",
                    labels=d.metadata.labels or {},
                    selector=d.spec.selector.match_labels if d.spec.selector else {},
                )
                for c in (d.status.conditions or []):
                    di.conditions[c.type] = f"{c.status} ({c.reason})" if c.reason else c.status
                deployments.append(di)

            continue_token = getattr(resp.metadata, "_continue", None) if resp.metadata else None
            if not continue_token:
                break
        return deployments

    def fetch_hpas(self) -> List[HPAInfo]:
        hpas: List[HPAInfo] = []
        continue_token = None
        autoscaling = client.AutoscalingV2Api()
        while True:
            try:
                kwargs = {"watch": False, "limit": 250, "_continue": continue_token}
                if self.label_selector:
                    kwargs["label_selector"] = self.label_selector
                if self.all_namespaces:
                    resp = autoscaling.list_horizontal_pod_autoscaler_for_all_namespaces(**kwargs)
                else:
                    resp = autoscaling.list_namespaced_horizontal_pod_autoscaler(namespace=self.namespace, **kwargs)
            except ApiException as e:
                # HPA v2 API might not be available, try v1
                if e.status == 404:
                    log.debug("HPA v2 API not available, trying v1")
                    return self._fetch_hpas_v1()
                log.warning("Failed to list HPAs: %s", e)
                break

            for h in resp.items:
                target_ref = h.spec.scale_target_ref
                cpu_target = None
                mem_target = None
                for metric in (h.spec.metrics or []):
                    if metric.type == "Resource":
                        res = metric.resource
                        if res and res.name == "cpu" and res.target:
                            if res.target.type == "Utilization" and res.target.average_utilization:
                                cpu_target = res.target.average_utilization
                        elif res and res.name == "memory" and res.target:
                            if res.target.type == "Utilization" and res.target.average_utilization:
                                mem_target = res.target.average_utilization

                started = h.metadata.creation_timestamp
                hi = HPAInfo(
                    name=h.metadata.name,
                    namespace=h.metadata.namespace,
                    target_kind=target_ref.kind if target_ref else "",
                    target_name=target_ref.name if target_ref else "",
                    min_replicas=h.spec.min_replicas or 0,
                    max_replicas=h.spec.max_replicas,
                    current_replicas=h.status.current_replicas or 0,
                    desired_replicas=h.status.desired_replicas or 0,
                    cpu_target=cpu_target,
                    mem_target=mem_target,
                    cpu_current=h.status.current_metrics[0].resource.current.average_utilization
                        if h.status.current_metrics and h.status.current_metrics[0].resource and h.status.current_metrics[0].resource.current and h.status.current_metrics[0].resource.current.average_utilization
                        else None,
                    age=fmt_age(started) if started else "-",
                )
                for c in (h.status.conditions or []):
                    hi.conditions[c.type] = f"{c.status} ({c.reason})" if c.reason else c.status
                hpas.append(hi)

            continue_token = getattr(resp.metadata, "_continue", None) if resp.metadata else None
            if not continue_token:
                break
        return hpas

    def _fetch_hpas_v1(self) -> List[HPAInfo]:
        """Fallback to HPA v1 API."""
        hpas: List[HPAInfo] = []
        continue_token = None
        autoscaling = client.AutoscalingV1Api()
        while True:
            try:
                kwargs = {"watch": False, "limit": 250, "_continue": continue_token}
                if self.label_selector:
                    kwargs["label_selector"] = self.label_selector
                if self.all_namespaces:
                    resp = autoscaling.list_horizontal_pod_autoscaler_for_all_namespaces(**kwargs)
                else:
                    resp = autoscaling.list_namespaced_horizontal_pod_autoscaler(namespace=self.namespace, **kwargs)
            except ApiException as e:
                log.warning("Failed to list HPAs (v1): %s", e)
                break

            for h in resp.items:
                target_ref = h.spec.scale_target_ref
                cpu_target = h.spec.target_cpu_utilization_percentage

                started = h.metadata.creation_timestamp
                hi = HPAInfo(
                    name=h.metadata.name,
                    namespace=h.metadata.namespace,
                    target_kind=target_ref.kind if target_ref else "",
                    target_name=target_ref.name if target_ref else "",
                    min_replicas=h.spec.min_replicas or 0,
                    max_replicas=h.spec.max_replicas,
                    current_replicas=h.status.current_replicas or 0,
                    desired_replicas=h.status.desired_replicas or 0,
                    cpu_target=cpu_target,
                    cpu_current=h.status.current_cpu_utilization_percentage,
                    age=fmt_age(started) if started else "-",
                )
                hpas.append(hi)

            continue_token = getattr(resp.metadata, "_continue", None) if resp.metadata else None
            if not continue_token:
                break
        return hpas


# --------------------------------------------------------------------------- #
# Rendering
# --------------------------------------------------------------------------- #

# Number-key → pod sort column
_SORT_KEY_MAP: Dict[str, str] = {
    "1": "name",    "2": "cpu",    "3": "mem",    "4": "restarts",
    "5": "age",     "6": "health", "7": "status", "8": "node",
}


def filter_and_sort_pods(pods: List[PodInfo], state: UIState,
                          all_namespaces: bool) -> List[PodInfo]:
    """Return pods after applying the text filter and sort order in *state*."""
    if state.filter_text:
        needle = state.filter_text.lower()
        pods = [
            p for p in pods if
            needle in p.name.lower() or
            needle in p.namespace.lower() or
            needle in p.phase.lower() or
            needle in p.node.lower()
        ]

    def sort_key(p: PodInfo):
        if state.sort_by == "cpu":       return -p.cpu_usage_m
        if state.sort_by == "mem":       return -p.mem_usage_mi
        if state.sort_by == "restarts":  return -p.total_restarts
        if state.sort_by == "age":
            return p.started_at or datetime.min.replace(tzinfo=timezone.utc)
        if state.sort_by == "health":
            order = {"critical": 0, "warning": 1, "pending": 2, "healthy": 3, "completed": 4}
            return order.get(p.health_bucket(), 5)
        if state.sort_by == "status":    return p.phase
        if state.sort_by == "node":      return p.node
        return p.name  # default: name

    return sorted(pods, key=sort_key, reverse=state.sort_reverse)


def handle_keys(keys: List[str], state: UIState, max_scroll: int,
                page_size: int, stop: dict) -> None:
    """Process a batch of queued keypresses, mutating *state* in place."""
    for key in keys:
        # ---- Filter typing mode ------------------------------------------ #
        if state.filter_mode:
            if key == "escape":
                state.filter_mode = False
                state.filter_text = ""
                state.scroll_offset = 0
            elif key == "enter":
                state.filter_mode = False
                state.scroll_offset = 0
            elif key == "backspace":
                state.filter_text = state.filter_text[:-1]
                state.scroll_offset = 0
            elif len(key) == 1:
                state.filter_text += key
                state.scroll_offset = 0
            continue

        # ---- Navigation & commands --------------------------------------- #
        if key == "q":
            stop["flag"] = True
        elif key in ("j", "down"):
            state.scroll_offset = min(state.scroll_offset + 1, max_scroll)
        elif key in ("k", "up"):
            state.scroll_offset = max(state.scroll_offset - 1, 0)
        elif key == "pagedown":
            state.scroll_offset = min(state.scroll_offset + page_size, max_scroll)
        elif key == "pageup":
            state.scroll_offset = max(state.scroll_offset - page_size, 0)
        elif key in ("g", "home"):
            state.scroll_offset = 0
        elif key in ("G", "end"):
            state.scroll_offset = max_scroll
        elif key == "r":
            state.sort_reverse = not state.sort_reverse
        elif key == "/":
            state.filter_mode = True
        elif key == "escape":
            state.filter_text = ""
            state.scroll_offset = 0
        elif key in _SORT_KEY_MAP:
            new_sort = _SORT_KEY_MAP[key]
            if state.sort_by == new_sort:
                state.sort_reverse = not state.sort_reverse  # second press toggles direction
            else:
                state.sort_by = new_sort
                state.sort_reverse = False


def build_table(pods: List[PodInfo], ui_state: UIState, all_namespaces: bool,
                metrics_available: bool, now: Optional[datetime] = None) -> Table:
    sort_arrow = "\u25bc" if ui_state.sort_reverse else "\u25b2"
    filter_indicator = f"  filter:'{ui_state.filter_text}'" if ui_state.filter_text else ""
    title = f"Kubernetes Pods  (sort: {ui_state.sort_by} {sort_arrow}{filter_indicator})"
    table = Table(title=title, expand=True, header_style="bold cyan", show_lines=False)

    if all_namespaces:
        table.add_column("NAMESPACE", style="magenta", ratio=1, min_width=10,
                         no_wrap=True, overflow="ellipsis")
    table.add_column("NAME",     ratio=3, min_width=20, no_wrap=True, overflow="ellipsis")
    table.add_column("READY",    justify="center", min_width=7,  no_wrap=True)
    table.add_column("STATUS",   min_width=10, no_wrap=True, overflow="ellipsis")
    table.add_column("RESTARTS", justify="right", min_width=9,  no_wrap=True)
    table.add_column("AGE",      justify="right", min_width=8,  no_wrap=True)
    table.add_column("CPU",      justify="right", min_width=9,  no_wrap=True)
    table.add_column("CPU%",     justify="right", min_width=6,  no_wrap=True)
    table.add_column("MEM",      justify="right", min_width=8,  no_wrap=True)
    table.add_column("MEM%",     justify="right", min_width=6,  no_wrap=True)
    table.add_column("NODE",     ratio=2, min_width=14, no_wrap=True, overflow="ellipsis")
    table.add_column("QOS",      min_width=10, no_wrap=True, overflow="ellipsis")
    table.add_column("HEALTH",   ratio=2, min_width=18, no_wrap=True, overflow="ellipsis")

    for pod in pods:
        ready, total = pod.ready_count
        cpu_p = pct(pod.cpu_usage_m, pod.cpu_limit_m)
        mem_p = pct(pod.mem_usage_mi, pod.mem_limit_mi)
        health_label, health_style = pod.health()

        row = []
        if all_namespaces:
            row.append(pod.namespace)
        row.extend([
            pod.name,
            f"{ready}/{total}",
            pod.phase,
            str(pod.total_restarts) if pod.total_restarts < 5 else f"[yellow]{pod.total_restarts}[/yellow]",
            fmt_age(pod.started_at, now),
            fmt_cpu(pod.cpu_usage_m) if metrics_available else "n/a",
            f"[{pct_color(cpu_p)}]{cpu_p:.0f}%[/]" if cpu_p is not None else "-",
            fmt_mem(pod.mem_usage_mi) if metrics_available else "n/a",
            f"[{pct_color(mem_p)}]{mem_p:.0f}%[/]" if mem_p is not None else "-",
            pod.node,
            pod.qos,
            f"[{health_style}]{health_label}[/{health_style}]",
        ])
        table.add_row(*row)

    return table


def build_deployment_table(deployments: List[DeploymentInfo], all_namespaces: bool,
                           sort_by: str, sort_reverse: bool = False,
                           now: Optional[datetime] = None) -> Table:
    table = Table(title="Deployments", expand=True, header_style="bold cyan", show_lines=False)

    if all_namespaces:
        table.add_column("NAMESPACE",  style="magenta", ratio=1, min_width=10,
                         no_wrap=True, overflow="ellipsis")
    table.add_column("NAME",       ratio=3, min_width=20, no_wrap=True, overflow="ellipsis")
    table.add_column("READY",      justify="center", min_width=9,  no_wrap=True)
    table.add_column("UP-TO-DATE", justify="center", min_width=11, no_wrap=True)
    table.add_column("AVAILABLE",  justify="center", min_width=10, no_wrap=True)
    table.add_column("AGE",        justify="right",  min_width=8,  no_wrap=True)
    table.add_column("STRATEGY",   min_width=12, no_wrap=True, overflow="ellipsis")
    table.add_column("HEALTH",     ratio=2, min_width=20, no_wrap=True, overflow="ellipsis")

    def sort_key(d: DeploymentInfo):
        if sort_by == "ready": return -(d.replicas_ready)
        if sort_by == "age":   return d.age
        return d.name

    for dep in sorted(deployments, key=sort_key, reverse=sort_reverse):
        health_label, health_style = dep.health()
        row = []
        if all_namespaces:
            row.append(dep.namespace)
        row.extend([
            dep.name,
            f"{dep.replicas_ready}/{dep.replicas_desired}",
            f"{dep.replicas_updated}/{dep.replicas_desired}",
            f"{dep.replicas_available}/{dep.replicas_desired}",
            dep.age,
            dep.strategy,
            f"[{health_style}]{health_label}[/{health_style}]",
        ])
        table.add_row(*row)

    return table


def build_hpa_table(hpas: List[HPAInfo], all_namespaces: bool, sort_by: str,
                    sort_reverse: bool = False, now: Optional[datetime] = None) -> Table:
    table = Table(title="Horizontal Pod Autoscalers", expand=True,
                  header_style="bold cyan", show_lines=False)

    if all_namespaces:
        table.add_column("NAMESPACE", style="magenta", ratio=1, min_width=10,
                         no_wrap=True, overflow="ellipsis")
    table.add_column("NAME",    ratio=2, min_width=20, no_wrap=True, overflow="ellipsis")
    table.add_column("TARGET",  ratio=2, min_width=18, no_wrap=True, overflow="ellipsis")
    table.add_column("MIN",     justify="right", min_width=5,  no_wrap=True)
    table.add_column("MAX",     justify="right", min_width=5,  no_wrap=True)
    table.add_column("CURRENT", justify="right", min_width=8,  no_wrap=True)
    table.add_column("DESIRED", justify="right", min_width=8,  no_wrap=True)
    table.add_column("CPU%",    justify="center", min_width=7, no_wrap=True)
    table.add_column("MEM%",    justify="center", min_width=7, no_wrap=True)
    table.add_column("AGE",     justify="right",  min_width=8, no_wrap=True)
    table.add_column("HEALTH",  ratio=2, min_width=20, no_wrap=True, overflow="ellipsis")

    def sort_key(h: HPAInfo):
        if sort_by == "desired": return -h.desired_replicas
        if sort_by == "current": return -h.current_replicas
        if sort_by == "age":     return h.age
        return h.name

    for hpa in sorted(hpas, key=sort_key, reverse=sort_reverse):
        health_label, health_style = hpa.health()
        target_str = f"{hpa.target_kind}/{hpa.target_name}" if hpa.target_kind and hpa.target_name else "-"
        cpu_str = f"{hpa.cpu_current}/{hpa.cpu_target}" if hpa.cpu_target else "-"
        mem_str = f"{hpa.mem_current}/{hpa.mem_target}" if hpa.mem_target else "-"
        row = []
        if all_namespaces:
            row.append(hpa.namespace)
        row.extend([
            hpa.name, target_str,
            str(hpa.min_replicas), str(hpa.max_replicas),
            str(hpa.current_replicas), str(hpa.desired_replicas),
            cpu_str, mem_str, hpa.age,
            f"[{health_style}]{health_label}[/{health_style}]",
        ])
        table.add_row(*row)

    return table


def health_summary(pods: List[PodInfo]) -> Dict[str, int]:
    counts = {"healthy": 0, "warning": 0, "critical": 0, "pending": 0, "completed": 0}
    for p in pods:
        counts[p.health_bucket()] += 1
    return counts


def build_footer(namespace_label: str, interval: int, metrics_available: bool,
                 error: Optional[str], pods: List[PodInfo], monitor_started_at: datetime,
                 ui_state: UIState, total_filtered: int, total_all: int,
                 visible_rows: int, key_reader_active: bool) -> Panel:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    counts = health_summary(pods)

    summary = (
        f"[bold green]{counts['healthy']} healthy[/bold green]  "
        f"[bold black on yellow]{counts['warning']} warning[/bold black on yellow]  "
        f"[bold white on red]{counts['critical']} critical[/bold white on red]  "
        f"[bold cyan]{counts['pending']} pending[/bold cyan]  "
        f"[dim]{counts['completed']} completed[/dim]"
    )

    # Scroll position
    if total_filtered > 0:
        end_row = min(ui_state.scroll_offset + visible_rows, total_filtered)
        scroll_info = f"[{ui_state.scroll_offset + 1}\u2013{end_row}/{total_filtered}]"
    else:
        scroll_info = "[0/0]"
    if total_filtered < total_all:
        scroll_info += f" [yellow](of {total_all})[/yellow]"

    sort_arrow = "\u25bc" if ui_state.sort_reverse else "\u25b2"

    # Filter display
    if ui_state.filter_mode:
        filter_display = (
            f"   [bold yellow on black] FILTER: {ui_state.filter_text}\u2588 [/bold yellow on black]"
        )
    elif ui_state.filter_text:
        filter_display = f"   [yellow]filter:[/yellow] [italic]{ui_state.filter_text}[/italic]"
    else:
        filter_display = ""

    parts = [
        f"[bold]scope:[/bold] {namespace_label}",
        f"[bold]pods:[/bold] {len(pods)} ({summary})",
        f"[bold]scroll:[/bold] {scroll_info}",
        f"[bold]sort:[/bold] [cyan]{ui_state.sort_by} {sort_arrow}[/cyan]",
        f"[bold]uptime:[/bold] {fmt_age(monitor_started_at)}",
        f"[bold]interval:[/bold] {interval}s",
        f"[bold]updated:[/bold] {ts}",
        f"[bold]metrics:[/bold] {'[green]ok[/green]' if metrics_available else '[red]n/a[/red]'}",
    ]
    if error:
        parts.append(f"[bold red]error:[/bold red] {error}")

    line1 = "   ".join(parts) + filter_display

    if key_reader_active:
        line2 = (
            "[dim]j/k \u2191/\u2193:scroll   PgDn/PgUp:page   g/G:top/bot   "
            "r:reverse   /:filter   Esc:clear   1-8:sort   q:quit[/dim]"
        )
    else:
        line2 = "[dim]Ctrl+C to quit[/dim]"

    return Panel(Align.left(Text.from_markup(f"{line1}\n{line2}")), border_style="dim")


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def load_kube_config(kubeconfig: Optional[str], context: Optional[str]) -> None:
    try:
        config.load_incluster_config()
        log.info("Loaded in-cluster kubeconfig")
        return
    except Exception:
        pass
    try:
        config.load_kube_config(config_file=kubeconfig, context=context)
        log.info("Loaded kubeconfig %s context=%s", kubeconfig or "~/.kube/config", context or "default")
    except Exception as e:
        log.error("Failed to load kubeconfig: %s", e)
        sys.exit(1)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Real-time Kubernetes pod monitor.")
    parser.add_argument("--version", action="version", version=f"k8m {__version__}")
    ns_group = parser.add_mutually_exclusive_group(required=True)
    ns_group.add_argument("-n", "--namespace", help="Namespace to monitor")
    ns_group.add_argument("-A", "--all-namespaces", action="store_true",
                           help="Monitor pods across all namespaces")
    parser.add_argument("--interval", type=int, default=2, help="Refresh interval in seconds (default: 2)")
    parser.add_argument("--kubeconfig", default=None, help="Path to kubeconfig file")
    parser.add_argument("--context", default=None, help="Kubeconfig context to use")
    parser.add_argument("--sort",
                         choices=["name", "cpu", "mem", "restarts", "age", "health", "status", "node"],
                         default="name",
                         help="Initial pod sort column (default: name; interactive keys: 1-8)")
    parser.add_argument("--sort-reverse", action="store_true",
                         help="Start with sort order reversed")
    parser.add_argument("--label-selector", default=None, help="Label selector e.g. app=web")
    parser.add_argument("--export", choices=["json", "csv"],
                         help="Export a single snapshot to stdout instead of the live view")
    parser.add_argument("--cpu-threshold", type=int, default=90, help="CPU warning threshold %% (1-100)")
    parser.add_argument("--mem-threshold", type=int, default=90, help="Memory warning threshold %% (1-100)")
    parser.add_argument("--log-file", default="k8m.log",
                         help="Log file while the live view is running (default: k8m.log)")
    parser.add_argument("--debug", action="store_true", help="Enable debug-level logging")
    parser.add_argument("--deployments", action="store_true", help="Also watch deployments")
    parser.add_argument("--hpas", action="store_true", help="Also watch horizontal pod autoscalers")
    parser.add_argument("--deployment-sort", choices=["name", "ready", "age"], default="name",
                         help="Sort deployments by column (default: name)")
    parser.add_argument("--hpa-sort", choices=["name", "desired", "current", "age"], default="name",
                         help="Sort HPAs by column (default: name)")
    return parser


def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    if args.interval < 1:
        args.interval = 1

    for name, val in (("--cpu-threshold", args.cpu_threshold), ("--mem-threshold", args.mem_threshold)):
        if not 1 <= val <= 100:
            parser.error(f"{name} must be between 1 and 100 (got {val})")

    setup_logging(log_file=args.log_file, debug=args.debug, mirror_console=bool(args.export))

    global CPU_WARN_THRESHOLD, MEM_WARN_THRESHOLD
    CPU_WARN_THRESHOLD = args.cpu_threshold
    MEM_WARN_THRESHOLD = args.mem_threshold

    load_kube_config(args.kubeconfig, args.context)
    collector = ClusterCollector(namespace=args.namespace, all_namespaces=args.all_namespaces,
                                  label_selector=args.label_selector)
    console = Console()
    namespace_label = "ALL NAMESPACES" if args.all_namespaces else args.namespace

    # ------------------------------------------------------------------ #
    # Export mode — single snapshot, then exit
    # ------------------------------------------------------------------ #
    if args.export:
        pods = collector.fetch_pods()
        if args.export == "json":
            import json
            out: Dict = {"pods": [], "deployments": [], "hpas": []}
            for p in pods:
                out["pods"].append({
                    "name": p.name, "namespace": p.namespace, "phase": p.phase,
                    "node": p.node, "qos": p.qos,
                    "ready": f"{p.ready_count[0]}/{p.ready_count[1]}",
                    "restarts": p.total_restarts,
                    "cpu_usage_m": p.cpu_usage_m, "mem_usage_mi": p.mem_usage_mi,
                    "health": p.health()[0],
                })
            if args.deployments:
                for d in collector.fetch_deployments():
                    out["deployments"].append({
                        "name": d.name, "namespace": d.namespace,
                        "replicas_desired": d.replicas_desired, "replicas_ready": d.replicas_ready,
                        "replicas_current": d.replicas_current, "replicas_updated": d.replicas_updated,
                        "replicas_available": d.replicas_available,
                        "age": d.age, "strategy": d.strategy, "health": d.health()[0],
                    })
            if args.hpas:
                for h in collector.fetch_hpas():
                    out["hpas"].append({
                        "name": h.name, "namespace": h.namespace,
                        "target": f"{h.target_kind}/{h.target_name}",
                        "min_replicas": h.min_replicas, "max_replicas": h.max_replicas,
                        "current_replicas": h.current_replicas, "desired_replicas": h.desired_replicas,
                        "cpu_target": h.cpu_target, "cpu_current": h.cpu_current,
                        "mem_target": h.mem_target, "mem_current": h.mem_current,
                        "age": h.age, "health": h.health()[0],
                    })
            print(json.dumps(out, indent=2))
        else:
            import csv
            writer = csv.writer(sys.stdout)
            writer.writerow(["namespace", "name", "phase", "node", "qos", "ready",
                              "restarts", "cpu_usage_m", "mem_usage_mi", "health"])
            for p in pods:
                writer.writerow([p.namespace, p.name, p.phase, p.node, p.qos,
                                  f"{p.ready_count[0]}/{p.ready_count[1]}",
                                  p.total_restarts, p.cpu_usage_m, p.mem_usage_mi, p.health()[0]])
            if args.deployments:
                writer.writerow([])
                writer.writerow(["namespace", "name", "desired", "ready", "current",
                                  "updated", "available", "age", "strategy", "health"])
                for d in collector.fetch_deployments():
                    writer.writerow([d.namespace, d.name, d.replicas_desired, d.replicas_ready,
                                      d.replicas_current, d.replicas_updated, d.replicas_available,
                                      d.age, d.strategy, d.health()[0]])
            if args.hpas:
                writer.writerow([])
                writer.writerow(["namespace", "name", "target", "min", "max",
                                  "current", "desired", "cpu%", "mem%", "age", "health"])
                for h in collector.fetch_hpas():
                    writer.writerow([
                        h.namespace, h.name, f"{h.target_kind}/{h.target_name}",
                        h.min_replicas, h.max_replicas, h.current_replicas, h.desired_replicas,
                        f"{h.cpu_current}/{h.cpu_target}" if h.cpu_target else "-",
                        f"{h.mem_current}/{h.mem_target}" if h.mem_target else "-",
                        h.age, h.health()[0],
                    ])
        return

    # ------------------------------------------------------------------ #
    # Interactive live view
    # ------------------------------------------------------------------ #
    ui_state = UIState(sort_by=args.sort, sort_reverse=args.sort_reverse)

    key_reader = KeyReader()
    key_reader_active = key_reader.start()
    log.info("Interactive key reader: %s", "enabled" if key_reader_active else "disabled (not a TTY)")

    stop = {"flag": False}

    def _handle_sigterm(signum, frame):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _handle_sigterm)

    try:
        monitor_started_at = datetime.now(timezone.utc)
        with Live(console=console, refresh_per_second=4, screen=True, transient=True) as live:
            while not stop["flag"]:
                error = None
                try:
                    t0 = time.monotonic()
                    pods = collector.fetch_pods()
                    deployments = collector.fetch_deployments() if args.deployments else []
                    hpas = collector.fetch_hpas() if args.hpas else []
                    elapsed = (time.monotonic() - t0) * 1000
                    log.info("Fetched %d pods, %d deployments, %d HPAs in %.0f ms",
                             len(pods), len(deployments), len(hpas), elapsed)
                except ApiException as e:
                    pods = []; deployments = []; hpas = []
                    error = f"{e.status} {e.reason}"
                    log.warning("API error: %s", error)
                except Exception as e:
                    pods = []; deployments = []; hpas = []
                    error = str(e)
                    log.exception("Unexpected error fetching resources")

                now = datetime.now(timezone.utc)

                # Estimate visible pod rows based on terminal height.
                # Overhead: pod-table header area (4) + footer panel (4) +
                # each extra table consumes ~12 lines on average.
                extra_tables = (1 if args.deployments else 0) + (1 if args.hpas else 0)
                visible_rows = max(1, console.height - 8 - extra_tables * 12)

                # Pre-build static tables (deployments, HPAs) — only rebuilt on each API cycle.
                static_tables: List = []
                if args.deployments:
                    static_tables.append(
                        build_deployment_table(deployments, args.all_namespaces,
                                               args.deployment_sort, now=now))
                if args.hpas:
                    static_tables.append(
                        build_hpa_table(hpas, args.all_namespaces, args.hpa_sort, now=now))

                # Inner render + key-input loop for one refresh interval.
                sleep_end = time.monotonic() + max(1, args.interval)
                while True:
                    remaining = sleep_end - time.monotonic()
                    if remaining <= 0 or stop["flag"]:
                        break

                    # Apply filter/sort, clamp scroll offset.
                    sorted_filtered = filter_and_sort_pods(pods, ui_state, args.all_namespaces)
                    total_filtered = len(sorted_filtered)
                    total_all = len(pods)
                    max_scroll = max(0, total_filtered - visible_rows)
                    ui_state.scroll_offset = min(ui_state.scroll_offset, max_scroll)

                    visible_pods = sorted_filtered[
                        ui_state.scroll_offset: ui_state.scroll_offset + visible_rows
                    ]

                    pod_table = build_table(visible_pods, ui_state, args.all_namespaces,
                                            collector._metrics_available, now)
                    footer = build_footer(
                        namespace_label, args.interval, collector._metrics_available,
                        error, pods, monitor_started_at,
                        ui_state, total_filtered, total_all, visible_rows, key_reader_active,
                    )
                    live.update(Group(pod_table, *static_tables, footer))

                    # Sleep in 50 ms increments for low-latency key handling.
                    time.sleep(min(0.05, remaining))

                    if key_reader_active:
                        keys = key_reader.get_keys()
                        if keys:
                            handle_keys(keys, ui_state, max_scroll, visible_rows, stop)

    except KeyboardInterrupt:
        pass
    finally:
        key_reader.stop()
        console.print("[dim]Stopped.[/dim]")


if __name__ == "__main__":
    main()

