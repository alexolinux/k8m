# K8m

Real-time terminal monitor for Kubernetes pods, deployments and Horizontal Pod Autoscalers (HPAs), with CPU/Memory usage, health flags, and export to JSON/CSV.

## Features

- Live-updating terminal view (Rich) of **pods**, optionally **deployments** and **HPAs**
- **CPU / Memory usage** (from metrics-server) vs requests/limits, with percentage and color-coded warning levels
- Container states with reasons (`CrashLoopBackOff`, `OOMKilled`, `ImagePullBackOff`, ...)
- Restart counts, readiness, pod age, node, QoS class
- Computed **health flag** per object (Healthy / Warning / Critical / Pending / Completed)
- Health summary footer (counts by status) refreshed on every cycle
- Sortable columns, label-selector filtering, namespace or all-namespaces scope
- One-shot **export** to JSON or CSV (no live view)
- Read-only: never mutates cluster state

## Requirements

| Requirement | Notes |
|---|---|
| Python >= 3.9 | |
| Kubernetes cluster + valid kubeconfig | loaded from `~/.kube/config` (or in-cluster if running inside a pod) |
| `metrics-server` installed in the cluster | required for CPU/Memory usage columns; without it the tool still runs and shows `n/a` |

## Installation

Pull this repo

```shell
git clone https://github.com/alexolinux/k8m.git
cd k8m
```

Create the required python virtual environment

```shell
# Using toml
python3 -m venv .venv          # create the venv
source .venv/bin/activate      # activate it (or .venv\Scripts\activate on Windows)
pip install -e .               # installs k8m + deps (kubernetes, rich)
```
Or

```shell
# Using requirements.txt
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Quick start

```shell
# Monitor pods in the "default" namespace (refreshes every 2s)
k8m -n default

# All namespaces, filtered by label, sorted by CPU usage
k8m -A --label-selector app=web --sort cpu

# Also watch deployments and HPAs
k8m -n production --deployments --hpas

# Use a specific kubeconfig / context
k8m -n production --kubeconfig ~/.kube/prod.yaml --context prod

# One-shot snapshot, exported as JSON / CSV
k8m -n default --export json
k8m -A --export csv > snapshot.csv

# Debug logging to a file while the live view runs
k8m -n default --debug --log-file k8m.log
```

## Usage steps

1. **Point at your cluster** – make sure `kubectl config current-context` returns the cluster you want to watch (or pass `--kubeconfig`/`--context`).
2. **Pick a scope** – provide `-n <namespace>` or `-A` (one of the two is required).
3. **Start monitoring** – run `k8m`; the live table redraws every `--interval` seconds. Press `Ctrl+C` to quit.
4. **Drill down** (optional) – add `--deployments` and/or `--hpas` to watch those resources in the same view; use `--sort`/`--deployment-sort`/`--hpa-sort` to reorder.
5. **Export** (optional, replaces live monitoring) – add `--export json|csv` to print a single snapshot to stdout and exit. Combine with `--deployments`/`--hpas` to include those resources in the export (logs go to stderr, so stdout stays clean).

## CLI options

| Option | Description | Default |
|---|---|---|
| `-n, --namespace` | Namespace to monitor (required unless `-A`) | |
| `-A, --all-namespaces` | Monitor all namespaces (required unless `-n`) | |
| `--interval` | Refresh interval in seconds | `2` |
| `--kubeconfig` | Path to a kubeconfig file | `~/.kube/config` |
| `--context` | Kubeconfig context to use | default context |
| `--sort` | Sort pods: `name`, `cpu`, `mem`, `restarts` | `name` |
| `--label-selector` | Label selector, e.g. `app=web` | |
| `--export` | Export one snapshot: `json` or `csv` (no live view) | |
| `--cpu-threshold` | CPU warning threshold % (1-100) | `90` |
| `--mem-threshold` | Memory warning threshold % (1-100) | `90` |
| `--deployments` | Also watch deployments | |
| `--deployment-sort` | Sort deployments: `name`, `ready`, `age` | `name` |
| `--hpas` | Also watch Horizontal Pod Autoscalers | |
| `--hpa-sort` | Sort HPAs: `name`, `desired`, `current`, `age` | `name` |
| `--log-file` | Log file for live-view diagnostics | `k8m.log` |
| `--debug` | Enable debug-level logging | |
| `--version` | Show version and exit | |

## Health flags

| Flag | Meaning |
|---|---|
| `Healthy` | All containers ready, no critical reasons, below thresholds |
| `WARNING (not ready)` | Running pod with fewer ready containers than expected |
| `WARNING (N restarts)` | 5+ total restarts |
| `WARNING (near limit)` | CPU or memory usage >= threshold (default 90%) |
| `CRITICAL (reason)` | CrashLoopBackOff, OOMKilled, ImagePullBackOff, ErrImagePull, Failed, Unknown, ... |
| `PENDING` | Pod not yet scheduled |
| `COMPLETED` | Pod succeeded (Job) |

## Troubleshooting

- **CPU/MEM columns show `n/a`** – `metrics-server` is not available. It is required for usage data; everything else still works.
- **Empty/error table** – check the API error shown in the footer and your kubeconfig (`kubectl get pods` should work first).
- **Screen "shaking" or log noise** – while the live view is active, nothing is written to stdout/stderr; diagnostics go to `--log-file k8m.log` only.

## Author

https://alexolinux.com

## License

MIT – see [LICENSE](LICENSE).
