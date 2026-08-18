# K8m

-----

Real-time terminal monitor for Kubernetes pods, deployments, HPAs with CPU/Memory usage, health flags, and export.

## Usage
```bash
pip install -e .
k8m -n default
k8m -A --label-selector app=web --sort cpu
k8m -n default --export json
```

## Requirements
metrics-server for usage columns.

