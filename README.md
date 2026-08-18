# K8Mon

Real-time terminal monitor for Kubernetes pods with CPU/Memory usage, health flags, and export.

## Usage
```bash
pip install -e .
k8mon -n default
k8mon -A --label-selector app=web --sort cpu
k8mon -n default --export json
```

## Requirements
metrics-server for usage columns.
