"""Static, dependency-light validation for the lab's Kubernetes/GitOps contract."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "deploy" / "kubernetes" / "base"


def documents(path: Path) -> list[dict[str, Any]]:
    return [doc for doc in yaml.safe_load_all(path.read_text(encoding="utf-8")) if doc]


def validate() -> list[str]:
    errors: list[str] = []
    resources = [doc for path in BASE.glob("*.yaml") for doc in documents(path)]
    by_kind = {doc.get("kind") for doc in resources}
    required = {
        "Deployment", "Service", "ServiceAccount", "ConfigMap", "HorizontalPodAutoscaler",
        "PodDisruptionBudget", "NetworkPolicy", "Gateway", "HTTPRoute",
    }
    if missing := sorted(required - by_kind):
        errors.append(f"missing Kubernetes kinds: {', '.join(missing)}")

    for resource in resources:
        kind = resource.get("kind", "unknown")
        api = str(resource.get("apiVersion", ""))
        if not api or (
            kind != "Kustomization" and not resource.get("metadata", {}).get("name")
        ):
            errors.append(f"{kind}: apiVersion and metadata.name are required")
        if kind == "Deployment":
            pod = resource["spec"]["template"]["spec"]
            if not pod.get("securityContext", {}).get("runAsNonRoot"):
                errors.append("Deployment must run as non-root")
            for container in pod.get("containers", []):
                if container.get("image", "").endswith(":latest"):
                    errors.append("container image must not use :latest")
                for field in ("readinessProbe", "livenessProbe", "resources", "securityContext"):
                    if field not in container:
                        errors.append(f"Deployment container missing {field}")
        if kind in {"Gateway", "HTTPRoute"} and api != "gateway.networking.k8s.io/v1":
            errors.append(f"{kind} must use stable Gateway API v1")

    app = documents(ROOT / "gitops" / "application.yaml")[0]
    revision = str(app["spec"]["source"]["targetRevision"])
    if revision in {"HEAD", "main", "master"}:
        errors.append("Argo CD targetRevision must be a pinned release")
    return errors


if __name__ == "__main__":
    failures = validate()
    if failures:
        raise SystemExit("\n".join(f"ERROR: {failure}" for failure in failures))
    print("Kubernetes and GitOps manifest contracts passed")
