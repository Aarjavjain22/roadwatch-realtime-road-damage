"""Train YOLOv8s on the large RDD2022 subset, with optional W&B tracking.

Example:
  python train.py --data data/rdd2022_large/data.yaml --model yolov8s.pt --epochs 60 --imgsz 640
"""
from __future__ import annotations

import argparse

from ultralytics import YOLO


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--model", default="yolov8s.pt")
    ap.add_argument("--epochs", type=int, default=60)
    ap.add_argument("--imgsz", type=int, default=640)
    ap.add_argument("--batch", type=int, default=-1, help="-1 = auto-fit GPU memory")
    ap.add_argument("--project", default="runs/roadwatch")
    ap.add_argument("--name", default="yolov8s_rdd2022")
    args = ap.parse_args()

    # Optional: enable Weights & Biases experiment tracking if installed + logged in
    try:
        import wandb  # noqa: F401
        from ultralytics import settings
        settings.update({"wandb": True})
        print("[train] W&B tracking enabled")
    except Exception:
        print("[train] W&B not active (pip install wandb && wandb login to enable)")

    model = YOLO(args.model)
    model.train(
        data=args.data,
        epochs=args.epochs,
        imgsz=args.imgsz,
        batch=args.batch,
        workers=4,
        patience=15,          # early stopping
        project=args.project,
        name=args.name,
        exist_ok=True,
    )
    print(f"[train] done -> {args.project}/{args.name}/weights/best.pt")


if __name__ == "__main__":
    main()
