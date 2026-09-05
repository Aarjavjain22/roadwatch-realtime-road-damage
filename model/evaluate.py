"""Evaluate a trained model: overall + per-class mAP, and save plots.

Produces the numbers/plots for the report:
  - precision, recall, mAP50, mAP50-95 (overall + per class)
  - confusion matrix + PR curves (saved by Ultralytics under the run dir)

Example:
  python evaluate.py --weights runs/roadwatch/yolov8s_rdd2022/weights/best.pt \
                     --data data/rdd2022_large/data.yaml
"""
from __future__ import annotations

import argparse

from ultralytics import YOLO

CLASS_NAMES = ["Longitudinal", "Transverse", "Alligator", "Pothole"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--weights", required=True)
    ap.add_argument("--data", required=True)
    ap.add_argument("--imgsz", type=int, default=640)
    ap.add_argument("--split", default="val")
    args = ap.parse_args()

    model = YOLO(args.weights)
    m = model.val(data=args.data, imgsz=args.imgsz, split=args.split,
                  plots=True, save_json=True)

    print("\n================ OVERALL ================")
    print(f"Precision   : {m.box.mp:.4f}")
    print(f"Recall      : {m.box.mr:.4f}")
    print(f"mAP@50      : {m.box.map50:.4f}")
    print(f"mAP@50-95   : {m.box.map:.4f}")

    print("\n================ PER-CLASS mAP@50 ================")
    for i, name in enumerate(CLASS_NAMES):
        try:
            ap50 = m.box.ap50[i]
            print(f"{name:14s}: {ap50:.4f}")
        except Exception:
            pass
    print("\nPlots (confusion matrix, PR curves) saved under the run's val dir.")


if __name__ == "__main__":
    main()
