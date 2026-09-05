"""Export the trained model to ONNX and benchmark inference latency.

Shows the model can leave the notebook -> "deployable". Prints avg/p95 latency.

Example:
  python export_onnx.py --weights runs/roadwatch/yolov8s_rdd2022/weights/best.pt
"""
from __future__ import annotations

import argparse
import time

import numpy as np
from ultralytics import YOLO


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--weights", required=True)
    ap.add_argument("--imgsz", type=int, default=640)
    ap.add_argument("--runs", type=int, default=50)
    args = ap.parse_args()

    model = YOLO(args.weights)
    onnx_path = model.export(format="onnx", imgsz=args.imgsz, dynamic=True)
    print(f"[export] ONNX saved -> {onnx_path}")

    dummy = np.random.randint(0, 255, (args.imgsz, args.imgsz, 3), dtype=np.uint8)
    lat = []
    for _ in range(args.runs):
        t0 = time.perf_counter()
        model.predict(dummy, imgsz=args.imgsz, verbose=False)
        lat.append((time.perf_counter() - t0) * 1000.0)
    lat.sort()
    print(f"[bench] avg latency : {sum(lat)/len(lat):.1f} ms")
    print(f"[bench] p95 latency : {lat[int(0.95*len(lat))-1]:.1f} ms")


if __name__ == "__main__":
    main()
