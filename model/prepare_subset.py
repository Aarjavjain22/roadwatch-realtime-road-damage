"""Build a LARGER YOLO-format subset from the full RDD2022 download.

Bump --max_train up (e.g. 12000-20000) to satisfy the "big data" requirement
while still finishing overnight on a single GPU.
"""
from __future__ import annotations

import argparse
import random
import shutil
from pathlib import Path

import yaml

IMAGE_EXTS = {".jpg", ".jpeg", ".png"}


def image_to_label(img: Path) -> Path:
    parts = list(img.parts)
    if "images" in parts:
        parts[parts.index("images")] = "labels"
        return Path(*parts).with_suffix(".txt")
    return img.with_suffix(".txt")


def collect(images_dir: Path):
    pairs = []
    for img in images_dir.rglob("*"):
        if img.suffix.lower() in IMAGE_EXTS:
            lbl = image_to_label(img)
            if lbl.exists():
                pairs.append((img, lbl))
    return pairs


def copy_subset(pairs, out_root: Path, split: str, limit: int):
    random.seed(42)
    random.shuffle(pairs)
    pairs = pairs[:limit] if limit else pairs
    (out_root / "images" / split).mkdir(parents=True, exist_ok=True)
    (out_root / "labels" / split).mkdir(parents=True, exist_ok=True)
    for img, lbl in pairs:
        shutil.copy2(img, out_root / "images" / split / img.name)
        shutil.copy2(lbl, out_root / "labels" / split / lbl.name)
    print(f"[{split}] copied {len(pairs)} pairs")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--train_images", required=True, help="RDD2022 train images dir")
    ap.add_argument("--val_images", required=True, help="RDD2022 val images dir")
    ap.add_argument("--out_dir", default="data/rdd2022_large")
    ap.add_argument("--max_train", type=int, default=15000)
    ap.add_argument("--max_val", type=int, default=3000)
    args = ap.parse_args()

    out = Path(args.out_dir).resolve()
    copy_subset(collect(Path(args.train_images)), out, "train", args.max_train)
    copy_subset(collect(Path(args.val_images)), out, "val", args.max_val)

    data = {
        "path": str(out),
        "train": "images/train",
        "val": "images/val",
        "names": ["Longitudinal", "Transverse", "Alligator", "Pothole"],
    }
    (out / "data.yaml").write_text(yaml.safe_dump(data, sort_keys=False))
    print("data.yaml written at", out / "data.yaml")


if __name__ == "__main__":
    main()
