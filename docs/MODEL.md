# Computer Vision Model

RoadWatch uses a **YOLOv8s object detector** as the inference stage inside the Kafka streaming pipeline. The model is not treated as a standalone notebook artifact: the deployed checkpoint is loaded by the always-on `yolo-consumer` service and its detections become versioned Kafka events consumed by independent downstream services.

## Model configuration

- Architecture: YOLOv8s
- Input resolution: 640 × 640
- Training subset: 15,000 RDD2022 images
- Validation subset: 3,000 images
- Classes:
  - Longitudinal crack
  - Transverse crack
  - Alligator crack
  - Pothole
- Default deployment confidence threshold: `0.25`
- Deployed checkpoint: [`model/weights/best.pt`](../model/weights/best.pt)

## Training pipeline

```text
RDD2022 images + labels
        │
        ▼
prepare_subset.py
        │
        ▼
YOLO-format train / val dataset
        │
        ▼
train.py / RoadWatch_Model_Training.ipynb
        │
        ▼
YOLOv8s checkpoint
        │
        ├──► evaluate.py ──► metrics / PR / confusion matrix
        │
        └──► yolo_consumer.py ──► Kafka detection events
```

The complete notebook is preserved at [`notebooks/RoadWatch_Model_Training.ipynb`](../notebooks/RoadWatch_Model_Training.ipynb).

## Deployment integration

`streaming/yolo_consumer.py`:

1. consumes a versioned `raw_frames` Kafka event;
2. base64-decodes the image;
3. runs YOLOv8s inference using `best.pt`;
4. extracts class, confidence and bounding box;
5. computes bounding-box area ratio and severity;
6. creates a deterministic detection event ID;
7. publishes the result to `road_damage_events`;
8. commits the input offset only after all detection events are acknowledged.

This model/event boundary allows inference to scale independently from persistence and analytics.

## Validation metrics

| Metric | YOLOv8n baseline | YOLOv8s |
|---|---:|---:|
| Precision | 0.448 | **0.582** |
| Recall | 0.377 | **0.513** |
| mAP@50 | 0.358 | **0.532** |
| mAP@50–95 | 0.150 | **0.259** |

Per-class mAP@50:

| Damage type | mAP@50 |
|---|---:|
| Alligator | **0.638** |
| Longitudinal | **0.544** |
| Transverse | **0.535** |
| Pothole | **0.411** |

See [RESULTS.md](RESULTS.md) for plots and evaluation details.

## Training

```bash
python model/prepare_subset.py \
  --train_images <RDD>/train/images \
  --val_images <RDD>/val/images \
  --out_dir data/rdd2022_large \
  --max_train 15000 \
  --max_val 3000

python model/train.py \
  --data data/rdd2022_large/data.yaml \
  --model yolov8s.pt \
  --epochs 60 \
  --imgsz 640
```

## Evaluation

```bash
python model/evaluate.py \
  --weights model/weights/best.pt \
  --data data/rdd2022_large/data.yaml
```

## Export

`model/export_onnx.py` provides an ONNX export path for deployments that use an optimized inference runtime rather than the native Ultralytics/PyTorch path.
