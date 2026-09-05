# Dataset Notes

RoadWatch uses imagery from the **Road Damage Dataset 2022 (RDD2022)** for detector training, validation and local streaming replay.

## Repository data

The repository contains a small subset of road images under `data/frames/` so the end-to-end Kafka/inference pipeline can run immediately without committing the full training dataset.

The complete RDD2022 dataset is intentionally not stored in this repository.

## Model classes

RoadWatch maps the detector output to four classes:

1. Longitudinal crack
2. Transverse crack
3. Alligator crack
4. Pothole

The class order is fixed in `streaming/contracts.py` and model evaluation code so training and streaming inference use a consistent interpretation.

## Reproducing the training subset

After downloading RDD2022 separately:

```bash
python model/prepare_subset.py \
  --train_images <RDD>/train/images \
  --val_images <RDD>/val/images \
  --out_dir data/rdd2022_large \
  --max_train 15000 \
  --max_val 3000
```

Review the dataset's original terms before redistribution or commercial use.
