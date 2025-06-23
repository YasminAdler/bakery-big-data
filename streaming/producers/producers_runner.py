import os
import pandas as pd
from feedback_producer import FeedbackProducer
from inventory_producer import InventoryProducer
from iot_producer import IoTProducer
from pos_producer import POSProducer

OUTPUT_DIR = "bronze_simulated_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def label_and_flatten(events, label):
    for e in events:
        e["source"] = label
    return pd.json_normalize(events)

def main():
    print("🎬 Running all producers...")

    feedback = FeedbackProducer()
    inventory = InventoryProducer()
    iot = IoTProducer()
    pos = POSProducer()

    print("📦 Generating feedback...")
    feedback_data = [feedback.generate_feedback_event() for _ in range(2500)]

    print("📦 Generating inventory...")
    inventory_data = [inventory.generate_inventory_update() for _ in range(2500)]

    print("📦 Generating IoT...")
    iot_data = []
    while len(iot_data) < 2500:
        for eq in iot.equipment:
            iot_data.append(iot.generate_sensor_reading(eq))
            if len(iot_data) >= 2500:
                break

    print("📦 Generating POS...")
    pos_data = [pos.generate_sale_event() for _ in range(2500)]

    print("🧾 Flattening and combining data...")
    combined_df = pd.concat([
        label_and_flatten(feedback_data, "feedback"),
        label_and_flatten(inventory_data, "inventory"),
        label_and_flatten(iot_data, "iot"),
        label_and_flatten(pos_data, "pos")
    ])

    csv_path = os.path.join(OUTPUT_DIR, "bronze_combined.csv")
    # Fill nulls based on dtype
    for col in combined_df.columns:
        if combined_df[col].dtype == "float64" or combined_df[col].dtype == "int64":
            combined_df[col] = combined_df[col].fillna(0)
        elif combined_df[col].dtype == "bool":
            combined_df[col] = combined_df[col].fillna(False)
        else:
            combined_df[col] = combined_df[col].fillna("null")

    # Save to CSV
    combined_df.to_csv(csv_path, index=False)
    print(f"✅ CSV saved with missing fields filled → {csv_path}")
    print(f"✅ Done! Combined CSV saved to {csv_path} with {len(combined_df)} rows")

if __name__ == "__main__":
    main()
