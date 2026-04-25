"""
TelecomInsights — Real-time Network Event Simulator
Simulates network events sent to Azure Event Hub
Events: call_initiated, call_dropped, data_session_start,
        data_session_end, recharge_completed, sms_sent

Run: python simulate_stream.py
Requires: pip install azure-eventhub
"""

import json
import random
import time
import os
import csv
from datetime import datetime

random.seed(None)   # truly random for stream simulation

# ── Azure Event Hub config ─────────────────────────────────────────────
# Set these as environment variables — never hardcode!
EVENT_HUB_CONN_STR  = os.environ.get("EVENT_HUB_CONN_STR",  "")
EVENT_HUB_NAME      = os.environ.get("EVENT_HUB_NAME",       "telecom-events")
MESSAGES_PER_SECOND = 2         # adjust to simulate load
TOTAL_MESSAGES      = 500       # set to -1 for infinite stream

# ── Reference data ─────────────────────────────────────────────────────
EVENT_TYPES = [
    "call_initiated", "call_completed", "call_dropped",
    "data_session_start", "data_session_end",
    "sms_sent", "recharge_completed", "roaming_start"
]
EVENT_WEIGHTS = [0.20, 0.18, 0.03, 0.22, 0.20, 0.10, 0.05, 0.02]

NETWORK_TYPES = ["4G","5G","3G","WiFi Calling"]
REGIONS       = ["Doha","Al Rayyan","Al Wakrah","Al Khor","Mesaieed","Lusail"]
TOWERS        = [f"TOWER_{r}_{i:03d}" for r in ["DOH","RAY","WAK"] for i in range(1, 21)]


def load_customer_msisdns():
    """Load MSISDNs from generated customer file"""
    customer_file = "output/customers/customers.csv"
    msisdns = []
    if os.path.exists(customer_file):
        with open(customer_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["is_active"] == "1":
                    msisdns.append({
                        "customer_id": row["customer_id"],
                        "msisdn":      row["msisdn"],
                        "segment":     row["segment"],
                        "plan_name":   row["plan_name"],
                        "churn_risk":  row["churn_risk"],
                        "region":      row["region"],
                    })
    if not msisdns:
        # Fallback — generate synthetic MSISDNs
        for i in range(1, 101):
            prefix = random.choice(["3","5","6","7"])
            msisdns.append({
                "customer_id": f"CUST{i:05d}",
                "msisdn":      f"+974{prefix}{random.randint(1000000,9999999)}",
                "segment":     random.choice(["Low Value","Mid Value","High Value"]),
                "plan_name":   random.choice(["Basic 10","Smart 25","Value 50"]),
                "churn_risk":  random.choice(["Low","Medium","High"]),
                "region":      random.choice(REGIONS),
            })
    return msisdns


def generate_event(customer):
    """Generate a single realistic network event"""
    event_type   = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    network_type = random.choice(NETWORK_TYPES)
    tower_id     = random.choice(TOWERS)
    now          = datetime.utcnow()

    base = {
        "event_id":      f"EVT{now.strftime('%Y%m%d%H%M%S')}{random.randint(10000,99999)}",
        "event_type":    event_type,
        "customer_id":   customer["customer_id"],
        "msisdn":        customer["msisdn"],
        "event_time":    now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "network_type":  network_type,
        "tower_id":      tower_id,
        "region":        customer["region"],
        "segment":       customer["segment"],
        "plan_name":     customer["plan_name"],
        "churn_risk":    customer["churn_risk"],
    }

    # Add event-specific fields
    if event_type in ["call_initiated","call_completed","call_dropped"]:
        base["duration_seconds"] = random.randint(0, 600) if event_type != "call_initiated" else 0
        base["call_type"]        = random.choice(["Voice","Video Call","IDD Voice"])
        base["signal_strength"]  = random.randint(-120, -50)     # dBm
        base["data_mb"]          = 0.0

    elif event_type in ["data_session_start","data_session_end"]:
        base["data_mb"]          = round(random.uniform(0.5, 500), 2)
        base["duration_seconds"] = random.randint(30, 3600)
        base["call_type"]        = "Data"
        base["signal_strength"]  = random.randint(-110, -50)

    elif event_type == "sms_sent":
        base["data_mb"]          = 0.0
        base["duration_seconds"] = 0
        base["call_type"]        = "SMS"
        base["signal_strength"]  = random.randint(-115, -50)

    elif event_type == "recharge_completed":
        base["recharge_amount"]  = random.choice([10, 25, 50, 100])
        base["recharge_channel"] = random.choice(["Mobile App","USSD","Retailer"])
        base["data_mb"]          = 0.0
        base["duration_seconds"] = 0
        base["signal_strength"]  = 0

    elif event_type == "roaming_start":
        base["roaming_country"]  = random.choice(["UAE","Saudi Arabia","Kuwait"])
        base["data_mb"]          = 0.0
        base["duration_seconds"] = 0
        base["signal_strength"]  = random.randint(-120, -60)

    # Fill missing optional fields
    for field in ["duration_seconds","data_mb","call_type","signal_strength",
                  "recharge_amount","recharge_channel","roaming_country"]:
        if field not in base:
            base[field] = None

    return base


def send_to_event_hub(events_batch):
    """Send batch of events to Azure Event Hub"""
    try:
        from azure.eventhub import EventHubProducerClient, EventData
        client = EventHubProducerClient.from_connection_string(
            EVENT_HUB_CONN_STR, eventhub_name=EVENT_HUB_NAME
        )
        with client:
            batch = client.create_batch()
            for event in events_batch:
                batch.add(EventData(json.dumps(event)))
            client.send_batch(batch)
        return True
    except ImportError:
        print("  azure-eventhub not installed. Run: pip install azure-eventhub")
        return False
    except Exception as e:
        print(f"  Event Hub error: {e}")
        return False


def save_to_local_json(events_batch, output_file="output/stream_preview.jsonl"):
    """Save events to local JSONL file for testing without Event Hub"""
    os.makedirs("output", exist_ok=True)
    with open(output_file, "a", encoding="utf-8") as f:
        for event in events_batch:
            f.write(json.dumps(event) + "\n")


def simulate_stream():
    print("TelecomInsights — Network Event Stream Simulator")
    print("=" * 50)

    customers = load_customer_msisdns()
    print(f"Loaded {len(customers)} active customers")

    use_event_hub = bool(EVENT_HUB_CONN_STR)
    if use_event_hub:
        print(f"Mode: Sending to Azure Event Hub: {EVENT_HUB_NAME}")
    else:
        print("Mode: LOCAL (set EVENT_HUB_CONN_STR env var to send to Event Hub)")
        print("      Saving to output/stream_preview.jsonl")

    print(f"Rate: {MESSAGES_PER_SECOND} messages/second")
    print(f"Total: {TOTAL_MESSAGES if TOTAL_MESSAGES > 0 else 'infinite'} messages")
    print("Press Ctrl+C to stop\n")

    sent       = 0
    batch_size = max(1, MESSAGES_PER_SECOND)

    try:
        while TOTAL_MESSAGES == -1 or sent < TOTAL_MESSAGES:
            batch = []
            for _ in range(batch_size):
                customer = random.choice(customers)
                event    = generate_event(customer)
                batch.append(event)

            if use_event_hub:
                success = send_to_event_hub(batch)
                status  = "sent" if success else "FAILED"
            else:
                save_to_local_json(batch)
                status = "saved locally"

            sent += len(batch)
            sample = batch[0]
            print(f"  [{sent:5d}] {status} | "
                  f"{sample['event_type']:<22} | "
                  f"customer={sample['customer_id']} | "
                  f"region={sample['region']}")

            time.sleep(1.0 / max(1, MESSAGES_PER_SECOND))

    except KeyboardInterrupt:
        print(f"\nStopped. Total events sent/saved: {sent}")

    print(f"\nStream simulation complete: {sent} events")
    if not use_event_hub:
        print("Preview saved to: output/stream_preview.jsonl")
        print("\nTo send to Event Hub, set environment variable:")
        print("  Windows: set EVENT_HUB_CONN_STR=your_connection_string_here")
        print("  Linux/Mac: export EVENT_HUB_CONN_STR=your_connection_string_here")
        print("Then run: python simulate_stream.py")


if __name__ == "__main__":
    simulate_stream()
