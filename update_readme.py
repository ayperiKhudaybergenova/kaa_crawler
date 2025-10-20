import os
import json
from datetime import datetime
from huggingface_hub import HfApi

HF_DATASET = "Ayperi/kaa_sentences"
STATS_FILE = "dataset_stats.json"


def get_hf_stats(token):
    api = HfApi()
    info = api.dataset_info(HF_DATASET, token=token)

    # Try to read number of examples (if provided in the card)
    num_sentences = info.cardData.get("num_examples") if info.cardData else None

    # 'size' attribute no longer exists — sum over files if possible
    total_bytes = 0
    if hasattr(info, "siblings"):
        for f in info.siblings:
            if hasattr(f, "size") and f.size:
                total_bytes += f.size

    # Return dictionary of stats
    return {
        "sentence_count": int(num_sentences or 0),
        "token_count": int((num_sentences or 0) * 10),  # rough estimate
        "size_bytes": int(total_bytes),
    }


def readable_size(num_bytes):
    for unit in ["B", "KB", "MB", "GB"]:
        if num_bytes < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f} TB"


def compute_diff(current, previous):
    def fmt_diff(key):
        diff = current[key] - previous.get(key, 0)
        return f"+{diff}" if diff > 0 else "0"

    return {
        "sentence_diff": fmt_diff("sentence_count"),
        "token_diff": fmt_diff("token_count"),
        "size_diff": fmt_diff("size_bytes"),
    }


def update_readme():
    hf_token = os.environ["HF_TOKEN"]

    # Get new stats
    current = get_hf_stats(hf_token)

    # Load previous stats if exist
    if os.path.exists(STATS_FILE):
        with open(STATS_FILE, "r") as f:
            previous = json.load(f)
    else:
        previous = {}

    diffs = compute_diff(current, previous)
    current["size_readable"] = readable_size(current["size_bytes"])

    # Save for next run
    with open(STATS_FILE, "w") as f:
        json.dump(current, f, indent=2)

    # Update README
    with open("README.md", "r", encoding="utf-8") as f:
        readme = f.read()

    updated = (
        readme.replace("{{ last_updated }}", datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))
        .replace("{{ sentence_count }}", str(current["sentence_count"]))
        .replace("{{ token_count }}", str(current["token_count"]))
        .replace("{{ size_readable }}", current["size_readable"])
        .replace("{{ sentence_diff }}", diffs["sentence_diff"])
        .replace("{{ token_diff }}", diffs["token_diff"])
        .replace("{{ size_diff }}", diffs["size_diff"])
    )

    with open("README.md", "w", encoding="utf-8") as f:
        f.write(updated)

    print(
        f"✅ Updated README — {current['sentence_count']} sentences, "
        f"{current['token_count']} tokens, {current['size_readable']} total"
    )


if __name__ == "__main__":
    update_readme()
