import os
import base64
import time
import json
import requests
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


# API Configuration - Primary server only
API_URL = "http://10.10.1.5:6969/v1/chat/completions"
MODEL = "Qwen/Qwen3-VL-8B-Instruct"
FOLDER = "data/properties/"

# TUNE THESE FOR MAX SPEED (REDUCED TO AVOID VLLM CACHE ISSUES):
BATCH_SIZE = 6        
CONCURRENCY = 6
REQUEST_DELAY = 0.05  # Small delay to prevent cache corruption

# Thread-local storage for HTTP sessions
thread_local = threading.local()

def get_session():
    """Get or create a session for the current thread"""
    if not hasattr(thread_local, 'session'):
        thread_local.session = requests.Session()
        thread_local.session.headers.update({'Connection': 'keep-alive'})
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=1
        )
        thread_local.session.mount('http://', adapter)
        thread_local.session.mount('https://', adapter)
    return thread_local.session        

prompt = (
    "You are an expert web screenshot quality analyst. Your task is to decide whether each screenshot shows a fully "
    "loaded, accessible page or a failure state and report the result in strict JSON.\\n\\n"
    "Respond ONLY with JSON matching this schema:\\n"
    "{\\n"
    '  "is_valid": "Yes" or "No",\\n'
    '  "classification": "valid" or one of ["captcha","error","timeout","rate_limit","under_construction","api_endpoint","low_quality"],\\n'
    '  "confidence": number between 0.0 and 1.0,\\n'
    '  "reason": "brief explanation (max 100 characters)"\\n'
    "}\\n\\n"
    'CRITICAL: The "classification" field MUST be EXACTLY one of these values:\\n'
    '- "valid" (working page)\\n'
    '- "captcha" (CAPTCHA challenge)\\n'
    '- "error" (HTTP errors, SSL errors, browser errors, blocking messages, 404, 403, 500, etc.)\\n'
    '- "timeout" (partially loaded/timeout)\\n'
    '- "rate_limit" (rate limiting)\\n'
    '- "under_construction" (maintenance/construction)\\n'
    '- "api_endpoint" (raw API/JSON response)\\n'
    '- "low_quality" (very low quality capture)\\n\\n'
    'DO NOT invent new classification values. Use "error" for all types of errors including SSL, blocking, HTTP codes.'
)

ALLOWED_CLASSES = {
    "valid",
    "captcha",
    "error",
    "timeout",
    "rate_limit",
    "under_construction",
    "api_endpoint",
    "low_quality",
}

def encode(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()

def make_request(batch_files):
    images = []
    for p in batch_files:
        images.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{encode(p)}"}
        })

    # final text query instructing array return
    images.append({
        "type": "text",
        "text": (
            "Classify each screenshot and respond ONLY with a JSON object of the form "
            '{"results": [ {...}, {...} ]} where each entry matches the schema in the prompt and '
            "appears in the same order as the provided images."
        ),
    })

    return {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": images}
        ],
        "max_tokens": 512,
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }

def process_batch(batch_files):
    payload = make_request(batch_files)
    session = get_session()
    
    # Add small delay to avoid overwhelming vLLM cache
    time.sleep(REQUEST_DELAY)
    
    t0 = time.time()
    try:
        resp = session.post(API_URL, json=payload, timeout=120)
        dt = time.time() - t0
        resp.raise_for_status()
        payload_json = resp.json()
        if "error" in payload_json:
            print(f"API Error: {payload_json['error']}")
            return [_default_result()] * len(batch_files), dt
    except Exception as e:
        print(f"Request failed: {e}")
        return [_default_result()] * len(batch_files), 0.1

    try:
        raw_text = payload_json["choices"][0]["message"]["content"].strip()
        parsed = json.loads(raw_text)
        items = parsed.get("results", [])
    except Exception as e:
        print(f"Parsing failed: {e}")
        items = []

    normalized = []
    for idx in range(len(batch_files)):
        item = items[idx] if idx < len(items) else {}
        normalized.append(_normalize_result(item))

    return normalized, dt


def _default_result():
    return {
        "is_valid": "No",
        "classification": "error",
        "confidence": 0.0,
        "reason": "invalid response",
    }


def _normalize_result(item):
    result = _default_result().copy()

    if isinstance(item, dict):
        if str(item.get("is_valid", "No")).strip().lower() in {"yes", "no"}:
            result["is_valid"] = str(item["is_valid"]).strip().title()

        classification = str(item.get("classification", "error")).strip().lower()
        if classification in ALLOWED_CLASSES:
            result["classification"] = classification

        try:
            confidence = float(item.get("confidence", 0.0))
        except (TypeError, ValueError):
            confidence = 0.0
        if 0.0 <= confidence <= 1.0:
            result["confidence"] = confidence

        reason = str(item.get("reason", "invalid response")).strip()
        result["reason"] = reason[:100] if reason else "invalid response"

    return result

def main():
    files = [
        os.path.join(FOLDER, f)
        for f in os.listdir(FOLDER)
        if f.lower().endswith((".png", ".jpg", ".jpeg", ".webp"))
    ]
    total_images = len(files)
    print(f"Found {total_images} images.")

    # make batches
    batches = [
        files[i:i+BATCH_SIZE]
        for i in range(0, len(files), BATCH_SIZE)
    ]

    results = []
    start = time.time()

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        fut_map = {ex.submit(process_batch, batch): batch for batch in batches}

        for fut in as_completed(fut_map):
            batch_files = fut_map[fut]
            results_batch, dt = fut.result()
            print(f"Batch of {len(batch_files)} processed in {dt:.2f}s")

            for f, result in zip(batch_files, results_batch, strict=False):
                print(
                    f"  -> {os.path.basename(f)} | is_valid={result['is_valid']} "
                    f"class={result['classification']} conf={result['confidence']:.2f}"
                )
                results.append(
                    (
                        os.path.basename(f),
                        result["is_valid"],
                        result["classification"],
                        f"{result['confidence']:.2f}",
                        result["reason"],
                    )
                )

    elapsed = time.time() - start
    ips = total_images / elapsed

    with open("results.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["filename", "is_valid", "classification", "confidence", "reason"])
        w.writerows(results)

    print("\n===== MAX-SPEED SUMMARY =====")
    print(f"Total images:          {total_images}")
    print(f"Total time:            {elapsed:.2f} s")
    print(f"Images per second:     {ips:.2f} IPS")
    print(f"Batch size:            {BATCH_SIZE}")
    print(f"Parallel requests:     {CONCURRENCY}")
    class_counts = {}
    for _, _, classification, _, _ in results:
        class_counts[classification] = class_counts.get(classification, 0) + 1
    print("Class distribution:")
    for cls, count in sorted(class_counts.items(), key=lambda x: x[0]):
        print(f" - {cls}: {count}")
    print("==============================")

if __name__ == "__main__":
    main()
