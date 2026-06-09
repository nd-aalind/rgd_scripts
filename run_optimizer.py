#!/usr/bin/env python3
"""
SQL Optimizer Framework
-----------------------
Reads query.sql + promt.txt, calls Claude API, writes output to optimise.py.

Usage:
    python run_optimizer.py
    python run_optimizer.py --query my_query.sql --output my_script.py
"""

import argparse
import os
import sys
from pathlib import Path

import anthropic

# ── Paths (relative to this script) ──────────────────────────────────
SCRIPT_DIR   = Path(__file__).parent
QUERY_FILE   = SCRIPT_DIR / "query.sql"
PROMPT_FILE  = SCRIPT_DIR / "promt.txt"
OUTPUT_FILE  = SCRIPT_DIR / "optimise.py"

MODEL = "claude-opus-4-6"


def load_file(path: Path, label: str) -> str:
    if not path.exists():
        print(f"ERROR: {label} not found: {path}", file=sys.stderr)
        sys.exit(1)
    content = path.read_text(encoding="utf-8").strip()
    if not content:
        print(f"ERROR: {label} is empty: {path}", file=sys.stderr)
        sys.exit(1)
    return content


def extract_python(text: str) -> str:
    """
    If Claude wraps the script in a markdown code fence, strip it.
    Otherwise return the text as-is.
    """
    lines = text.splitlines()

    # Find opening fence (```python or ```)
    start = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("```python") or stripped == "```":
            start = i + 1
            break

    if start is None:
        return text  # No fence found — return raw

    # Find closing fence
    end = len(lines)
    for i in range(start, len(lines)):
        if lines[i].strip() == "```":
            end = i
            break

    return "\n".join(lines[start:end])


def main():
    parser = argparse.ArgumentParser(description="SQL → Optimized Python ETL generator")
    parser.add_argument("--query",  default=str(QUERY_FILE),  help="Path to SQL file")
    parser.add_argument("--prompt", default=str(PROMPT_FILE), help="Path to prompt/system file")
    parser.add_argument("--output", default=str(OUTPUT_FILE), help="Path to write Python output")
    args = parser.parse_args()

    query_path  = Path(args.query)
    prompt_path = Path(args.prompt)
    output_path = Path(args.output)

    # ── Load inputs ───────────────────────────────────────────────────
    system_prompt = load_file(prompt_path, "Prompt file")
    sql_query     = load_file(query_path,  "SQL query file")

    print(f"  Model   : {MODEL}")
    print(f"  Query   : {query_path}")
    print(f"  Prompt  : {prompt_path}")
    print(f"  Output  : {output_path}")
    print()

    # ── Call Claude API with streaming ────────────────────────────────
    client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from env

    print("Calling Claude API (streaming)...\n")
    print("-" * 60)

    full_response = []

    with client.messages.stream(
        model=MODEL,
        max_tokens=64000,
        thinking={"type": "adaptive"},
        system=system_prompt,
        messages=[
            {
                "role": "user",
                "content": (
                    "Here is the SQL query to optimize:\n\n"
                    f"```sql\n{sql_query}\n```\n\n"
                    "Please generate the optimized Python ETL script following the rules "
                    "in the system prompt. Output ONLY the Python script, nothing else."
                ),
            }
        ],
    ) as stream:
        for event in stream:
            if event.type == "content_block_delta":
                if event.delta.type == "text_delta":
                    chunk = event.delta.text
                    full_response.append(chunk)
                    print(chunk, end="", flush=True)

    print("\n" + "-" * 60)

    raw_output = "".join(full_response)
    python_code = extract_python(raw_output)

    # ── Write to output file ──────────────────────────────────────────
    output_path.write_text(python_code, encoding="utf-8")
    print(f"\nOutput written to: {output_path}")
    print(f"Lines: {len(python_code.splitlines())}")


if __name__ == "__main__":
    main()
