from __future__ import annotations

import ast
import hashlib
import json
import math
import re
import sqlite3
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

TOOL_SCHEMAS = {
    "csv_sql": {
        "type": "object",
        "properties": {"sql": {"type": "string"}},
        "required": ["sql"],
    },
    "policy_retriever": {
        "type": "object",
        "properties": {
            "url": {"type": "string", "format": "uri"},
            "query": {"type": "string"},
            "k": {"type": "integer", "minimum": 1, "default": 3},
        },
        "required": ["url", "query"],
    },
    "calculator": {
        "type": "object",
        "properties": {
            "expression": {"type": "string", "pattern": r"^[0-9+\\-*/().\\s]+$"},
            "units": {"type": "string"},
        },
        "required": ["expression"],
    },
}

ALLOWED_CALC_PATTERN = re.compile(r"^[0-9+\-*/().\s]+$")
READ_ONLY_SQL = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)


@dataclass
class ToolLog:
    step: int
    tool: str
    args_hash: str
    latency_ms: float
    success: bool
    stop_reason: str


@dataclass
class Policy:
    month_fee: float
    single_unlock: float
    single_classic_per_min: float
    single_ebike_per_min: float
    member_included_min: float
    member_classic_overage_per_min: float
    member_ebike_per_min: float
    member_ebike_after_per_min: float
    captured_at: str
    pricing_url: str


class SafeCalculator(ast.NodeVisitor):
    allowed_binary = {
        ast.Add: lambda a, b: a + b,
        ast.Sub: lambda a, b: a - b,
        ast.Mult: lambda a, b: a * b,
        ast.Div: lambda a, b: a / b,
    }
    allowed_unary = {
        ast.UAdd: lambda a: +a,
        ast.USub: lambda a: -a,
    }

    def visit_Expression(self, node: ast.Expression) -> float:
        return self.visit(node.body)

    def visit_BinOp(self, node: ast.BinOp) -> float:
        op_type = type(node.op)
        if op_type not in self.allowed_binary:
            raise ValueError("Unsupported operator")
        return self.allowed_binary[op_type](self.visit(node.left), self.visit(node.right))

    def visit_UnaryOp(self, node: ast.UnaryOp) -> float:
        op_type = type(node.op)
        if op_type not in self.allowed_unary:
            raise ValueError("Unsupported unary operator")
        return self.allowed_unary[op_type](self.visit(node.operand))

    def visit_Constant(self, node: ast.Constant) -> float:
        if not isinstance(node.value, (int, float)):
            raise ValueError("Only numbers are allowed")
        return float(node.value)

    def generic_visit(self, node: ast.AST) -> float:
        raise ValueError("Unsupported expression")



def now_ts() -> str:
    return datetime.now(UTC).isoformat()



def make_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:12]



def tool_result(*, success: bool, data: Any | None = None, error: str | None = None, source: str | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {"success": success, "ts": now_ts()}
    if data is not None:
        result["data"] = data
    if error is not None:
        result["error"] = error
    if source is not None:
        result["source"] = source
    return result



def fetch_text_lines(url: str) -> list[str]:
    response = requests.get(
        url,
        timeout=25,
        headers={"User-Agent": "Mozilla/5.0 Homework7Agent/1.0"},
    )
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    lines = []
    for raw in soup.get_text("\n").splitlines():
        cleaned = re.sub(r"\s+", " ", raw).strip()
        if len(cleaned) >= 2:
            lines.append(cleaned)
    return lines



def policy_retriever(url: str, query: str, k: int = 3) -> dict[str, Any]:
    try:
        lines = fetch_text_lines(url)
        chunks: list[str] = []
        for start in range(0, len(lines)):
            chunk = " ".join(lines[start : start + 8]).strip()
            if len(chunk) >= 60:
                chunks.append(chunk)

        query_terms = {term.lower() for term in re.findall(r"[a-zA-Z0-9$./]+", query) if len(term) > 2}
        scored: list[tuple[float, str]] = []
        for chunk in chunks:
            lower_chunk = chunk.lower()
            score = sum(1 for term in query_terms if term in lower_chunk)
            if "$" in chunk:
                score += 0.25
            if score > 0:
                scored.append((score, chunk))

        scored.sort(key=lambda item: item[0], reverse=True)
        passages = []
        seen = set()
        for score, chunk in scored:
            if chunk in seen:
                continue
            seen.add(chunk)
            passages.append({"text": chunk, "source": url, "score": round(score, 2)})
            if len(passages) >= k:
                break

        return tool_result(success=True, data={"passages": passages}, source=url)
    except Exception as exc:  # pragma: no cover - UI app fallback path
        return tool_result(success=False, error=str(exc), source=url)



def calculator(expression: str, units: str | None = None) -> dict[str, Any]:
    try:
        if not ALLOWED_CALC_PATTERN.fullmatch(expression):
            raise ValueError("Expression failed whitelist validation")
        tree = ast.parse(expression, mode="eval")
        value = SafeCalculator().visit(tree)
        payload: dict[str, Any] = {"value": round(float(value), 2)}
        if units:
            payload["units"] = units
        return tool_result(success=True, data=payload)
    except Exception as exc:
        return tool_result(success=False, error=str(exc))



def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    frame.columns = [str(column).strip().lower().replace(" ", "_") for column in frame.columns]

    start_col = next(
        (
            column
            for column in ["started_at", "start_time", "start_time_and_date"]
            if column in frame.columns
        ),
        None,
    )
    end_col = next(
        (
            column
            for column in ["ended_at", "end_time", "end_time_and_date"]
            if column in frame.columns
        ),
        None,
    )
    duration_col = next(
        (
            column
            for column in ["duration_sec", "trip_duration", "trip_duration_(seconds)"]
            if column in frame.columns
        ),
        None,
    )
    ride_type_col = next(
        (
            column
            for column in ["rideable_type", "bike_type"]
            if column in frame.columns
        ),
        None,
    )

    if start_col:
        frame["started_at"] = pd.to_datetime(frame[start_col], errors="coerce")
    else:
        frame["started_at"] = pd.NaT

    if end_col:
        frame["ended_at"] = pd.to_datetime(frame[end_col], errors="coerce")
    else:
        frame["ended_at"] = pd.NaT

    if duration_col:
        frame["duration_min"] = pd.to_numeric(frame[duration_col], errors="coerce") / 60.0
    else:
        frame["duration_min"] = (
            (frame["ended_at"] - frame["started_at"]).dt.total_seconds() / 60.0
        )

    frame["duration_min"] = frame["duration_min"].fillna(0).clip(lower=0)

    if ride_type_col:
        ride_text = frame[ride_type_col].astype(str).str.lower()
        frame["ride_type"] = ride_text
    else:
        frame["ride_type"] = "classic"

    frame["is_ebike"] = frame["ride_type"].str.contains("electric|ebike|e-bike", regex=True).astype(int)

    start_station_col = next(
        (
            column
            for column in ["start_station_name", "from_station_name"]
            if column in frame.columns
        ),
        None,
    )
    end_station_col = next(
        (
            column
            for column in ["end_station_name", "to_station_name"]
            if column in frame.columns
        ),
        None,
    )

    frame["start_station_name"] = frame[start_station_col] if start_station_col else "Unknown"
    frame["end_station_name"] = frame[end_station_col] if end_station_col else "Unknown"
    frame["week_start"] = frame["started_at"].dt.to_period("W-MON").dt.start_time.astype(str)
    frame["weekday"] = frame["started_at"].dt.day_name().fillna("Unknown")
    frame["start_hour"] = frame["started_at"].dt.hour.fillna(-1).astype(int)
    frame = frame.dropna(subset=["duration_min"]).reset_index(drop=True)
    return frame



def csv_sql(df: pd.DataFrame, sql: str) -> dict[str, Any]:
    try:
        if not READ_ONLY_SQL.match(sql):
            raise ValueError("Only read-only SELECT/WITH queries are allowed")
        connection = sqlite3.connect(":memory:")
        try:
            df.to_sql("trips", connection, index=False, if_exists="replace")
            result = pd.read_sql_query(sql, connection)
        finally:
            connection.close()
        return tool_result(
            success=True,
            data={
                "rows": result.to_dict(orient="records"),
                "row_count": int(len(result)),
                "source": "uploaded.csv",
            },
            source="uploaded.csv",
        )
    except Exception as exc:
        return tool_result(success=False, error=str(exc), source="uploaded.csv")



def parse_baywheels_policy(url: str) -> Policy:
    text = " ".join(fetch_text_lines(url))

    def extract(pattern: str, name: str, flags: int = re.IGNORECASE) -> re.Match[str]:
        match = re.search(pattern, text, flags)
        if not match:
            raise ValueError(f"Could not parse {name} from pricing page")
        return match

    month_fee = float(extract(r"Month pass\s*\$([0-9]+(?:\.[0-9]+)?)\/month", "month pass price").group(1))
    single_match = extract(
        r"Single rides.*?\$([0-9]+(?:\.[0-9]+)?) to unlock, then \$([0-9]+(?:\.[0-9]+)?)\/min after that for classic bikes\. Upgrade to an ebike for \$([0-9]+(?:\.[0-9]+)?)\/minute",
        "single ride pricing",
        flags=re.IGNORECASE | re.DOTALL,
    )
    month_match = extract(
        r"Month pass.*?\$([0-9]+(?:\.[0-9]+)?)\/month.*?45 min free, then \$([0-9]+(?:\.[0-9]+)?)\/min.*?Free unlocks \+ \$([0-9]+(?:\.[0-9]+)?)\/min for 45 min, then \$([0-9]+(?:\.[0-9]+)?)\/min",
        "month pass details",
        flags=re.IGNORECASE | re.DOTALL,
    )

    return Policy(
        month_fee=month_fee,
        single_unlock=float(single_match.group(1)),
        single_classic_per_min=float(single_match.group(2)),
        single_ebike_per_min=float(single_match.group(3)),
        member_included_min=45.0,
        member_classic_overage_per_min=float(month_match.group(2)),
        member_ebike_per_min=float(month_match.group(3)),
        member_ebike_after_per_min=float(month_match.group(4)),
        captured_at=now_ts(),
        pricing_url=url,
    )



def run_tool(
    *,
    step: int,
    tool_name: str,
    args: dict[str, Any],
    func,
    logs: list[ToolLog],
) -> dict[str, Any]:
    started = time.perf_counter()
    result = func(**args)
    latency_ms = round((time.perf_counter() - started) * 1000, 2)
    logs.append(
        ToolLog(
            step=step,
            tool=tool_name,
            args_hash=make_hash(args),
            latency_ms=latency_ms,
            success=result["success"],
            stop_reason="ok" if result["success"] else result.get("error", "failed"),
        )
    )
    return result



def compute_costs(df: pd.DataFrame, policy: Policy) -> tuple[pd.DataFrame, dict[str, float]]:
    rides = df.copy()
    minutes = rides["duration_min"].astype(float)
    ebike_mask = rides["is_ebike"] == 1

    rides["pay_per_use_cost"] = 0.0
    rides.loc[~ebike_mask, "pay_per_use_cost"] = policy.single_unlock + (
        minutes[~ebike_mask] * policy.single_classic_per_min
    )
    rides.loc[ebike_mask, "pay_per_use_cost"] = policy.single_unlock + (
        minutes[ebike_mask] * policy.single_ebike_per_min
    )

    classic_member = (minutes - policy.member_included_min).clip(lower=0) * policy.member_classic_overage_per_min
    ebike_member = (
        minutes.clip(upper=policy.member_included_min) * policy.member_ebike_per_min
        + (minutes - policy.member_included_min).clip(lower=0) * policy.member_ebike_after_per_min
    )
    rides["member_variable_cost"] = 0.0
    rides.loc[~ebike_mask, "member_variable_cost"] = classic_member[~ebike_mask]
    rides.loc[ebike_mask, "member_variable_cost"] = ebike_member[ebike_mask]

    metrics = {
        "pay_total": round(float(rides["pay_per_use_cost"].sum()), 2),
        "member_variable_total": round(float(rides["member_variable_cost"].sum()), 2),
        "member_total": round(float(rides["member_variable_cost"].sum() + policy.month_fee), 2),
        "ride_count": int(len(rides)),
        "avg_duration": round(float(minutes.mean()), 2) if len(rides) else 0.0,
        "ebike_share": round(float(rides["is_ebike"].mean() * 100), 2) if len(rides) else 0.0,
        "total_minutes": round(float(minutes.sum()), 2),
    }
    return rides, metrics



def build_weekly_table(rides: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        rides.groupby("week_start", dropna=False)
        .agg(
            ride_count=("duration_min", "size"),
            avg_duration_min=("duration_min", "mean"),
            ebike_share_pct=("is_ebike", lambda s: float(s.mean() * 100)),
            pay_per_use_spend=("pay_per_use_cost", "sum"),
            member_spend=("member_variable_cost", "sum"),
        )
        .reset_index()
    )
    numeric_cols = [
        "avg_duration_min",
        "ebike_share_pct",
        "pay_per_use_spend",
        "member_spend",
    ]
    grouped[numeric_cols] = grouped[numeric_cols].round(2)
    return grouped



def build_recommendation(metrics: dict[str, float], policy: Policy) -> tuple[str, float, float]:
    delta = round(metrics["pay_total"] - metrics["member_total"], 2)
    decision = "Buy Monthly Membership" if metrics["member_total"] < metrics["pay_total"] else "Pay Per Ride/Minute"
    avg_savings_per_ride = (metrics["pay_total"] - metrics["member_variable_total"]) / max(metrics["ride_count"], 1)
    if avg_savings_per_ride > 0:
        break_even_rides = round(policy.month_fee / avg_savings_per_ride, 2)
    else:
        break_even_rides = math.inf

    avg_savings_per_minute = (metrics["pay_total"] - metrics["member_variable_total"]) / max(metrics["total_minutes"], 1)
    if avg_savings_per_minute > 0:
        break_even_minutes = round(policy.month_fee / avg_savings_per_minute, 2)
    else:
        break_even_minutes = math.inf

    return decision, break_even_rides, break_even_minutes



def make_trace_message(kind: str, text: str) -> dict[str, str]:
    return {"kind": kind, "text": text}



def run_agent(df: pd.DataFrame, pricing_url: str) -> dict[str, Any]:
    started = time.perf_counter()
    logs: list[ToolLog] = []
    trace: list[dict[str, str]] = []
    stop_reason = "completed"

    normalized = normalize_columns(df)
    if normalized.empty:
        raise ValueError("Uploaded CSV has no usable trip rows after normalization")

    trace.append(make_trace_message("Thought", "I should inspect the uploaded trip data and confirm the schema is usable."))
    overview_sql = """
        SELECT
            COUNT(*) AS trip_count,
            ROUND(AVG(duration_min), 2) AS avg_duration_min,
            ROUND(SUM(is_ebike) * 100.0 / COUNT(*), 2) AS ebike_share_pct,
            MIN(started_at) AS min_started_at,
            MAX(started_at) AS max_started_at
        FROM trips
    """
    overview_result = run_tool(
        step=1,
        tool_name="csv_sql",
        args={"df": normalized, "sql": overview_sql},
        func=csv_sql,
        logs=logs,
    )
    if not overview_result["success"]:
        raise ValueError(overview_result["error"])
    overview = overview_result["data"]["rows"][0]
    trace.append(make_trace_message("Action", "Ran csv_sql for monthly overview."))
    trace.append(
        make_trace_message(
            "Observation",
            f"Found {overview['trip_count']} rides, average duration {overview['avg_duration_min']} min, ebike share {overview['ebike_share_pct']}%.",
        )
    )

    trace.append(make_trace_message("Thought", "Next I need a weekly breakdown for the results table."))
    weekly_sql = """
        SELECT
            week_start,
            COUNT(*) AS ride_count,
            ROUND(AVG(duration_min), 2) AS avg_duration_min,
            ROUND(SUM(is_ebike) * 100.0 / COUNT(*), 2) AS ebike_share_pct
        FROM trips
        GROUP BY week_start
        ORDER BY week_start
    """
    weekly_result = run_tool(
        step=2,
        tool_name="csv_sql",
        args={"df": normalized, "sql": weekly_sql},
        func=csv_sql,
        logs=logs,
    )
    if not weekly_result["success"]:
        raise ValueError(weekly_result["error"])
    trace.append(make_trace_message("Action", "Ran csv_sql for weekly usage aggregation."))
    trace.append(make_trace_message("Observation", f"Built {weekly_result['data']['row_count']} weekly rows."))

    policy_queries = [
        "Month pass price classic bike prices 45 min free ebike prices 0.17/min 0.20/min",
        "Single rides 1.00 unlock 0.19/min classic bikes 0.49/minute ebike",
    ]
    policy_passages: list[dict[str, Any]] = []
    for step_number, query in enumerate(policy_queries, start=3):
        trace.append(make_trace_message("Thought", f"I need official pricing evidence for: {query}."))
        policy_result = run_tool(
            step=step_number,
            tool_name="policy_retriever",
            args={"url": pricing_url, "query": query, "k": 3},
            func=policy_retriever,
            logs=logs,
        )
        if not policy_result["success"]:
            raise ValueError(policy_result["error"])
        trace.append(make_trace_message("Action", f"Fetched pricing snippets for query: {query}."))
        returned = policy_result["data"]["passages"]
        policy_passages.extend(returned)
        first_snippet = returned[0]["text"][:180] if returned else "No snippet returned."
        trace.append(make_trace_message("Observation", first_snippet))

    policy = parse_baywheels_policy(pricing_url)
    rides, metrics = compute_costs(normalized, policy)

    pay_calc = run_tool(
        step=5,
        tool_name="calculator",
        args={"expression": f"{metrics['pay_total']}"},
        func=calculator,
        logs=logs,
    )
    member_calc = run_tool(
        step=6,
        tool_name="calculator",
        args={"expression": f"{policy.month_fee}+{metrics['member_variable_total']}"},
        func=calculator,
        logs=logs,
    )
    if not pay_calc["success"] or not member_calc["success"]:
        raise ValueError("Calculator step failed")

    weekly_table = build_weekly_table(rides)
    decision, break_even_rides, break_even_minutes = build_recommendation(metrics, policy)
    delta = round(metrics["pay_total"] - metrics["member_total"], 2)

    trace.append(make_trace_message("Thought", "I can now compare pay-per-use against the month pass using the official pricing rules."))
    trace.append(make_trace_message("Action", "Calculated both totals and break-even values."))
    trace.append(
        make_trace_message(
            "Observation",
            f"Pay-per-use = ${metrics['pay_total']}, month pass = ${metrics['member_total']}, difference = ${delta}.",
        )
    )
    trace.append(make_trace_message("Final Answer", decision))

    justification = [
        (
            f"Bay Wheels lists a Month pass at ${policy.month_fee}/month, while single rides cost ${policy.single_unlock:.2f} to unlock plus ${policy.single_classic_per_min:.2f}/min on classic bikes and ${policy.single_ebike_per_min:.2f}/min on ebikes."
        ),
        (
            f"The pricing page also says month-pass riders get 45-minute classic rides included, and ebikes are charged ${policy.member_ebike_per_min:.2f}/min for the first 45 minutes and ${policy.member_ebike_after_per_min:.2f}/min after that, with no unlock fee."
        ),
        (
            f"For this uploaded month, the rides total ${metrics['pay_total']} under pay-per-use versus ${metrics['member_total']} with the month pass."
        ),
        (
            f"Because this month has {metrics['ride_count']} rides averaging {metrics['avg_duration']} minutes with {metrics['ebike_share']}% ebike usage, the better choice is: {decision}."
        ),
    ]

    assumptions = [
        "If bike type is missing in the CSV, the app conservatively assumes the ride is a classic bike.",
        "Costs use the durations present in the CSV without rounding up to the next whole minute unless your instructor explicitly requires minute-rounding.",
        "The uploaded CSV is treated as the rider's month of trips (for example, a station-pair or commute-window subset from a public monthly file).",
    ]

    total_time_sec = round(time.perf_counter() - started, 2)
    return {
        "trace": trace,
        "logs": [asdict(item) for item in logs],
        "weekly_table": weekly_table,
        "decision": decision,
        "metrics": metrics,
        "break_even_rides": break_even_rides,
        "break_even_minutes": break_even_minutes,
        "difference": delta,
        "justification": justification,
        "assumptions": assumptions,
        "policy_passages": policy_passages,
        "policy": asdict(policy),
        "stop_reason": stop_reason,
        "total_time_sec": total_time_sec,
    }



def render_results(result: dict[str, Any]) -> None:
    policy = result["policy"]
    metrics = result["metrics"]

    st.subheader("Timeline")
    for item in result["trace"]:
        st.markdown(f"**{item['kind']}** - {item['text']}")

    col1, col2, col3 = st.columns(3)
    col1.metric("Steps", len(result["logs"]))
    col2.metric("Total time (sec)", result["total_time_sec"])
    col3.metric("Stop reason", result["stop_reason"])

    st.subheader("Final decision")
    st.success(result["decision"])

    st.subheader("Justification")
    for sentence in result["justification"]:
        st.write(f"- {sentence}")

    st.subheader("Cost comparison")
    cost_table = pd.DataFrame(
        [
            {"Scenario": "Pay per ride/minute", "Cost ($)": metrics["pay_total"]},
            {"Scenario": "Month pass", "Cost ($)": result["policy"]["month_fee"]},
            {"Scenario": "Month pass residual ride charges", "Cost ($)": metrics["member_variable_total"]},
            {"Scenario": "Month pass total", "Cost ($)": metrics["member_total"]},
            {"Scenario": "Difference (pay - month pass)", "Cost ($)": result["difference"]},
        ]
    )
    st.dataframe(cost_table, use_container_width=True)

    st.subheader("Break-even")
    st.write(f"Break-even rides on this usage profile: {result['break_even_rides']}")
    st.write(f"Break-even minutes on this usage profile: {result['break_even_minutes']}")

    st.subheader("Weekly table")
    st.dataframe(result["weekly_table"], use_container_width=True)

    st.subheader("Citations")
    st.write(f"Pricing page URL: {policy['pricing_url']}")
    st.write(f"Capture date/time: {policy['captured_at']}")
    for passage in result["policy_passages"][:6]:
        st.markdown(f"> {passage['text']}  ")
        st.caption(f"Source: {passage['source']} | score={passage['score']}")

    st.subheader("Assumptions and caveats")
    for item in result["assumptions"]:
        st.write(f"- {item}")

    st.subheader("Per-step logs")
    st.dataframe(pd.DataFrame(result["logs"]), use_container_width=True)



def main() -> None:
    st.set_page_config(page_title="Bike-Share Pass Optimizer", layout="wide")
    st.title("Single-Agent ReAct + MRKL Bike-Share Pass Optimizer")
    st.caption("Recommended city for this implementation: Bay Wheels (monthly membership available).")

    with st.expander("Tool schemas", expanded=False):
        st.json(TOOL_SCHEMAS)

    csv_file = st.file_uploader("Upload one CSV month/subset", type=["csv"])
    pricing_url = st.text_input(
        "Official pricing URL",
        value="https://www.lyft.com/bikes/bay-wheels/pricing",
    )
    run_clicked = st.button("Run")

    if run_clicked:
        if csv_file is None or not pricing_url.strip():
            st.error("Please upload a CSV and provide the pricing URL.")
            return
        try:
            df = pd.read_csv(csv_file)
            result = run_agent(df, pricing_url.strip())
            render_results(result)
        except Exception as exc:  # pragma: no cover - Streamlit UI path
            st.exception(exc)


if __name__ == "__main__":
    main()
