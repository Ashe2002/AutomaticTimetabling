#!/usr/bin/env python3
"""
xhstt_to_csv.py

Parse an XHSTT / HSEval High School Timetable Archive XML into multiple CSVs.

Usage:
  python xhstt_to_csv.py FinlandArtificialSchool.xml output_folder

Outputs (when present in the XML):
  archives.csv
  instances.csv
  time_groups.csv
  times.csv
  time_group_membership.csv
  resource_types.csv
  resource_groups.csv
  resources.csv
  resource_group_membership.csv
  event_groups.csv
  events.csv
  event_eventgroup_membership.csv
  event_resources.csv
  event_resourcegroups.csv
  constraints.csv
  constraint_applies_to.csv
  constraint_params.csv
  solution_groups.csv
  solutions.csv
  solution_events.csv
  solution_event_resources.csv
  reports.csv
  report_resource_violations.csv
  report_event_violations.csv
  report_eventgroup_violations.csv
"""

from __future__ import annotations

import csv
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
import xml.etree.ElementTree as ET


def strip_ns(tag: str) -> str:
    """Strip XML namespace if present: {ns}Tag -> Tag"""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def text_of(elem: ET.Element | None) -> str:
    return "" if elem is None else (elem.text or "").strip()


def find_child(elem: ET.Element | None, name: str) -> ET.Element | None:
    if elem is None:
        return None
    for ch in list(elem):
        if strip_ns(ch.tag) == name:
            return ch
    return None


def find_children(elem: ET.Element | None, name: str) -> list[ET.Element]:
    if elem is None:
        return []
    out = []
    for ch in list(elem):
        if strip_ns(ch.tag) == name:
            out.append(ch)
    return out


def parse_metadata(meta_elem: ET.Element | None, prefix: str = "") -> dict[str, str]:
    """
    MetaData children are text-only. We store them as columns:
      Name, Contributor, Date, Country, Description, Remarks, Publication...
    """
    d: dict[str, str] = {}
    if meta_elem is None:
        return d
    for ch in list(meta_elem):
        k = prefix + strip_ns(ch.tag)
        d[k] = text_of(ch)
    return d


@dataclass
class CSVSink:
    out_dir: Path
    rows: dict[str, list[dict[str, str]]]

    def __init__(self, out_dir: Path):
        self.out_dir = out_dir
        self.rows = defaultdict(list)

    def add(self, table: str, row: dict[str, object]) -> None:
        # Normalize None -> "" and cast to string for CSV safety
        clean: dict[str, str] = {}
        for k, v in row.items():
            if v is None:
                clean[k] = ""
            else:
                clean[k] = str(v)
        self.rows[table].append(clean)

    def write_all(self) -> None:
        self.out_dir.mkdir(parents=True, exist_ok=True)

        for table, rows in self.rows.items():
            # Build a stable header order:
            # first-seen key order, then any missing keys appended (deterministic).
            header: list[str] = []
            seen = set()
            for r in rows:
                for k in r.keys():
                    if k not in seen:
                        seen.add(k)
                        header.append(k)

            # Ensure all rows have all keys
            for r in rows:
                for k in header:
                    r.setdefault(k, "")

            path = self.out_dir / f"{table}.csv"
            with path.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=header)
                w.writeheader()
                w.writerows(rows)


# -----------------------------
# Constraint parsing (generic)
# -----------------------------

def parse_applies_to(applies_elem: ET.Element | None, instance_id: str, constraint_id: str, sink: CSVSink) -> None:
    """
    AppliesTo can contain nested lists of references (Resources, Events, EventGroups, TimeGroups, etc.).
    We record every node with a Reference attribute.
    """
    if applies_elem is None:
        return

    stack: list[tuple[ET.Element, str]] = [(applies_elem, "AppliesTo")]
    while stack:
        node, path = stack.pop()
        for ch in list(node):
            tag = strip_ns(ch.tag)
            ch_path = f"{path}/{tag}"
            if "Reference" in ch.attrib:
                sink.add(
                    "constraint_applies_to",
                    {
                        "instance_id": instance_id,
                        "constraint_id": constraint_id,
                        "path": ch_path,
                        "ref_type": tag,
                        "reference": ch.attrib.get("Reference", ""),
                    },
                )
            stack.append((ch, ch_path))


def gather_constraint_params(
    elem: ET.Element,
    instance_id: str,
    constraint_id: str,
    sink: CSVSink,
    base_path: str = "",
) -> None:
    """
    Store *all* constraint content (other than AppliesTo which goes elsewhere)
    as a generic path/value table. This avoids hardcoding dozens of constraint schemas.
    """
    for ch in list(elem):
        tag = strip_ns(ch.tag)
        if tag == "AppliesTo":
            continue

        path = f"{base_path}/{tag}" if base_path else tag

        # Store attributes (including Reference if present)
        for ak, av in ch.attrib.items():
            if ak == "Id":
                continue
            sink.add(
                "constraint_params",
                {
                    "instance_id": instance_id,
                    "constraint_id": constraint_id,
                    "path": path,
                    "attr": ak,
                    "value": av,
                },
            )

        # Store leaf text if any
        t = text_of(ch)
        if t:
            sink.add(
                "constraint_params",
                {
                    "instance_id": instance_id,
                    "constraint_id": constraint_id,
                    "path": path,
                    "attr": "text",
                    "value": t,
                },
            )

        # Recurse
        if list(ch):
            gather_constraint_params(ch, instance_id, constraint_id, sink, path)


# -----------------------------
# Instance parsing
# -----------------------------

def parse_instance(inst: ET.Element, sink: CSVSink) -> None:
    instance_id = inst.attrib.get("Id", "")

    # Instance metadata
    sink.add("instances", {"instance_id": instance_id, **parse_metadata(find_child(inst, "MetaData"))})

    # Times
    times_elem = find_child(inst, "Times")
    if times_elem is not None:
        # TimeGroups declaration (Weeks/Days/TimeGroup)
        tgs = find_child(times_elem, "TimeGroups")
        if tgs is not None:
            for tg in list(tgs):
                tg_tag = strip_ns(tg.tag)  # Week, Day, TimeGroup
                sink.add(
                    "time_groups",
                    {
                        "instance_id": instance_id,
                        "group_id": tg.attrib.get("Id", ""),
                        "group_type": tg_tag,
                        "name": text_of(find_child(tg, "Name")),
                    },
                )

        # Times (order matters)
        all_times = find_children(times_elem, "Time")
        for order_index, t in enumerate(all_times):
            time_id = t.attrib.get("Id", "")
            name = text_of(find_child(t, "Name"))

            week = find_child(t, "Week")
            day = find_child(t, "Day")
            week_ref = "" if week is None else week.attrib.get("Reference", "")
            day_ref = "" if day is None else day.attrib.get("Reference", "")

            sink.add(
                "times",
                {
                    "instance_id": instance_id,
                    "time_id": time_id,
                    "name": name,
                    "order_index": order_index,
                    "week_ref": week_ref,
                    "day_ref": day_ref,
                },
            )

            if week_ref:
                sink.add(
                    "time_group_membership",
                    {"instance_id": instance_id, "time_id": time_id, "group_id": week_ref, "membership_type": "Week"},
                )
            if day_ref:
                sink.add(
                    "time_group_membership",
                    {"instance_id": instance_id, "time_id": time_id, "group_id": day_ref, "membership_type": "Day"},
                )

            tgm = find_child(t, "TimeGroups")
            if tgm is not None:
                for tgref in find_children(tgm, "TimeGroup"):
                    ref = tgref.attrib.get("Reference", "")
                    if ref:
                        sink.add(
                            "time_group_membership",
                            {
                                "instance_id": instance_id,
                                "time_id": time_id,
                                "group_id": ref,
                                "membership_type": "TimeGroup",
                            },
                        )

    # Resources
    res_elem = find_child(inst, "Resources")
    if res_elem is not None:
        rtypes = find_child(res_elem, "ResourceTypes")
        if rtypes is not None:
            for rt in find_children(rtypes, "ResourceType"):
                sink.add(
                    "resource_types",
                    {
                        "instance_id": instance_id,
                        "resource_type_id": rt.attrib.get("Id", ""),
                        "name": text_of(find_child(rt, "Name")),
                    },
                )

        rgs = find_child(res_elem, "ResourceGroups")
        if rgs is not None:
            for rg in find_children(rgs, "ResourceGroup"):
                rtype = find_child(rg, "ResourceType")
                sink.add(
                    "resource_groups",
                    {
                        "instance_id": instance_id,
                        "resource_group_id": rg.attrib.get("Id", ""),
                        "name": text_of(find_child(rg, "Name")),
                        "resource_type_ref": "" if rtype is None else rtype.attrib.get("Reference", ""),
                    },
                )

        for r in find_children(res_elem, "Resource"):
            resource_id = r.attrib.get("Id", "")
            rtype = find_child(r, "ResourceType")

            sink.add(
                "resources",
                {
                    "instance_id": instance_id,
                    "resource_id": resource_id,
                    "name": text_of(find_child(r, "Name")),
                    "resource_type_ref": "" if rtype is None else rtype.attrib.get("Reference", ""),
                },
            )

            rgs2 = find_child(r, "ResourceGroups")
            if rgs2 is not None:
                for rgref in find_children(rgs2, "ResourceGroup"):
                    ref = rgref.attrib.get("Reference", "")
                    if ref:
                        sink.add(
                            "resource_group_membership",
                            {"instance_id": instance_id, "resource_id": resource_id, "resource_group_id": ref},
                        )

    # Events
    events_elem = find_child(inst, "Events")
    if events_elem is not None:
        egs = find_child(events_elem, "EventGroups")
        if egs is not None:
            for grp in list(egs):
                sink.add(
                    "event_groups",
                    {
                        "instance_id": instance_id,
                        "event_group_id": grp.attrib.get("Id", ""),
                        "group_type": strip_ns(grp.tag),  # Course or EventGroup
                        "name": text_of(find_child(grp, "Name")),
                    },
                )

        for e in find_children(events_elem, "Event"):
            event_id = e.attrib.get("Id", "")
            color = e.attrib.get("Color", "")

            course = find_child(e, "Course")
            time = find_child(e, "Time")

            sink.add(
                "events",
                {
                    "instance_id": instance_id,
                    "event_id": event_id,
                    "name": text_of(find_child(e, "Name")),
                    "color": color,
                    "duration": text_of(find_child(e, "Duration")),
                    "workload": text_of(find_child(e, "Workload")),
                    "course_ref": "" if course is None else course.attrib.get("Reference", ""),
                    "preassigned_time_ref": "" if time is None else time.attrib.get("Reference", ""),
                },
            )

            egms = find_child(e, "EventGroups")
            if egms is not None:
                for egref in find_children(egms, "EventGroup"):
                    ref = egref.attrib.get("Reference", "")
                    if ref:
                        sink.add(
                            "event_eventgroup_membership",
                            {"instance_id": instance_id, "event_id": event_id, "event_group_id": ref},
                        )

            # Event resources (instance-level)
            eres = find_child(e, "Resources")
            if eres is not None:
                for er_index, er in enumerate(find_children(eres, "Resource")):
                    sink.add(
                        "event_resources",
                        {
                            "instance_id": instance_id,
                            "event_id": event_id,
                            "er_index": er_index,
                            "role": text_of(find_child(er, "Role")),
                            "resource_type_ref": (
                                "" if find_child(er, "ResourceType") is None else find_child(er, "ResourceType").attrib.get("Reference", "")
                            ),
                            "reference_resource_id": er.attrib.get("Reference", ""),
                            "workload": text_of(find_child(er, "Workload")),
                        },
                    )

            # Event resource groups
            ergs = find_child(e, "ResourceGroups")
            if ergs is not None:
                for rgref in find_children(ergs, "ResourceGroup"):
                    ref = rgref.attrib.get("Reference", "")
                    if ref:
                        sink.add(
                            "event_resourcegroups",
                            {"instance_id": instance_id, "event_id": event_id, "resource_group_id": ref},
                        )

    # Constraints
    cons_elem = find_child(inst, "Constraints")
    if cons_elem is not None:
        for con in list(cons_elem):
            ctype = strip_ns(con.tag)
            constraint_id = con.attrib.get("Id", "")

            sink.add(
                "constraints",
                {
                    "instance_id": instance_id,
                    "constraint_id": constraint_id,
                    "constraint_type": ctype,
                    "name": text_of(find_child(con, "Name")),
                    "required": text_of(find_child(con, "Required")),
                    "weight": text_of(find_child(con, "Weight")),
                    "cost_function": text_of(find_child(con, "CostFunction")),
                },
            )

            parse_applies_to(find_child(con, "AppliesTo"), instance_id, constraint_id, sink)
            gather_constraint_params(con, instance_id, constraint_id, sink)


# -----------------------------
# Solution groups / solutions
# -----------------------------

def parse_solution_groups(root: ET.Element, sink: CSVSink) -> None:
    sgs = find_child(root, "SolutionGroups")
    if sgs is None:
        return

    for sg in find_children(sgs, "SolutionGroup"):
        sgid = sg.attrib.get("Id", "")
        sink.add("solution_groups", {"solution_group_id": sgid, **parse_metadata(find_child(sg, "MetaData"))})

        for sol_idx, sol in enumerate(find_children(sg, "Solution")):
            inst_ref = sol.attrib.get("Reference", "")

            sink.add(
                "solutions",
                {
                    "solution_group_id": sgid,
                    "solution_index": sol_idx,
                    "instance_reference": inst_ref,
                    "description": text_of(find_child(sol, "Description")),
                    "running_time": text_of(find_child(sol, "RunningTime")),
                },
            )

            evs = find_child(sol, "Events")
            if evs is not None:
                for se_idx, sev in enumerate(find_children(evs, "Event")):
                    time = find_child(sev, "Time")
                    sink.add(
                        "solution_events",
                        {
                            "solution_group_id": sgid,
                            "solution_index": sol_idx,
                            "solution_event_index": se_idx,
                            "instance_event_reference": sev.attrib.get("Reference", ""),
                            "duration": text_of(find_child(sev, "Duration")),
                            "time_reference": "" if time is None else time.attrib.get("Reference", ""),
                        },
                    )

                    sres = find_child(sev, "Resources")
                    if sres is not None:
                        for sr in find_children(sres, "Resource"):
                            sink.add(
                                "solution_event_resources",
                                {
                                    "solution_group_id": sgid,
                                    "solution_index": sol_idx,
                                    "solution_event_index": se_idx,
                                    "resource_reference": sr.attrib.get("Reference", ""),
                                    "role": text_of(find_child(sr, "Role")),
                                },
                            )

            rep = find_child(sol, "Report")
            if rep is not None:
                sink.add(
                    "reports",
                    {
                        "solution_group_id": sgid,
                        "solution_index": sol_idx,
                        "infeasibility_value": text_of(find_child(rep, "InfeasibilityValue")),
                        "objective_value": text_of(find_child(rep, "ObjectiveValue")),
                    },
                )

                rep_res = find_child(rep, "Resources")
                if rep_res is not None:
                    for r in find_children(rep_res, "Resource"):
                        rref = r.attrib.get("Reference", "")
                        for c in find_children(r, "Constraint"):
                            sink.add(
                                "report_resource_violations",
                                {
                                    "solution_group_id": sgid,
                                    "solution_index": sol_idx,
                                    "resource_reference": rref,
                                    "constraint_reference": c.attrib.get("Reference", ""),
                                    "cost": text_of(find_child(c, "Cost")),
                                    "description": text_of(find_child(c, "Description")),
                                },
                            )

                rep_ev = find_child(rep, "Events")
                if rep_ev is not None:
                    for e in find_children(rep_ev, "Event"):
                        eref = e.attrib.get("Reference", "")
                        for c in find_children(e, "Constraint"):
                            sink.add(
                                "report_event_violations",
                                {
                                    "solution_group_id": sgid,
                                    "solution_index": sol_idx,
                                    "event_reference": eref,
                                    "constraint_reference": c.attrib.get("Reference", ""),
                                    "cost": text_of(find_child(c, "Cost")),
                                    "description": text_of(find_child(c, "Description")),
                                },
                            )

                rep_eg = find_child(rep, "EventGroups")
                if rep_eg is not None:
                    for eg in find_children(rep_eg, "EventGroup"):
                        egref = eg.attrib.get("Reference", "")
                        for c in find_children(eg, "Constraint"):
                            sink.add(
                                "report_eventgroup_violations",
                                {
                                    "solution_group_id": sgid,
                                    "solution_index": sol_idx,
                                    "event_group_reference": egref,
                                    "constraint_reference": c.attrib.get("Reference", ""),
                                    "cost": text_of(find_child(c, "Cost")),
                                    "description": text_of(find_child(c, "Description")),
                                },
                            )


# -----------------------------
# Archive parsing (top level)
# -----------------------------

def parse_archive(xml_path: Path, out_dir: Path) -> int:
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # If namespaces are used, ElementTree still gives tags like "{ns}HighSchoolTimetableArchive".
    # We'll rely on strip_ns() everywhere.

    sink = CSVSink(out_dir)

    # Archive
    sink.add("archives", {"archive_id": root.attrib.get("Id", ""), **parse_metadata(find_child(root, "MetaData"))})

    # Instances
    insts = find_child(root, "Instances")
    if insts is not None:
        for inst in find_children(insts, "Instance"):
            parse_instance(inst, sink)

    # SolutionGroups (optional)
    parse_solution_groups(root, sink)

    sink.write_all()
    print(f"Done. Wrote {len(sink.rows)} CSV tables into: {out_dir}")
    return 0


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: python xhstt_to_csv.py input.xml output_dir", file=sys.stderr)
        return 2

    xml_path = Path(sys.argv[1])
    out_dir = Path(sys.argv[2])

    if not xml_path.exists():
        print(f"ERROR: input file not found: {xml_path}", file=sys.stderr)
        return 2

    return parse_archive(xml_path, out_dir)


if __name__ == "__main__":
    raise SystemExit(main())
