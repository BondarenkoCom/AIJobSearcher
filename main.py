import argparse
import csv
from pathlib import Path
from typing import List

from src.config import load_config, cfg_get, resolve_path
from src.collectors.manual_csv import collect_from_csv
from src.collectors.http_json import collect_from_http_json
from src.collectors.vinasa_members import collect_vinasa_members
from src.collectors.danhbaict import collect_danhbaict
from src.filtering import filter_and_score
from src.output import write_scored_jobs
from src.company_output import write_company_leads
from src.ats_output import write_ats_sources
from src.ats_scanner import scan_companies_for_ats, collect_jobs_from_ats
from src.models import CompanyLead, ATSSource
from src.utils import dedupe_jobs, dedupe_companies, dedupe_ats_sources
from src.email_jobs import send_applications


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AI Job Searcher - CLI MVP")
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to config YAML (default: config/config.yaml)",
    )
    return parser.parse_args()


def read_companies_csv(path: Path) -> List[CompanyLead]:
    if not path.exists():
        return []
    leads: List[CompanyLead] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row:
                continue
            leads.append(
                CompanyLead(
                    name=(row.get("name") or "").strip(),
                    website=(row.get("website") or "").strip(),
                    location=(row.get("location") or "").strip(),
                    email=(row.get("email") or "").strip(),
                    source=(row.get("source") or "").strip(),
                    url=(row.get("url") or "").strip(),
                    notes=(row.get("notes") or "").strip(),
                    raw=row,
                )
            )
    return leads


def read_ats_sources_csv(path: Path) -> List[ATSSource]:
    if not path.exists():
        return []
    items: List[ATSSource] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row:
                continue
            items.append(
                ATSSource(
                    company=(row.get("company") or "").strip(),
                    website=(row.get("website") or "").strip(),
                    ats_type=(row.get("ats_type") or "").strip(),
                    board_url=(row.get("board_url") or "").strip(),
                    api_url=(row.get("api_url") or "").strip(),
                    source_url=(row.get("source_url") or "").strip(),
                )
            )
    return items


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parent

    cfg_path = resolve_path(root, args.config)
    cfg = load_config(str(cfg_path))

    out_dir = resolve_path(root, cfg_get(cfg, "output.out_dir", "data/out"))
    write_all = cfg_get(cfg, "output.write_all", "jobs_all.csv")
    write_shortlist = cfg_get(cfg, "output.write_shortlist", "jobs_shortlist.csv")
    write_companies = cfg_get(cfg, "output.write_companies", "companies.csv")
    write_ats_sources = cfg_get(cfg, "output.write_ats_sources", "ats_sources.csv")

    jobs = []

    manual_enabled = bool(cfg_get(cfg, "sources.manual_csv.enabled", False))
    if manual_enabled:
        manual_path = cfg_get(cfg, "sources.manual_csv.path", "data/inbox/manual_jobs.csv")
        manual_path = resolve_path(root, manual_path)
        jobs.extend(collect_from_csv(manual_path))

    http_sources = cfg_get(cfg, "sources.http_json", []) or []
    for src in http_sources:
        if not isinstance(src, dict):
            continue
        if not src.get("enabled", False):
            continue
        jobs.extend(collect_from_http_json(src))

    # Company leads
    company_sources = cfg_get(cfg, "company_sources", []) or []
    company_leads = []
    for src in company_sources:
        if not isinstance(src, dict):
            continue
        if not src.get("enabled", False):
            continue
        if not src.get("legal_confirmed", False):
            print(f"[company] skipped {src.get('name','source')} (legal_confirmed=false)")
            continue
        src_type = str(src.get("type") or "").lower()
        if src_type == "vinasa_members":
            leads = collect_vinasa_members(
                url=str(src.get("url", "")),
                location=str(src.get("location", "")),
                source=str(src.get("name") or "vinasa"),
                timeout_sec=float(src.get("timeout_sec", 20)),
            )
            company_leads.extend(leads)
        elif src_type == "danhbaict":
            leads = collect_danhbaict(
                list_url=str(src.get("list_url", "https://danhbaict.vn/doanh-nghiep/")),
                feed_url=str(src.get("feed_url", "")),
                max_pages=int(src.get("max_pages", 3)),
                max_items=int(src.get("max_items", 200)),
                fetch_details=bool(src.get("fetch_details", True)),
                sleep_sec=float(src.get("sleep_sec", 0.5)),
                timeout_sec=float(src.get("timeout_sec", 20)),
            )
            name = str(src.get("name") or "danhbaict")
            for lead in leads:
                if not lead.source:
                    lead.source = name
            company_leads.extend(leads)
        else:
            print(f"[company] unknown type: {src_type}")

    if company_leads:
        company_leads = dedupe_companies(company_leads)
        companies_path = out_dir / write_companies
        try:
            write_company_leads(company_leads, companies_path)
            print(f"Companies -> {companies_path}")
        except PermissionError:
            print(f"[company] companies.csv is locked: {companies_path} (close it and rerun)")

    # ATS scan and jobs
    ats_cfg = cfg_get(cfg, "ats", {}) or {}
    ats_sources: List[ATSSource] = []
    if ats_cfg.get("enabled", False):
        leads_for_ats = company_leads
        if not leads_for_ats:
            companies_csv = resolve_path(root, str(ats_cfg.get("companies_csv", out_dir / write_companies)))
            leads_for_ats = read_companies_csv(companies_csv)

        if leads_for_ats:
            start_index = int(ats_cfg.get("start_index", 0))
            max_companies = int(ats_cfg.get("max_companies", 200))
            leads_slice = leads_for_ats[start_index:start_index + max_companies]

            ats_sources = scan_companies_for_ats(
                leads_slice,
                max_companies=max_companies,
                max_links_per_company=int(ats_cfg.get("max_links_per_company", 3)),
                timeout_sec=float(ats_cfg.get("timeout_sec", 15)),
                sleep_sec=float(ats_cfg.get("sleep_sec", 0.3)),
                user_agent=str(ats_cfg.get("user_agent", "Mozilla/5.0")),
                start_index=start_index,
                progress_every=int(ats_cfg.get("progress_every", 10)),
                scan_links=bool(ats_cfg.get("scan_links", True)),
                scan_common_paths=bool(ats_cfg.get("scan_common_paths", True)),
            )

            if ats_sources:
                ats_path = out_dir / write_ats_sources
                existing = read_ats_sources_csv(ats_path)
                merged = dedupe_ats_sources(existing + ats_sources)
                write_ats_sources(merged, ats_path)
                print(f"ATS sources -> {ats_path}")

            if ats_cfg.get("fetch_jobs", True) and ats_sources:
                enabled_types = ats_cfg.get("types", None)
                ats_jobs = collect_jobs_from_ats(
                    ats_sources,
                    enabled_types=enabled_types,
                    timeout_sec=float(ats_cfg.get("jobs_timeout_sec", 20)),
                    sleep_sec=float(ats_cfg.get("sleep_sec", 0.3)),
                )
                jobs.extend(ats_jobs)

    jobs = dedupe_jobs(jobs)

    include_keywords = cfg_get(cfg, "profile.keywords.include", []) or []
    exclude_keywords = cfg_get(cfg, "profile.keywords.exclude", []) or []
    locations = cfg_get(cfg, "profile.locations", []) or []
    min_score = int(cfg_get(cfg, "filters.min_score", 1))
    max_results = int(cfg_get(cfg, "filters.max_results", 200))

    scored_all, shortlisted = filter_and_score(
        jobs,
        include_keywords=include_keywords,
        exclude_keywords=exclude_keywords,
        locations=locations,
        min_score=min_score,
        max_results=max_results,
    )

    write_scored_jobs(scored_all, out_dir / write_all)
    write_scored_jobs(shortlisted, out_dir / write_shortlist)

    print(f"Collected: {len(jobs)}")
    print(f"Shortlisted: {len(shortlisted)}")
    print(f"All jobs -> {out_dir / write_all}")
    print(f"Shortlist -> {out_dir / write_shortlist}")

    sent = send_applications(root, cfg)
    if sent:
        print(f"Emails sent: {sent}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
