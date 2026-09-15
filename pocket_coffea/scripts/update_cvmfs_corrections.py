from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union
from urllib.parse import quote

import requests


CVMFS_PATTERN = re.compile(r"\$\{cvmfs:([^{}]+)\}")
DATE_TAG_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DEFAULT_BASE_PATH = "/cvmfs/cms-griddata.cern.ch/cat/metadata"
DEFAULT_PARAMETERS_GLOB = "pocket_coffea/parameters/*.yaml"
DEFAULT_BRANCH_PREFIX = "ci/cvmfs-corrections"


class CorrectionUpdateError(Exception):
    """Raised when a referenced CVMFS correction cannot be inspected safely."""


@dataclass(frozen=True)
class CvmfsReference:
    path: str
    start: int
    end: int
    line: int
    expression: str
    campaign: str
    pog: str
    filename: str
    tag: Optional[str]
    tag_start: Optional[int]
    tag_end: Optional[int]


@dataclass
class ReferenceReport:
    path: str
    line: int
    campaign: str
    pog: str
    filename: str
    old_tag: Optional[str]
    selected_tag: Optional[str]
    status: str
    cvmfs_path: Optional[str] = None
    message: Optional[str] = None

    def to_dict(self) -> Dict[str, Optional[str]]:
        return {
            "path": self.path,
            "line": self.line,
            "campaign": self.campaign,
            "pog": self.pog,
            "filename": self.filename,
            "old_tag": self.old_tag,
            "selected_tag": self.selected_tag,
            "status": self.status,
            "cvmfs_path": self.cvmfs_path,
            "message": self.message,
        }


@dataclass
class MergeRequestReport:
    branch: str
    commit_id: Optional[str] = None
    merge_request_url: Optional[str] = None
    status: str = "not_created"
    message: Optional[str] = None

    def to_dict(self) -> Dict[str, Optional[str]]:
        return {
            "branch": self.branch,
            "commit_id": self.commit_id,
            "merge_request_url": self.merge_request_url,
            "status": self.status,
            "message": self.message,
        }


@dataclass
class PogUpdatePlan:
    pog: str
    reports: List[ReferenceReport] = field(default_factory=list)
    changed_files: Dict[str, str] = field(default_factory=dict)
    merge_request: Optional[MergeRequestReport] = None

    @property
    def changed_file_paths(self) -> List[str]:
        return sorted(self.changed_files)

    def to_dict(self) -> Dict[str, object]:
        return {
            "pog": self.pog,
            "changed_files": self.changed_file_paths,
            "references": [report.to_dict() for report in self.reports],
            "merge_request": (
                self.merge_request.to_dict() if self.merge_request is not None else None
            ),
        }


@dataclass
class UpdatePlan:
    requested_pogs: str
    selected_pogs: List[str]
    base_path: str
    parameter_files: List[str]
    pogs: Dict[str, PogUpdatePlan]
    combined_changed_files: Dict[str, str]
    errors: List[str] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return any(plan.changed_files for plan in self.pogs.values())

    def to_dict(self) -> Dict[str, object]:
        return {
            "requested_pogs": self.requested_pogs,
            "selected_pogs": self.selected_pogs,
            "base_path": self.base_path,
            "parameter_files": self.parameter_files,
            "changed_files": sorted(self.combined_changed_files),
            "errors": self.errors,
            "pogs": {pog: plan.to_dict() for pog, plan in sorted(self.pogs.items())},
        }


def parse_cvmfs_references(path: Path, text: str) -> List[CvmfsReference]:
    references: List[CvmfsReference] = []
    for match in CVMFS_PATTERN.finditer(text):
        args = match.group(1).split(",")
        if len(args) not in (3, 4):
            continue

        campaign = args[0].strip()
        pog = args[1].strip()
        filename = args[2].strip()
        tag = args[3].strip() if len(args) == 4 else None
        tag_start = tag_end = None
        if len(args) == 4:
            tag_start = match.start(1) + sum(len(arg) + 1 for arg in args[:3])
            tag_end = match.end(1)

        references.append(
            CvmfsReference(
                path=str(path),
                start=match.start(),
                end=match.end(),
                line=text.count("\n", 0, match.start()) + 1,
                expression=match.group(0),
                campaign=campaign,
                pog=pog,
                filename=filename,
                tag=tag,
                tag_start=tag_start,
                tag_end=tag_end,
            )
        )
    return references


def collect_references(parameter_files: Sequence[Path]) -> Tuple[Dict[str, str], List[CvmfsReference]]:
    file_texts: Dict[str, str] = {}
    references: List[CvmfsReference] = []
    for path in parameter_files:
        text = path.read_text()
        file_texts[str(path)] = text
        references.extend(parse_cvmfs_references(path, text))
    return file_texts, references


def discover_pogs(references: Iterable[CvmfsReference]) -> List[str]:
    return sorted({reference.pog for reference in references})


def resolve_requested_pogs(requested: Optional[str], known_pogs: Sequence[str]) -> List[str]:
    if requested is None or requested.strip() == "" or requested.strip().lower() == "all":
        return sorted(known_pogs)

    known_by_upper = {pog.upper(): pog for pog in known_pogs}
    selected: List[str] = []
    missing: List[str] = []
    for raw_pog in requested.split(","):
        pog = raw_pog.strip()
        if not pog:
            continue
        resolved = known_by_upper.get(pog.upper())
        if resolved is None:
            missing.append(pog)
        elif resolved not in selected:
            selected.append(resolved)

    if missing:
        known = ", ".join(sorted(known_pogs)) or "<none>"
        raise CorrectionUpdateError(
            f"Requested POG(s) not found in parameter references: {', '.join(missing)}. "
            f"Known POGs: {known}"
        )
    if not selected:
        raise CorrectionUpdateError("No POGs selected for update.")
    return selected


def _date_tags_with_file(base_path: Path, pog: str, campaign: str, filename: str) -> List[str]:
    campaign_dir = base_path / pog / campaign
    if not campaign_dir.is_dir():
        raise CorrectionUpdateError(f"CVMFS campaign directory not found: {campaign_dir}")

    tags = [
        path.name
        for path in campaign_dir.iterdir()
        if path.is_dir()
        and DATE_TAG_PATTERN.match(path.name)
        and (path / filename).is_file()
    ]
    if not tags:
        raise CorrectionUpdateError(
            f"No date-versioned CVMFS directory contains {filename}: {campaign_dir}"
        )
    return sorted(tags)


def latest_tag_for_reference(base_path: Path, reference: CvmfsReference) -> Tuple[str, str]:
    tags = _date_tags_with_file(
        base_path, reference.pog, reference.campaign, reference.filename
    )
    selected_tag = tags[-1]
    selected_path = (
        base_path
        / reference.pog
        / reference.campaign
        / selected_tag
        / reference.filename
    )
    return selected_tag, str(selected_path)


def _apply_replacements(text: str, replacements: Sequence[Tuple[int, int, str]]) -> str:
    updated = text
    for start, end, value in sorted(replacements, reverse=True):
        updated = updated[:start] + value + updated[end:]
    return updated


def build_update_plan(
    parameter_files: Sequence[Path],
    requested_pogs: str = "all",
    base_path: Union[Path, str] = DEFAULT_BASE_PATH,
) -> UpdatePlan:
    base = Path(base_path)
    file_texts, references = collect_references(parameter_files)
    known_pogs = discover_pogs(references)

    errors: List[str] = []
    try:
        selected_pogs = resolve_requested_pogs(requested_pogs, known_pogs)
    except CorrectionUpdateError as exc:
        selected_pogs = []
        errors.append(str(exc))

    pog_plans = {pog: PogUpdatePlan(pog=pog) for pog in selected_pogs}
    replacements_by_pog: Dict[str, Dict[str, List[Tuple[int, int, str]]]] = {
        pog: {} for pog in selected_pogs
    }
    combined_replacements: Dict[str, List[Tuple[int, int, str]]] = {}

    for reference in references:
        if reference.pog not in pog_plans:
            continue

        if reference.tag is None:
            pog_plans[reference.pog].reports.append(
                ReferenceReport(
                    path=reference.path,
                    line=reference.line,
                    campaign=reference.campaign,
                    pog=reference.pog,
                    filename=reference.filename,
                    old_tag=None,
                    selected_tag=None,
                    status="skipped_untagged",
                    message="Reference has no explicit date tag; leaving it unpinned.",
                )
            )
            continue

        if reference.tag_start is None or reference.tag_end is None:
            continue

        if not DATE_TAG_PATTERN.match(reference.tag):
            pog_plans[reference.pog].reports.append(
                ReferenceReport(
                    path=reference.path,
                    line=reference.line,
                    campaign=reference.campaign,
                    pog=reference.pog,
                    filename=reference.filename,
                    old_tag=reference.tag,
                    selected_tag=None,
                    status="skipped_non_date_tag",
                    message="Reference tag is not an ISO date; leaving it unchanged.",
                )
            )
            continue

        try:
            selected_tag, selected_path = latest_tag_for_reference(base, reference)
        except CorrectionUpdateError as exc:
            message = f"{reference.path}:{reference.line}: {exc}"
            errors.append(message)
            pog_plans[reference.pog].reports.append(
                ReferenceReport(
                    path=reference.path,
                    line=reference.line,
                    campaign=reference.campaign,
                    pog=reference.pog,
                    filename=reference.filename,
                    old_tag=reference.tag,
                    selected_tag=None,
                    status="error",
                    message=str(exc),
                )
            )
            continue

        if selected_tag > reference.tag:
            status = "updated"
            replacements_by_pog[reference.pog].setdefault(reference.path, []).append(
                (reference.tag_start, reference.tag_end, selected_tag)
            )
            combined_replacements.setdefault(reference.path, []).append(
                (reference.tag_start, reference.tag_end, selected_tag)
            )
        elif selected_tag == reference.tag:
            status = "current"
        else:
            status = "ahead"

        pog_plans[reference.pog].reports.append(
            ReferenceReport(
                path=reference.path,
                line=reference.line,
                campaign=reference.campaign,
                pog=reference.pog,
                filename=reference.filename,
                old_tag=reference.tag,
                selected_tag=selected_tag,
                status=status,
                cvmfs_path=selected_path,
            )
        )

    for pog, files in replacements_by_pog.items():
        for path, replacements in files.items():
            updated = _apply_replacements(file_texts[path], replacements)
            if updated != file_texts[path]:
                pog_plans[pog].changed_files[path] = updated

    combined_changed_files: Dict[str, str] = {}
    for path, replacements in combined_replacements.items():
        updated = _apply_replacements(file_texts[path], replacements)
        if updated != file_texts[path]:
            combined_changed_files[path] = updated

    return UpdatePlan(
        requested_pogs=requested_pogs,
        selected_pogs=selected_pogs,
        base_path=str(base),
        parameter_files=[str(path) for path in parameter_files],
        pogs=pog_plans,
        combined_changed_files=combined_changed_files,
        errors=errors,
    )


class GitLabClient:
    def __init__(self, api_v4_url: str, project_id: str, token: str):
        self.api_v4_url = api_v4_url.rstrip("/")
        self.project_id = project_id
        self.session = requests.Session()
        self.session.headers.update({"PRIVATE-TOKEN": token})

    def _url(self, path: str) -> str:
        project = quote(str(self.project_id), safe="")
        return f"{self.api_v4_url}/projects/{project}{path}"

    def _request(self, method: str, path: str, **kwargs):
        response = self.session.request(method, self._url(path), timeout=60, **kwargs)
        if response.status_code >= 400:
            raise CorrectionUpdateError(
                f"GitLab API {method} {path} failed with HTTP {response.status_code}: "
                f"{response.text}"
            )
        if response.text:
            return response.json()
        return None

    def branch_exists(self, branch: str) -> bool:
        branch_path = quote(branch, safe="")
        response = self.session.request(
            "GET",
            self._url(f"/repository/branches/{branch_path}"),
            timeout=60,
        )
        if response.status_code == 404:
            return False
        if response.status_code >= 400:
            raise CorrectionUpdateError(
                f"GitLab API GET branch failed with HTTP {response.status_code}: "
                f"{response.text}"
            )
        return True

    def raw_file_content(self, file_path: str, ref: str) -> Optional[str]:
        encoded_file = quote(file_path, safe="")
        response = self.session.request(
            "GET",
            self._url(f"/repository/files/{encoded_file}/raw"),
            params={"ref": ref},
            timeout=60,
        )
        if response.status_code == 404:
            return None
        if response.status_code >= 400:
            raise CorrectionUpdateError(
                f"GitLab API GET file failed with HTTP {response.status_code}: "
                f"{response.text}"
            )
        return response.text

    def commit_files(
        self,
        branch: str,
        start_branch: str,
        files: Dict[str, str],
        commit_message: str,
    ) -> Optional[str]:
        exists = self.branch_exists(branch)
        actions = []
        compare_ref = branch if exists else start_branch
        for path, content in sorted(files.items()):
            remote_content = self.raw_file_content(path, compare_ref)
            if remote_content == content:
                continue
            actions.append(
                {
                    "action": "update" if remote_content is not None else "create",
                    "file_path": path,
                    "content": content,
                }
            )

        if not actions:
            return None

        payload = {
            "branch": branch,
            "commit_message": commit_message,
            "actions": actions,
        }
        if not exists:
            payload["start_branch"] = start_branch

        data = self._request("POST", "/repository/commits", json=payload)
        return data.get("id") if isinstance(data, dict) else None

    def ensure_merge_request(
        self,
        source_branch: str,
        target_branch: str,
        title: str,
        description: str,
    ) -> str:
        merge_requests = self._request(
            "GET",
            "/merge_requests",
            params={
                "state": "opened",
                "source_branch": source_branch,
                "target_branch": target_branch,
            },
        )
        if merge_requests:
            return merge_requests[0]["web_url"]

        data = self._request(
            "POST",
            "/merge_requests",
            json={
                "source_branch": source_branch,
                "target_branch": target_branch,
                "title": title,
                "description": description,
                "remove_source_branch": True,
            },
        )
        return data["web_url"]


def slugify_pog(pog: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", pog.lower()).strip("-")


def branch_for_pog(pog: str, prefix: str = DEFAULT_BRANCH_PREFIX) -> str:
    return f"{prefix.rstrip('/')}/{slugify_pog(pog)}"


def merge_request_description(pog: str, plan: PogUpdatePlan) -> str:
    updated = [report for report in plan.reports if report.status == "updated"]
    lines = [
        f"Automated CVMFS correction tag update for `{pog}`.",
        "",
        "Updated references:",
    ]
    for report in updated:
        lines.append(
            f"- `{report.path}:{report.line}` `{report.campaign}` "
            f"`{report.filename}`: `{report.old_tag}` -> `{report.selected_tag}`"
        )
    if not updated:
        lines.append("- No reference updates were needed.")
    lines.extend(
        [
            "",
            "This merge request was created by the scheduled PocketCoffea "
            "CVMFS correction updater.",
        ]
    )
    return "\n".join(lines)


def create_merge_requests(
    plan: UpdatePlan,
    client: GitLabClient,
    target_branch: str,
    branch_prefix: str = DEFAULT_BRANCH_PREFIX,
) -> None:
    for pog, pog_plan in sorted(plan.pogs.items()):
        branch = branch_for_pog(pog, branch_prefix)
        if not pog_plan.changed_files:
            pog_plan.merge_request = MergeRequestReport(
                branch=branch,
                status="not_needed",
                message="No correction tag updates found.",
            )
            continue

        commit_id = client.commit_files(
            branch=branch,
            start_branch=target_branch,
            files=pog_plan.changed_files,
            commit_message=f"Update {pog} CVMFS correction tags",
        )
        mr_url = client.ensure_merge_request(
            source_branch=branch,
            target_branch=target_branch,
            title=f"Update {pog} CVMFS correction tags",
            description=merge_request_description(pog, pog_plan),
        )
        pog_plan.merge_request = MergeRequestReport(
            branch=branch,
            commit_id=commit_id,
            merge_request_url=mr_url,
            status="created_or_updated",
        )


def write_local_changes(plan: UpdatePlan) -> None:
    for path, content in sorted(plan.combined_changed_files.items()):
        Path(path).write_text(content)


def write_report(plan: UpdatePlan, report_path: Optional[str]) -> None:
    if report_path is None:
        return
    Path(report_path).write_text(json.dumps(plan.to_dict(), indent=2, sort_keys=True) + "\n")


def _parameter_files_from_glob(pattern: str) -> List[Path]:
    files = [Path(path) for path in sorted(glob.glob(pattern))]
    if not files:
        raise CorrectionUpdateError(f"No parameter files matched glob: {pattern}")
    return files


def _gitlab_client_from_environment(token_env: str) -> GitLabClient:
    token = os.environ.get(token_env)
    if not token:
        raise CorrectionUpdateError(f"Missing required GitLab token variable: {token_env}")

    api_v4_url = os.environ.get("CI_API_V4_URL", "https://gitlab.cern.ch/api/v4")
    project_id = os.environ.get("CI_PROJECT_ID")
    if not project_id:
        raise CorrectionUpdateError("Missing required GitLab variable: CI_PROJECT_ID")

    return GitLabClient(api_v4_url=api_v4_url, project_id=project_id, token=token)


def print_summary(plan: UpdatePlan) -> None:
    for pog, pog_plan in sorted(plan.pogs.items()):
        updated = sum(1 for report in pog_plan.reports if report.status == "updated")
        current = sum(1 for report in pog_plan.reports if report.status == "current")
        skipped = sum(1 for report in pog_plan.reports if report.status.startswith("skipped"))
        ahead = sum(1 for report in pog_plan.reports if report.status == "ahead")
        print(
            f"{pog}: {updated} updated, {current} current, {ahead} ahead, "
            f"{skipped} skipped, {len(pog_plan.changed_files)} changed file(s)"
        )
        if pog_plan.merge_request is not None and pog_plan.merge_request.merge_request_url:
            print(f"{pog}: merge request {pog_plan.merge_request.merge_request_url}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Update date-versioned CVMFS correction tags in parameter YAML files."
    )
    parser.add_argument(
        "--pogs",
        default="all",
        help="POG selection: 'all', one POG such as 'JME', or a comma-separated list.",
    )
    parser.add_argument(
        "--base-path",
        default=DEFAULT_BASE_PATH,
        help="Base CVMFS metadata path.",
    )
    parser.add_argument(
        "--parameters-glob",
        default=DEFAULT_PARAMETERS_GLOB,
        help="Glob selecting parameter YAML files to inspect.",
    )
    parser.add_argument(
        "--report",
        default=None,
        help="Optional JSON report output path.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Inspect and report without writing local files or calling the GitLab API.",
    )
    parser.add_argument(
        "--create-merge-requests",
        action="store_true",
        help="Commit updates to per-POG branches and create or reuse merge requests.",
    )
    parser.add_argument(
        "--token-env",
        default="CORRECTIONS_UPDATE_TOKEN",
        help="Environment variable containing a GitLab project access token.",
    )
    parser.add_argument(
        "--target-branch",
        default=os.environ.get("CI_DEFAULT_BRANCH", "main"),
        help="Target branch for merge requests.",
    )
    parser.add_argument(
        "--branch-prefix",
        default=DEFAULT_BRANCH_PREFIX,
        help="Prefix for generated per-POG update branches.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        parameter_files = _parameter_files_from_glob(args.parameters_glob)
        plan = build_update_plan(
            parameter_files=parameter_files,
            requested_pogs=args.pogs,
            base_path=args.base_path,
        )

        if plan.errors:
            write_report(plan, args.report)
            for error in plan.errors:
                print(error, file=sys.stderr)
            return 2

        if args.dry_run:
            pass
        elif args.create_merge_requests:
            client = _gitlab_client_from_environment(args.token_env)
            create_merge_requests(
                plan=plan,
                client=client,
                target_branch=args.target_branch,
                branch_prefix=args.branch_prefix,
            )
        else:
            write_local_changes(plan)

        write_report(plan, args.report)
        print_summary(plan)
        return 0
    except CorrectionUpdateError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
