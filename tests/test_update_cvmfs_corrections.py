import json

from pocket_coffea.scripts.update_cvmfs_corrections import (
    GitLabClient,
    branch_for_pog,
    build_update_plan,
    create_merge_requests,
    resolve_requested_pogs,
    write_local_changes,
    write_report,
)


def _touch_correction(base, pog, campaign, tag, filename, content="{}"):
    path = base / pog / campaign / tag
    path.mkdir(parents=True, exist_ok=True)
    (path / filename).write_text(content)
    return path / filename


def test_all_pogs_are_discovered_from_existing_references(tmp_path):
    cvmfs = tmp_path / "cvmfs"
    params = tmp_path / "params.yaml"
    params.write_text(
        "\n".join(
            [
                "jme: ${cvmfs:Campaign,JME,jetid.json.gz,2025-01-01}",
                "btv: ${cvmfs:Campaign,BTV,btagging.json.gz,2025-01-01}",
                "",
            ]
        )
    )
    _touch_correction(cvmfs, "JME", "Campaign", "2025-01-01", "jetid.json.gz")
    _touch_correction(cvmfs, "BTV", "Campaign", "2025-01-01", "btagging.json.gz")

    plan = build_update_plan([params], requested_pogs="all", base_path=cvmfs)

    assert plan.selected_pogs == ["BTV", "JME"]
    assert sorted(plan.pogs) == ["BTV", "JME"]
    assert not plan.errors


def test_requested_pogs_accept_single_and_comma_separated_subset():
    known = ["BTV", "EGM", "JME"]

    assert resolve_requested_pogs("jme", known) == ["JME"]
    assert resolve_requested_pogs("JME,BTV", known) == ["JME", "BTV"]


def test_updates_only_selected_pog_and_preserves_raw_yaml_text(tmp_path):
    cvmfs = tmp_path / "cvmfs"
    params = tmp_path / "params.yaml"
    original = "\n".join(
        [
            "jet_scale_factors:",
            "  # keep this comment",
            '  jet_id: "${cvmfs:Campaign,JME,jetid.json.gz,2025-01-01}"',
            "  btag: ${cvmfs:Campaign,BTV,btagging.json.gz,2025-01-01}",
            "",
        ]
    )
    params.write_text(original)
    _touch_correction(cvmfs, "JME", "Campaign", "2025-01-01", "jetid.json.gz")
    _touch_correction(cvmfs, "JME", "Campaign", "2026-02-03", "jetid.json.gz")
    _touch_correction(cvmfs, "BTV", "Campaign", "2026-02-03", "btagging.json.gz")
    (cvmfs / "JME" / "Campaign" / "newJERFormats").mkdir(parents=True)
    (cvmfs / "JME" / "Campaign" / "latest").mkdir(parents=True)

    plan = build_update_plan([params], requested_pogs="JME", base_path=cvmfs)

    updated = plan.pogs["JME"].changed_files[str(params)]
    assert '# keep this comment' in updated
    assert '"${cvmfs:Campaign,JME,jetid.json.gz,2026-02-03}"' in updated
    assert "${cvmfs:Campaign,BTV,btagging.json.gz,2025-01-01}" in updated
    assert "newJERFormats" not in updated
    assert not plan.errors


def test_latest_selection_is_per_file_not_per_campaign(tmp_path):
    cvmfs = tmp_path / "cvmfs"
    params = tmp_path / "params.yaml"
    params.write_text("jme: ${cvmfs:Campaign,JME,jetid.json.gz,2025-01-01}\n")
    _touch_correction(cvmfs, "JME", "Campaign", "2025-01-01", "jetid.json.gz")
    _touch_correction(cvmfs, "JME", "Campaign", "2026-01-01", "jetid.json.gz")
    _touch_correction(cvmfs, "JME", "Campaign", "2026-04-01", "different.json.gz")

    plan = build_update_plan([params], requested_pogs="JME", base_path=cvmfs)

    report = plan.pogs["JME"].reports[0]
    assert report.selected_tag == "2026-01-01"
    assert "2026-01-01" in plan.pogs["JME"].changed_files[str(params)]


def test_never_downgrades_when_current_tag_is_ahead(tmp_path):
    cvmfs = tmp_path / "cvmfs"
    params = tmp_path / "params.yaml"
    params.write_text("jme: ${cvmfs:Campaign,JME,jetid.json.gz,2026-05-01}\n")
    _touch_correction(cvmfs, "JME", "Campaign", "2026-04-01", "jetid.json.gz")

    plan = build_update_plan([params], requested_pogs="JME", base_path=cvmfs)

    assert plan.pogs["JME"].reports[0].status == "ahead"
    assert not plan.pogs["JME"].changed_files


def test_untagged_references_are_reported_but_not_pinned(tmp_path):
    cvmfs = tmp_path / "cvmfs"
    params = tmp_path / "params.yaml"
    params.write_text("jme: ${cvmfs:Campaign,JME,jetid.json.gz}\n")

    plan = build_update_plan([params], requested_pogs="JME", base_path=cvmfs)

    assert plan.pogs["JME"].reports[0].status == "skipped_untagged"
    assert not plan.pogs["JME"].changed_files
    assert not plan.errors


def test_missing_referenced_file_is_a_clear_error(tmp_path):
    cvmfs = tmp_path / "cvmfs"
    params = tmp_path / "params.yaml"
    params.write_text("jme: ${cvmfs:Campaign,JME,jetid.json.gz,2025-01-01}\n")
    (cvmfs / "JME" / "Campaign" / "2025-01-01").mkdir(parents=True)

    plan = build_update_plan([params], requested_pogs="JME", base_path=cvmfs)

    assert plan.errors
    assert "No date-versioned CVMFS directory contains jetid.json.gz" in plan.errors[0]
    assert plan.pogs["JME"].reports[0].status == "error"


def test_write_local_changes_applies_combined_all_pog_updates(tmp_path):
    cvmfs = tmp_path / "cvmfs"
    params = tmp_path / "params.yaml"
    params.write_text(
        "\n".join(
            [
                "jme: ${cvmfs:Campaign,JME,jetid.json.gz,2025-01-01}",
                "btv: ${cvmfs:Campaign,BTV,btagging.json.gz,2025-01-01}",
                "",
            ]
        )
    )
    _touch_correction(cvmfs, "JME", "Campaign", "2026-01-01", "jetid.json.gz")
    _touch_correction(cvmfs, "BTV", "Campaign", "2026-02-01", "btagging.json.gz")

    plan = build_update_plan([params], requested_pogs="all", base_path=cvmfs)
    write_local_changes(plan)

    text = params.read_text()
    assert "${cvmfs:Campaign,JME,jetid.json.gz,2026-01-01}" in text
    assert "${cvmfs:Campaign,BTV,btagging.json.gz,2026-02-01}" in text


class FakeGitLabClient:
    def __init__(self):
        self.commits = []
        self.merge_requests = []

    def commit_files(self, branch, start_branch, files, commit_message):
        self.commits.append(
            {
                "branch": branch,
                "start_branch": start_branch,
                "files": files,
                "commit_message": commit_message,
            }
        )
        return f"commit-{branch}"

    def ensure_merge_request(self, source_branch, target_branch, title, description):
        self.merge_requests.append(
            {
                "source_branch": source_branch,
                "target_branch": target_branch,
                "title": title,
                "description": description,
            }
        )
        return f"https://gitlab.example/{source_branch}"


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, text=None):
        self.status_code = status_code
        self._json_data = json_data
        self.text = json.dumps(json_data) if json_data is not None else (text or "")

    def json(self):
        return self._json_data


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.headers = {}

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


def test_create_merge_requests_uses_one_branch_and_mr_per_changed_pog(tmp_path):
    cvmfs = tmp_path / "cvmfs"
    params = tmp_path / "params.yaml"
    params.write_text(
        "\n".join(
            [
                "jme: ${cvmfs:Campaign,JME,jetid.json.gz,2025-01-01}",
                "btv: ${cvmfs:Campaign,BTV,btagging.json.gz,2025-01-01}",
                "",
            ]
        )
    )
    _touch_correction(cvmfs, "JME", "Campaign", "2026-01-01", "jetid.json.gz")
    _touch_correction(cvmfs, "BTV", "Campaign", "2026-02-01", "btagging.json.gz")
    plan = build_update_plan([params], requested_pogs="all", base_path=cvmfs)
    client = FakeGitLabClient()

    create_merge_requests(plan, client, target_branch="main")

    assert [commit["branch"] for commit in client.commits] == [
        branch_for_pog("BTV"),
        branch_for_pog("JME"),
    ]
    assert [mr["source_branch"] for mr in client.merge_requests] == [
        branch_for_pog("BTV"),
        branch_for_pog("JME"),
    ]
    assert plan.pogs["BTV"].merge_request.merge_request_url.endswith("btv")
    assert plan.pogs["JME"].merge_request.merge_request_url.endswith("jme")


def test_gitlab_client_reuses_existing_merge_request():
    client = GitLabClient("https://gitlab.example/api/v4", "group/project", "token")
    session = FakeSession(
        [
            FakeResponse(
                json_data=[
                    {"web_url": "https://gitlab.example/group/project/-/merge_requests/1"}
                ]
            )
        ]
    )
    client.session = session

    url = client.ensure_merge_request(
        source_branch="ci/cvmfs-corrections/jme",
        target_branch="main",
        title="Update JME CVMFS correction tags",
        description="description",
    )

    assert url == "https://gitlab.example/group/project/-/merge_requests/1"
    assert [call[0] for call in session.calls] == ["GET"]


def test_gitlab_client_skips_commit_when_branch_content_is_current():
    client = GitLabClient("https://gitlab.example/api/v4", 123, "token")
    session = FakeSession(
        [
            FakeResponse(json_data={"name": "ci/cvmfs-corrections/jme"}),
            FakeResponse(text="same content"),
        ]
    )
    client.session = session

    commit_id = client.commit_files(
        branch="ci/cvmfs-corrections/jme",
        start_branch="main",
        files={"pocket_coffea/parameters/jet_scale_factors.yaml": "same content"},
        commit_message="Update JME CVMFS correction tags",
    )

    assert commit_id is None
    assert [call[0] for call in session.calls] == ["GET", "GET"]


def test_report_records_changed_files_and_merge_request_state(tmp_path):
    cvmfs = tmp_path / "cvmfs"
    params = tmp_path / "params.yaml"
    report_path = tmp_path / "report.json"
    params.write_text("jme: ${cvmfs:Campaign,JME,jetid.json.gz,2025-01-01}\n")
    _touch_correction(cvmfs, "JME", "Campaign", "2026-01-01", "jetid.json.gz")
    plan = build_update_plan([params], requested_pogs="JME", base_path=cvmfs)
    create_merge_requests(plan, FakeGitLabClient(), target_branch="main")

    write_report(plan, str(report_path))

    report = json.loads(report_path.read_text())
    assert report["selected_pogs"] == ["JME"]
    assert report["pogs"]["JME"]["changed_files"] == [str(params)]
    assert report["pogs"]["JME"]["merge_request"]["status"] == "created_or_updated"
