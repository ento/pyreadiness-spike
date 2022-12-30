import argparse
import dataclasses
import enum
import functools
import json
import sys
import urllib
from pathlib import Path
from packaging.version import Version, InvalidVersion
import typing as t

from wheel_filename import parse_wheel_filename, InvalidFilenameError, ParsedWheelFilename

Comparator: t.TypeAlias = t.Literal[">="] | t.Literal["=="]
ComparableVersion: t.TypeAlias = t.Tuple[Comparator, Version]

PYPI_URL = "https://pypi.org/pypi/{name}/json"

# BigQuery query for listing top projects
TOP_PROJECTS_QUERY = """
SELECT
  file.project,
  COUNT(*) AS total_downloads
FROM
  `bigquery-public-data.pypi.file_downloads`
WHERE
  DATE(timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND details.python LIKE '{major}.%'
GROUP BY
  file.project
ORDER BY
  total_downloads DESC
LIMIT
  360
"""

def cache_json(filename: str):
    def decorator(fetcher):
        @functools.wraps(fetcher)
        def wrapper(*args, **kwargs):
            saved = Path(filename)
            if saved.is_file():
                return json.loads(saved.read_text())
            data = fetcher(*args, **kwargs)
            saved.write_text(json.dumps(data, indent=2))
            return data
        return wrapper
    return decorator


class ReadyStatus(enum.IntEnum):
    # Order here determines how readiness statuses get combined
    yes = enum.auto()
    maybe = enum.auto()
    no = enum.auto()
    unknown = enum.auto()


def fetch_top_projects():
    # Using the client requires a credentials json for GCP.
    # You can go to GCP and query the public dataset manually and place the result
    # in top-projects.json instead. (I did it this way, so I'm actually not sure
    # if the shape of the data in the file matches what the client returns.)
    # https://console.cloud.google.com/marketplace/product/gcp-public-data-pypi/pypi
    try:
        from google.cloud import bigquery
    except ImportError:
        print(f"ImportError: Please run `pip install google-cloud-bigquery`", file=sys.stderr)
        sys.exit(1)

    bq_client = bigquery.Client()
    print("Fetching top projects", file=sys.stderr)
    projects = list(bq_client.query(TOP_PROJECTS_QUERY.format(major="3")).result())
    print("Fetching top projects complete", file=sys.stderr)
    return projects


@cache_json("top-projects.json")
def load_top_projects():
    return fetch_top_projects()


def fetch_project_meta(name):
    print(f"Fetching '{name}'", file=sys.stderr)
    response = urllib.request.urlopen(PYPI_URL.format(name=name))
    return json.loads(response.read())


@cache_json("top-metas.json")
def load_top_metas():
    projects = load_top_projects()
    metas = {}
    for project in projects:
        metas[project["project"]] = fetch_project_meta(project["project"])
    return metas


def list_unyanked_wheels(project_meta_files) -> t.Iterator[ParsedWheelFilename]:
    for file_meta in project_meta_files:
        if file_meta["yanked"]:
            continue
        if file_meta["packagetype"] != "bdist_wheel":
            continue
        try:
            yield parse_wheel_filename(file_meta["filename"])
        except InvalidFilenameError:
            print("warn: skpping wheel with invalid filename:", file_meta["filename"], file=sys.stderr)


@dataclasses.dataclass
class Wheel:
    name: str
    python_tags: t.Set[str]
    abi_tags: t.Set[str]


@dataclasses.dataclass
class PackageVersion:
    version: Version
    wheels: t.List[Wheel] = dataclasses.field(default_factory=list)

    def __lt__(self, other: "PackageVersion") -> bool:
        return self.version < other.version


def list_available_versions(project_meta) -> t.Iterator[PackageVersion]:
    for version_string, files in project_meta["releases"].items():
        try:
            version = Version(version_string)
        except InvalidVersion:
            print("warn: skpping version with invalid format", project_meta["info"]["name"], version_string, file=sys.stderr)
            continue
        if version.is_prerelease:
            continue
        wheels = []
        for wheel_name in list_unyanked_wheels(files):
            wheels.append(Wheel(name=str(wheel_name), python_tags=wheel_name.python_tags, abi_tags=wheel_name.abi_tags))
        if wheels:
            yield PackageVersion(version=Version(version_string), wheels=wheels)


def is_cpython_compatible(tag: str) -> bool:
    return tag.startswith("cp") or tag.startswith("py")


def parse_wheel_python_tag(tag: str) -> Version:
    version_part = tag[2:]
    major_version = version_part[0]
    minor_version = version_part[1:]
    return Version(".".join((major_version, minor_version)) if minor_version else major_version)


def any_matches(version_constraints: t.Set[ComparableVersion], version: Version) -> bool:
    for comparator, base_version in version_constraints:
        match comparator:
            case ">=":
                if version >= base_version:
                    return True
            case "==":
                if version == base_version:
                    return True
    return False


def get_support_status_based_on_wheel_version(python_version: Version, package_versions: t.Sequence[PackageVersion]) -> ReadyStatus:
    if not package_versions:
        return ReadyStatus.unknown

    latest_version = package_versions[-1]
    python_version_constraints = set()
    for wheel in latest_version.wheels:
        # abi3 signals that the package adheres to a minimum set of instructions and
        # is forward-compatible
        comparator = ">=" if "abi3" in wheel.abi_tags else "=="
        for python_tag in wheel.python_tags:
            if not is_cpython_compatible(python_tag):
                continue
            try:
                for_python_version = parse_wheel_python_tag(python_tag)
                python_version_constraints.add((comparator, for_python_version))
            except InvalidVersion:
                print("warn: ignoring invalid python version tag", python_tag, "from wheel", wheel.name, file=sys.stderr)

    if any_matches(python_version_constraints, python_version):
        return ReadyStatus.yes

    minor_version_constraints = [pair for pair in python_version_constraints if len(pair[1].release) > 1]
    for previous_minor_version in range(python_version.minor - 1, -1, -1):
        previous_python_version = Version(f"{python_version.major}.{previous_minor_version}")
        if any_matches(minor_version_constraints, previous_python_version):
            return ReadyStatus.no

    if any(map(lambda x: x[1].major == python_version.major, python_version_constraints)):
        return ReadyStatus.maybe

    return ReadyStatus.unknown


def trove_classifier_string(python_version: str) -> str:
    return f"Programming Language :: Python :: {python_version}"


def get_support_status_based_on_classifier(python_version: Version, classifiers: t.Sequence[str]) -> ReadyStatus:
    # Is the exact version in the classifiers?
    if trove_classifier_string(str(python_version)) in classifiers:
        return ReadyStatus.yes

    # If this package lists Python versions at the granularity of minor versions,
    # and yet the exact version was not in its classifiers (see the conditional above),
    # it might mean this particular Python version is not supported.
    minor_classifier = trove_classifier_string(f"{python_version.major}.")
    for classifier in classifiers:
        if classifier.startswith(minor_classifier):
            return ReadyStatus.no

    if trove_classifier_string("3") in classifiers:
        return ReadyStatus.maybe

    return ReadyStatus.unknown


def readiness_statuses_of_top_projects(python_version: Version):
    metas = load_top_metas()

    output = []
    for project_name, project_meta in metas.items():
        versions = list(sorted(list_available_versions(project_meta)))
        classifiers = project_meta["info"]["classifiers"]

        version_status = get_support_status_based_on_wheel_version(python_version, versions)
        classifier_status = get_support_status_based_on_classifier(python_version, classifiers)
        combined_status = min(version_status, classifier_status)

        latest_release = versions[-1] if versions else None
        previous_release = versions[-2] if len(versions) > 1 else None
        classifier_versions = [s.lstrip(trove_classifier_string("")) for s in classifiers if s.startswith(trove_classifier_string("3")) and not s.endswith(":: Only")]

        project_data = dict(
            project=project_name,
            latest_version=str(latest_release.version) if latest_release else None,
            latest_wheels=[dataclasses.asdict(wheel) for wheel in latest_release.wheels] if latest_release else [],
            previous_version=str(previous_release.version) if previous_release else None,
            previous_wheels=[dataclasses.asdict(wheel) for wheel in previous_release.wheels] if previous_release else [],
            classifier_versions=classifier_versions,
            version_readiness=version_status.name,
            classifier_readiness=classifier_status.name,
            combined_readiness=combined_status.name,
        )
        output.append(project_data)

    print(json.dumps(output))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("python_version", type=Version)
    args = parser.parse_args()
    readiness_statuses_of_top_projects(args.python_version)
