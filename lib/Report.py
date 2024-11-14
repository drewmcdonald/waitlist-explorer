import enum
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import ClassVar, Iterator, Optional

import pandas as pd
from google.cloud.storage import Bucket, Client

from lib.util import Environment, config, getLogger

logger = getLogger(__name__)


class ReportKind(enum.Enum):
    WAITLIST = "waitlist"


class ReportStatus(enum.Enum):
    RAW = "raw"
    PROCESSED = "processed"

    @property
    def extension(self) -> str:
        return {
            ReportStatus.RAW: "csv",
            ReportStatus.PROCESSED: "parquet",
        }[self]


@dataclass(frozen=True)
class Report:
    kind: ReportKind
    status: ReportStatus
    datetime_retrieved: datetime

    env: Environment = config.env
    date_format: ClassVar[str] = "%Y%m%d%H%M%S"

    @property
    def filename(self) -> str:
        kind, status = self.kind.value, self.status.value
        dt = self.datetime_retrieved.strftime(self.date_format)
        return f"{kind}-{status}-{dt}.{self.status.extension}"

    @property
    def remote_path(self) -> str:
        d = self.datetime_retrieved.date().isoformat()
        return f"{self.env.value}/{d}/{self.filename}"

    @classmethod
    def from_remote_path(cls, remote_path: str) -> "Report":
        kind, status, dtstr = Path(remote_path).name.split('.')[0].split("-")
        dt = datetime.strptime(dtstr, cls.date_format)
        return cls(ReportKind(kind), ReportStatus(status), dt)

    def download(self, bucket: Bucket, local_path: Path) -> Path:
        remote_path = self.remote_path
        logger.info(f"Downloading {remote_path} to {local_path}")
        bucket.blob(remote_path).download_to_filename(str(local_path))
        return local_path


@dataclass
class ReportCollection:
    client: Client
    bucket: Bucket

    _reports: dict[tuple[ReportKind, ReportStatus, Optional[date]], list[Report]] = (
        field(default_factory=dict)
    )

    def reports(
        self,
        kind: ReportKind = ReportKind.WAITLIST,
        status: ReportStatus = ReportStatus.PROCESSED,
        d: Optional[date] = None,
    ) -> list[Report]:
        key = (kind, status, d)
        if key not in self._reports:
            glob = "/".join(
                [
                    config.env.value,
                    d.isoformat() if d else "*",
                    f"{kind.value}-{status.value}-*.{status.extension}",
                ]
            )
            self._reports[key] = list(
                Report.from_remote_path(file.name)
                for file in self.client.list_blobs(self.bucket, match_glob=glob)
            )
        return self._reports[key]

    def find_latest_report(
        self,
        *,
        kind: ReportKind = ReportKind.WAITLIST,
        status: ReportStatus = ReportStatus.PROCESSED,
        d: Optional[date] = None,
    ) -> Report:
        reports = self.reports(kind, status, d)
        reports.sort(key=lambda r: r.datetime_retrieved, reverse=True)
        return reports[0]

    @contextmanager
    def download_latest_report(
        self,
        *,
        kind: ReportKind = ReportKind.WAITLIST,
        status: ReportStatus = ReportStatus.PROCESSED,
        d: Optional[date] = None,
    ) -> Iterator[Path]:
        report = self.find_latest_report(kind=kind, status=status, d=d)
        local_path = Path(tempfile.mkstemp(suffix=f".{status.extension}")[1])
        try:
            report.download(self.bucket, local_path)
            yield local_path
        finally:
            local_path.unlink(missing_ok=True)

    def get_processed_waitlist(self, d: Optional[date] = None) -> pd.DataFrame:
        with self.download_latest_report(
            kind=ReportKind.WAITLIST,
            status=ReportStatus.PROCESSED,
            d=d,
        ) as local_path:
            return pd.read_parquet(local_path)
