import tempfile
from pathlib import Path

from google.cloud.storage import Client as GcsClient
from google.cloud.storage.bucket import Bucket
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

from lib.optn import download_waitlist_report, process_waitlist_report
from lib.Report import Report, ReportKind, ReportStatus
from lib.util import Environment, config, getLogger

logger = getLogger(__name__)


def now():
    from datetime import datetime, timedelta, timezone

    return datetime.now(timezone(timedelta(hours=-5), "EST"))


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=15),
    stop=stop_after_attempt(5),
)
def upload(bucket: Bucket, remote_path: str, local_path: str):
    bucket.blob(remote_path).upload_from_filename(local_path)


def main(Env: Environment):

    bucket = GcsClient().get_bucket(config.gcs_bucket)
    dt = now()

    raw = Report(ReportKind.WAITLIST, ReportStatus.RAW, dt, env=Env)
    processed = Report(ReportKind.WAITLIST, ReportStatus.PROCESSED, dt, env=Env)

    with tempfile.TemporaryDirectory() as temp_dir:

        logger.info(f"Downloading raw waitlist report to {temp_dir}")
        local_path = download_waitlist_report(Path(temp_dir))
        logger.info(f"Uploading raw waitlist report to {raw.remote_path}")
        upload(bucket, raw.remote_path, str(local_path))

        logger.info(f"Processing waitlist report")
        processed_df = process_waitlist_report(local_path, dt)
        processed_path = tempfile.mkstemp(dir=temp_dir, suffix=".parquet")[1]
        logger.info(f"Writing processed waitlist report to {processed_path}")
        processed_df.to_parquet(processed_path)

        logger.info(f"Uploading processed waitlist report to {processed.remote_path}")
        upload(bucket, processed.remote_path, processed_path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--prod",
        action="store_true",
        help="Run in production mode",
    )
    args = parser.parse_args()

    env = Environment.PROD if args.prod else Environment.DEV

    main(env)
