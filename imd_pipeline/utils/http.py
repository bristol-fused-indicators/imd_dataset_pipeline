import json
from pathlib import Path

import requests
from loguru import logger
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CHUNK_SIZE = 8192


def create_session(
    retries: int = 10,
    backoff: float = 2,
    status_forcelist: list[int] | None = None,
) -> requests.Session:
    """Creates a requests Session with retry logic.

    Args:
        retries: Maximum number of retries.
        backoff: Exponential backoff factor between retries.
        status_forcelist: HTTP status codes that trigger a retry.

    Returns:
        A configured requests Session.
    """
    status_forcelist = status_forcelist or [429, 500, 503, 504]
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=status_forcelist,
        respect_retry_after_header=True,
        allowed_methods={"GET", "POST", "HEAD"},
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


def cached_fetch(
    url: str,
    output_path: Path,
    session: requests.Session,
    force_refresh: bool = False,
) -> Path:
    """Downloads a file to disk, skipping the download if the file already exists.

    Args:
        url: URL to download from.
        output_path: Path to save the file to.
        session: requests Session to use.
        force_refresh: If True, re-download even if the file exists.

    Returns:
        Path to the downloaded file.
    """
    if output_path.exists() and not force_refresh:
        logger.debug("cache hit", path=output_path)
        return output_path

    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("downloading", url=url, output_path=output_path)

    response = session.get(url, stream=True)
    response.raise_for_status()

    tmp = output_path.with_suffix(output_path.suffix + ".tmp")
    try:
        with open(tmp, "wb") as f:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                f.write(chunk)
        tmp.rename(output_path)
    except Exception:  # todo handle actual exceptions gracefully
        tmp.unlink(missing_ok=True)
        raise

    logger.info("saved", path=output_path)
    return output_path


def cached_fetch_json(
    url: str,
    output_path: Path,
    session: requests.Session,
    force_refresh: bool = False,
    params: dict | None = None,
) -> Path:
    """Fetches JSON from a URL and saves it to disk, skipping if the file already exists.

    Args:
        url: URL to fetch from.
        output_path: Path to save the JSON file to.
        session: requests Session to use.
        force_refresh: If True, re-fetch even if the file exists.
        params: Optional query parameters to include in the request.

    Returns:
        Path to the saved JSON file.
    """
    if output_path.exists() and not force_refresh:
        logger.debug("cache hit", path=output_path)
        return output_path

    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("fetching JSON", url=url, output_path=output_path)

    response = session.get(url, params=params)
    response.raise_for_status()

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(response.json(), f)

    logger.info("saved", path=output_path)
    return output_path
