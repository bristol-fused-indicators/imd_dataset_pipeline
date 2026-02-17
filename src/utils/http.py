import json
from pathlib import Path

import requests
from loguru import logger
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CHUNK_SIZE = 8192


def create_session(
    retries: int = 5,
    backoff: float = 2,
    status_forcelist: list[int] | None = None,
) -> requests.Session:
    status_forcelist = status_forcelist or [429, 500, 503]
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=status_forcelist,
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def cached_fetch(
    url: str,
    output_path: Path,
    session: requests.Session,
    force: bool = False,
) -> Path:
    if output_path.exists() and not force:
        logger.debug(f"cache hit: {output_path}")
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

    logger.info(f"saved {output_path}")
    return output_path


def cached_fetch_json(
    url: str,
    output_path: Path,
    session: requests.Session,
    force: bool = False,
    params: dict | None = None,
) -> Path:
    if output_path.exists() and not force:
        logger.debug(f"cache hit: {output_path}")
        return output_path

    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("fetching JSON", url=url, output_path=output_path)

    response = session.get(url, params=params)
    response.raise_for_status()

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(response.json(), f)

    logger.info(f"saved {output_path}")
    return output_path
