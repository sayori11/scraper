import json
import random
import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Hashable, Optional
from urllib.parse import urlparse

from curl_cffi.requests import AsyncSession


class BaseScraper(ABC):
    def __init__(self, url: str):
        self.url = self.normalize_url(url)
        self.dir_name = self.get_dir_name_for_url(self.url)
        self.make_dir()
        self.session = None

    def normalize_url(self, url: str) -> str:
        parsed = urlparse(url)

        netloc = parsed.netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]

        normalized = parsed._replace(scheme="https", netloc=netloc)
        return normalized.geturl()

    def get_dir_name_for_url(self, url: str) -> str:
        parsed = urlparse(url)
        clean = parsed.netloc + parsed.path
        clean = re.sub(r"[^\w\-\.]", "_", clean)
        return clean.strip("_")

    def make_dir(self):
        Path(self.dir_name).mkdir(exist_ok=True)

    def get_tag_dir(self, tag: str) -> Path:
        tag_dir = Path(self.dir_name) / tag
        return tag_dir

    def load_json(self, tag: str) -> Optional[dict]:
        tag_dir = self.get_tag_dir(tag)
        if not tag_dir.is_dir():
            return None

        result = {}
        for file_path in tag_dir.iterdir():
            if file_path.is_file() and file_path.suffix == ".json":
                try:
                    with file_path.open("r", encoding="utf-8") as f:
                        data = json.load(f)
                        result[file_path.stem] = data
                except (json.JSONDecodeError, OSError):
                    continue

        return result

    def write_json(self, tag: str, data: dict):
        tag_dir_path = self.get_tag_dir(tag)
        tag_dir_path.mkdir(exist_ok=True)
        for id, items in data.items():
            file_path = tag_dir_path / f"{id}.json"
            with open(file_path, "w") as file:
                json.dump(items, file, indent=4)

    def deepget(self, dct: dict, keys: list[Hashable], default=None):
        if not dct:
            return default

        for key in keys:
            try:
                dct = dct[key]
            except Exception:
                return default
        return dct

    def get_impersonate_browser(self) -> str:
        browsers = [
            "chrome99",
            "chrome100",
            "chrome101",
            "chrome104",
            "chrome107",
            "chrome110",
            "chrome116",
            "chrome119",
            "chrome120",
            "chrome123",
            "chrome124",
            "chrome131",
            "chrome133a",
            "edge99",
            "edge101",
            "safari15_3",
            "safari15_5",
            "safari17_0",
            "safari18_0",
            "firefox133",
        ]
        return random.choice(browsers)

    async def create_session(
        self, headers: dict = {}, timeout: int = 8, **kwargs
    ) -> AsyncSession:
        session_timeout = timeout * 1.5
        headers = {
            "Referer": "https://www.google.com/",
            **headers,
        }
        args = {
            "timeout": session_timeout,
            "headers": headers,
            "max_clients": 100,
            **kwargs,
            "impersonate": kwargs.get("impersonate") or self.get_impersonate_browser(),
        }
        return AsyncSession(
            **args,
        )

    async def request(
        self, session: AsyncSession, method: str, url: str, kwargs: dict = {}
    ):
        return await getattr(session, method)(url, **kwargs)

    async def extract_directory(
        self,
        tag: str,
        parent_data: Optional[dict] = None,
        data: Optional[dict] = None,
    ) -> dict:
        if not parent_data:
            return {
                "root": await self.directory(
                    tag=tag,
                    parent_data={"url": self.url},
                    data=data.get("root") if data else None,
                )
            }

        scraped_data = {}
        for _, category_data in parent_data.items():
            for category in category_data.get("rows", []):
                scraped_data[category["id"]] = await self.directory(
                    tag=tag,
                    parent_data=category,
                    data=data.get(category["id"]) if data else None,
                )

        return scraped_data

    async def extract_detail(self, tag: str, parent_tag_data: dict) -> dict:
        detail_data = {}
        for _, item_data in parent_tag_data.items():
            scraped_data = await self.detail(tag=tag, parent_data=item_data.get("rows", []))
            detail_data.update(scraped_data)
        return detail_data

    @abstractmethod
    async def directory(
        self, tag: str, parent_data: dict, data: Optional[dict] = None
    ) -> dict: ...

    @abstractmethod
    async def detail(self, tag: str, parent_data: list[dict]) -> dict: ...

    async def scrape_directory(
        self, tag: str, parent_tag: Optional[str] = None
    ) -> dict:
        tag_data = self.load_json(tag=tag)
        parent_tag_data = None
        if parent_tag:
            parent_tag_data = self.load_json(tag=parent_tag)
            if not parent_tag_data:
                raise ValueError(f"No data found for parent tag: {parent_tag}")

        scraped_data = await self.extract_directory(
            tag=tag, data=tag_data, parent_data=parent_tag_data
        )
        self.write_json(tag=tag, data=scraped_data)
        return scraped_data

    async def scrape_detail(self, tag: str, parent_tag: str) -> dict:
        data = self.load_json(tag=parent_tag)
        if not data:
            raise ValueError(f"No data found for tag: {parent_tag}")
        scraped_data = await self.extract_detail(tag=tag, parent_tag_data=data)
        self.write_json(tag=tag, data=scraped_data)
        return scraped_data
