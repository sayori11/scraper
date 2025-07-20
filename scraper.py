import asyncio
import re
from typing import Optional
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

from base import BaseScraper


class PucciScraper(BaseScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = None
        self.current_state = {}
        self.pages_limit = 3  # Scrape 3 pages at a time

    def get_id_from_url(self, url: str) -> str:
        parsed = urlparse(url)
        clean = parsed.netloc + parsed.path
        clean = re.sub(r"[^\w\-\.]", "_", clean)
        return clean.strip("_")

    async def get_soup(self, url: str) -> Optional[object]:
        if not self.session:
            self.session = await self.create_session()
        response = await self.request(session=self.session, method="get", url=url)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    async def directory(
        self, tag: str, parent_data: dict, data: Optional[dict]
    ) -> dict:
        if tag == "collections":
            return {
                "state": {"page": 0},
                "rows": await self.extract_links(url=parent_data["url"], tag=tag),
            }
        elif tag == "products":
            return await self.extract_products(url=parent_data["url"], data=data)

    async def extract_links(self, url: str, tag: str) -> dict:
        soup = await self.get_soup(url)
        links = []
        link_urls = set()

        # Heuristic: Look for <a> tags in navigation menus
        for nav in soup.find_all(["nav", "ul", "div", "section"]):
            for link in nav.find_all("a", href=True):
                link_classes = link.get("class", [])  # list of class names
                text = link.get_text(strip=True)
                href = link["href"]

                if not text or len(text) < 2:
                    continue

                # Check for category-related keywords in href or text
                if (
                    any(tag in link_class.lower() for link_class in link_classes)
                    or tag in href.lower()
                    or tag in text.lower()
                ):

                    full_url = self.normalize_url(urljoin(self.url, href))

                    # Only internal links
                    if self._is_internal_link(full_url):
                        if full_url not in link_urls:
                            link_urls.add(full_url)
                            links.append(
                                {
                                    "id": self.get_id_from_url(href),
                                    "text": text,
                                    "url": full_url,
                                }
                            )

        return list(links)

    async def extract_products(self, url: str, data: Optional[dict] = None) -> dict:
        current_page = self.deepget(data, ["state", "page"], 0)
        current_products = self.deepget(data, ["rows"], [])
        current_page, next_url = self.get_next_page_url(
            current_url=url, current_page=current_page
        )
        seen_urls = set()
        product_links = set()

        page_count = 0

        while next_url and next_url not in seen_urls:
            seen_urls.add(next_url)
            print(next_url)
            new_products = set()

            products = await self.extract_links(url=next_url, tag="products")
            for product in products:
                if product["url"] not in product_links:
                    product_links.add(product["url"])
                    new_products.add(product["url"])
                    current_products.append(product)

            # If no new products found, break the loop
            if not new_products:
                print(f"No new products found on {next_url}. Stopping.")
                current_page -= 1
                break

            current_page, next_url = self.get_next_page_url(
                current_url=next_url, current_page=current_page
            )
            page_count += 1
            if page_count >= self.pages_limit:
                current_page -= 1
                break

        return {"state": {"page": current_page}, "rows": current_products}

    def _is_internal_link(self, url: str) -> bool:
        """
        Check if the URL is an internal link (same domain).
        """
        return urlparse(self.normalize_url(url)).netloc == urlparse(self.url).netloc

    def get_next_page_url(
        self, current_url: str, current_page: Optional[int] = None
    ) -> tuple[int, str]:
        parsed = urlparse(current_url)
        query_params = parse_qs(parsed.query)

        if current_page is None:
            page = query_params.get("page", ["0"])
            current_page = int(page[0])
        query_params["page"] = [str(current_page + 1)]

        # Rebuild the URL
        new_query = urlencode(query_params, doseq=True)
        new_url = urlunparse(parsed._replace(query=new_query))
        return current_page + 1, new_url

    async def detail(self, tag: str, parent_data: dict) -> dict:
        print(f"Scraping details for {parent_data['url']}")
        soup = await self.get_soup(parent_data["url"])
        title = soup.find("h1", id="product-title").get_text(strip=True)
        price = soup.find("span", class_="price-item").get_text(strip=True)
        return {"details": {"title": title, "price": price}}


async def main():
    scraper = PucciScraper(url="https://www.pucci.com")
    # await scraper.scrape_directory(tag="collections")
    # await scraper.scrape_directory(tag="products", parent_tag="collections")
    await scraper.scrape_detail(tag="product_details", parent_tag="products")


if __name__ == "__main__":
    asyncio.run(main())
