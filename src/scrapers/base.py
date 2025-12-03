import logging
from typing import Optional, Callable, TypeVar, Any
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError
from src.models import ScrapeStatus, ScrapeResult
import traceback
import os

logger = logging.getLogger(__name__)

T = TypeVar("T")

class BaseScraper:
    def __init__(self, source_name: str, headless: bool = True):
        self.source_name = source_name
        self.headless = headless

    def handle_errors(self, func: Callable[..., ScrapeResult]) -> Callable[..., ScrapeResult]:
        """
        Decorator to handle common Playwright errors and return a standardized ScrapeResult.
        """
        def wrapper(*args, **kwargs) -> ScrapeResult:
            try:
                return func(*args, **kwargs)
            except PlaywrightTimeoutError:
                logger.error(f"Timeout scraping {self.source_name}")
                return ScrapeResult(
                    status=ScrapeStatus.NETWORK_ERROR,
                    source_name=self.source_name,
                    message="Operation timed out",
                    error_details=traceback.format_exc()
                )
            except PlaywrightError as e:
                error_msg = str(e)
                status = ScrapeStatus.UNKNOWN_ERROR

                # Simple heuristics for detection
                if "403" in error_msg or "Access Denied" in error_msg:
                    status = ScrapeStatus.BLOCKED
                elif "net::" in error_msg:
                    status = ScrapeStatus.NETWORK_ERROR

                logger.error(f"Playwright error scraping {self.source_name}: {error_msg}")
                return ScrapeResult(
                    status=status,
                    source_name=self.source_name,
                    message=f"Playwright Error: {error_msg}",
                    error_details=traceback.format_exc()
                )
            except Exception as e:
                logger.error(f"Unexpected error scraping {self.source_name}: {e}")
                return ScrapeResult(
                    status=ScrapeStatus.UNKNOWN_ERROR,
                    source_name=self.source_name,
                    message=f"Unexpected Error: {str(e)}",
                    error_details=traceback.format_exc()
                )
        return wrapper

    def capture_screenshot(self, page: Page, name_prefix: str = "error") -> Optional[str]:
        """Captures a screenshot for debugging."""
        try:
            timestamp = os.path.basename(os.getcwd()) # Just using something to differentiate, usually timestamp
            filename = f"screenshots/{self.source_name}_{name_prefix}.png"
            os.makedirs("screenshots", exist_ok=True)
            page.screenshot(path=filename)
            return filename
        except Exception as e:
            logger.warning(f"Failed to capture screenshot: {e}")
            return None
