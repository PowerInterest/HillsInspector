import os
import base64
import json
import logging
from typing import Optional, Dict, Any, Union
from openai import OpenAI
from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger(__name__)

# --- Data Models for Validation ---

class PDFReadabilityStatus(BaseModel):
    is_readable: bool
    error_message: Optional[str] = None

class TaxDeedDocument(BaseModel):
    readability: PDFReadabilityStatus
    instrument_number: Optional[str] = None
    book_page: Optional[str] = None
    recording_date: Optional[str] = None
    document_type: Optional[str] = None
    grantor: Optional[str] = None
    grantee: Optional[str] = None
    legal_description: Optional[str] = None
    amount: Optional[float] = None
    raw_text: Optional[str] = None

# --- Prompts ---

TAX_DEED_PROMPT = """
You are an expert AI Assistant specialized in processing legal documents, specifically Tax Deed PDFs.
Your task is to analyze the provided image of a document and extract key information into a structured JSON format.

Please extract the following fields:
- instrument_number (string)
- book_page (string, e.g., "1234/5678")
- recording_date (string, YYYY-MM-DD format if possible)
- document_type (string, e.g., "TAX DEED", "NOTICE OF SALE")
- grantor (string)
- grantee (string)
- legal_description (string)
- amount (number, the consideration amount or sale price)
- raw_text (string, a brief summary of the text content)

CRITICAL:
1. First, assess if the document is readable. If it is blurry, blank, or not a legal document, set "is_readable" to false and provide a reason.
2. If readable, set "is_readable" to true and extract the data.
3. Return ONLY valid JSON. Do not include markdown formatting (like ```json).

Expected JSON Structure:
{
  "readability": {
    "is_readable": true,
    "error_message": null
  },
  "instrument_number": "...",
  "book_page": "...",
  "recording_date": "...",
  "document_type": "...",
  "grantor": "...",
  "grantee": "...",
  "legal_description": "...",
  "amount": 100.00,
  "raw_text": "..."
}
"""

# --- Client ---

class QwenVisionClient:
    def __init__(self, base_url: str = "http://10.10.1.5:8000/v1", api_key: str = "EMPTY"):
        self.client = OpenAI(base_url=base_url, api_key=api_key)
        self.model = "Qwen/Qwen2-VL-72B-Instruct" # Adjust model name as per server config

    def encode_image(self, image_path: str) -> str:
        """Encodes a local image file to base64."""
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')

    def analyze_document(self, image_input: Union[str, bytes], prompt: str = TAX_DEED_PROMPT) -> TaxDeedDocument:
        """
        Analyzes a document image and returns a validated TaxDeedDocument object.
        image_input: Can be a file path (str) or base64 bytes.
        """

        # Prepare Base64 string
        if isinstance(image_input, str) and os.path.exists(image_input):
            base64_image = self.encode_image(image_input)
        elif isinstance(image_input, str):
            base64_image = image_input # Assume it's already base64 string
        else:
            raise ValueError("Invalid image input. Must be a file path.")

        try:
            logger.info("Sending request to Qwen3vl...")
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{base64_image}"
                                },
                            },
                        ],
                    }
                ],
                max_tokens=1024,
                temperature=0.1, # Low temperature for factual extraction
            )

            content = response.choices[0].message.content
            logger.debug(f"Raw AI Response: {content}")

            # Clean markdown if present
            if content.startswith("```json"):
                content = content.replace("```json", "").replace("```", "")

            # Parse JSON
            data = json.loads(content)

            # Validate with Pydantic
            result = TaxDeedDocument(**data)
            return result

        except json.JSONDecodeError:
            logger.error("Failed to parse JSON from AI response.")
            return TaxDeedDocument(
                readability=PDFReadabilityStatus(is_readable=False, error_message="AI returned invalid JSON")
            )
        except ValidationError as e:
            logger.error(f"Validation Error: {e}")
            return TaxDeedDocument(
                readability=PDFReadabilityStatus(is_readable=False, error_message=f"Schema validation failed: {str(e)}")
            )
        except Exception as e:
            logger.error(f"Error communicating with Qwen3vl: {e}")
            return TaxDeedDocument(
                readability=PDFReadabilityStatus(is_readable=False, error_message=f"System Error: {str(e)}")
            )
