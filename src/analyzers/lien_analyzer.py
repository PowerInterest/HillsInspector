from typing import List, Dict
from datetime import date
from src.models.property import Lien

class LienAnalyzer:
    def analyze_liens(self, liens: List[Lien], foreclosure_lis_pendens_date: date) -> Dict[str, List[Lien]]:
        """
        Analyzes liens to determine which survive the foreclosure.
        Assumes the foreclosure is based on a Mortgage.
        """
        surviving = []
        wiped_out = []
        
        for lien in liens:
            # Rule 1: Tax Liens and Municipal Liens often survive
            if "TAX" in lien.document_type.upper() or "MUNICIPAL" in lien.document_type.upper():
                surviving.append(lien)
                continue
                
            # Rule 2: Priority by Date
            # If recorded BEFORE the Lis Pendens of the foreclosure, it survives.
            if lien.recording_date < foreclosure_lis_pendens_date:
                surviving.append(lien)
            else:
                # Recorded AFTER Lis Pendens
                # Generally wiped out, assuming proper service.
                wiped_out.append(lien)
                
        return {
            "surviving": surviving,
            "wiped_out": wiped_out
        }
