from typing import Dict, Any

class InvestmentAnalyzer:
    def calculate_equity(self, market_value: float, owed_amount: float) -> float:
        """Calculate equity spread."""
        return market_value - owed_amount

    def analyze_property(self, property_data: Dict[str, Any], documents: list) -> Dict[str, Any]:
        """
        Analyze a property to determine if it's a good investment.
        This is a placeholder for more complex logic.
        """
        market_value = property_data.get("justValue", 0)
        # In a real scenario, 'owed_amount' would come from analyzing the foreclosure judgment PDF.
        # For now, we'll assume a placeholder debt.
        owed_amount = market_value * 0.5 
        
        equity = self.calculate_equity(market_value, owed_amount)
        
        return {
            "folio": property_data.get("folio"),
            "market_value": market_value,
            "estimated_debt": owed_amount,
            "equity": equity,
            "is_hot": equity > 50000
        }
