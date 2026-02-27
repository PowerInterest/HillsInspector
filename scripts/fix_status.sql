UPDATE status SET step_market_fetched = CURRENT_TIMESTAMP WHERE step_homeharvest_enriched IS NOT NULL;
