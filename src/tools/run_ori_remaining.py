"""Run ori_search + survival for any remaining unprocessed properties."""

from src.services.pg_pipeline_controller import PgPipelineController, ControllerSettings

settings = ControllerSettings(
    force_all=False,
    skip_hcpa=True,
    skip_clerk_bulk=True,
    skip_nal=True,
    skip_flr=True,
    skip_sunbiz_entity=True,
    skip_county_permits=True,
    skip_tampa_permits=True,
    skip_foreclosure_refresh=True,
    skip_final_refresh=False,
    skip_trust_accounts=True,
    skip_title_chain=True,
    skip_title_breaks=True,
    skip_auction_scrape=True,
    skip_judgment_extract=True,
    skip_identifier_recovery=True,
    skip_ori_search=False,
    skip_survival=False,
    skip_market_data=True,
)

controller = PgPipelineController(settings)
result = controller.run()

for s in result["steps"]:
    if s["status"] != "skipped":
        print(s["name"], s["status"], f"{s['elapsed_seconds']}s")

print("Failed steps:", result["failed_steps"])
