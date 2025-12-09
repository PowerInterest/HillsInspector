"""
Clean up old/duplicate pipeline run files from property directories.

This script:
1. Removes invalid property directories (PropertyAppraiser, MULTIPLEPARCEL)
2. Keeps only the latest vision output for each scraper type per property
3. Optionally removes temp_images directories
"""

import shutil
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from loguru import logger

# Directories that are invalid and should be completely removed
INVALID_DIRS = {
    "PropertyAppraiser",
    "Property Appraiser",
    "MULTIPLEPARCEL",
    "N/A",
    "none",
}


def cleanup_invalid_dirs(prop_dir: Path, dry_run: bool = True) -> int:
    """Remove directories with invalid property IDs."""
    removed = 0
    for invalid in INVALID_DIRS:
        invalid_path = prop_dir / invalid
        if invalid_path.exists():
            file_count = sum(1 for _ in invalid_path.rglob('*') if _.is_file())
            if dry_run:
                logger.info(f"[DRY RUN] Would remove: {invalid_path} ({file_count} files)")
            else:
                logger.info(f"Removing: {invalid_path} ({file_count} files)")
                shutil.rmtree(invalid_path)
            removed += file_count
    return removed


def cleanup_duplicate_vision(prop_dir: Path, dry_run: bool = True) -> int:
    """Keep only the latest vision output for each scraper type."""
    removed = 0

    for prop in prop_dir.iterdir():
        if not prop.is_dir() or prop.name in INVALID_DIRS:
            continue

        vision_dir = prop / 'vision'
        if not vision_dir.exists():
            continue

        # Group files by scraper type
        scrapers = defaultdict(list)
        for f in vision_dir.glob('*.json'):
            parts = f.stem.split('_')
            if len(parts) >= 3:
                # Extract date from filename (YYYYMMDD format)
                date_idx = next((i for i, p in enumerate(parts) if p.isdigit() and len(p) == 8), None)
                if date_idx is not None:
                    scraper = '_'.join(parts[:date_idx])
                    try:
                        # Parse timestamp for sorting
                        date_str = parts[date_idx]
                        time_str = parts[date_idx + 1] if date_idx + 1 < len(parts) and len(parts[date_idx + 1]) == 6 else "000000"
                        timestamp = datetime.strptime(f"{date_str}_{time_str}", "%Y%m%d_%H%M%S")
                        scrapers[scraper].append((timestamp, f))
                    except (ValueError, IndexError):
                        # Can't parse, keep the file
                        pass

        # For each scraper, keep only the latest file
        for scraper, files in scrapers.items():
            if len(files) <= 1:
                continue

            # Sort by timestamp, newest first
            files.sort(key=lambda x: x[0], reverse=True)

            # Keep the newest, remove the rest
            for timestamp, filepath in files[1:]:
                if dry_run:
                    logger.debug(f"[DRY RUN] Would remove: {filepath}")
                else:
                    logger.debug(f"Removing: {filepath}")
                    filepath.unlink()
                removed += 1

            if len(files) > 1:
                kept = files[0][1].name
                logger.info(f"{prop.name}/{scraper}: keeping {kept}, removed {len(files)-1} older files")

    return removed


def cleanup_temp_images(prop_dir: Path, dry_run: bool = True) -> int:
    """Remove temp_images directories."""
    removed = 0

    for temp_dir in prop_dir.rglob('temp_images'):
        if temp_dir.is_dir():
            file_count = sum(1 for _ in temp_dir.rglob('*') if _.is_file())
            if dry_run:
                logger.info(f"[DRY RUN] Would remove: {temp_dir} ({file_count} files)")
            else:
                logger.info(f"Removing: {temp_dir} ({file_count} files)")
                shutil.rmtree(temp_dir)
            removed += file_count

    return removed


def main(dry_run: bool = True):
    logger.info("=" * 60)
    logger.info(f"CLEANUP OLD PIPELINE FILES {'(DRY RUN)' if dry_run else ''}")
    logger.info("=" * 60)

    prop_dir = Path('data/properties')
    if not prop_dir.exists():
        logger.error("Property directory not found!")
        return

    # Count files before
    total_before = sum(1 for _ in prop_dir.rglob('*') if _.is_file())
    logger.info(f"Total files before cleanup: {total_before}")

    # Step 1: Remove invalid directories
    logger.info("\n--- Removing invalid directories ---")
    invalid_removed = cleanup_invalid_dirs(prop_dir, dry_run)

    # Step 2: Remove duplicate vision outputs
    logger.info("\n--- Removing duplicate vision outputs ---")
    duplicates_removed = cleanup_duplicate_vision(prop_dir, dry_run)

    # Step 3: Remove temp_images
    logger.info("\n--- Removing temp_images directories ---")
    temp_removed = cleanup_temp_images(prop_dir, dry_run)

    # Summary
    total_removed = invalid_removed + duplicates_removed + temp_removed
    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Invalid directories: {invalid_removed} files")
    logger.info(f"Duplicate vision outputs: {duplicates_removed} files")
    logger.info(f"Temp images: {temp_removed} files")
    logger.info(f"Total {'would be ' if dry_run else ''}removed: {total_removed} files")

    if dry_run:
        logger.info("\nRun with --execute to actually delete files")


if __name__ == "__main__":
    import sys
    dry_run = "--execute" not in sys.argv
    main(dry_run=dry_run)
