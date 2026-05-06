#!/usr/bin/env python3
import argparse
import os
import sqlite3
import sys
from dataclasses import dataclass
from typing import Dict, Iterable, List


@dataclass
class DeletionPlan:
    programme_id: int
    episode_ids: List[int]
    media_ids: List[int]
    counts: Dict[str, int]


def _build_plan(conn: sqlite3.Connection, programme_id: int) -> DeletionPlan:
    cursor = conn.cursor()

    episode_rows = cursor.execute(
        "SELECT id FROM episodes WHERE programme_id = ?",
        (programme_id,),
    ).fetchall()
    episode_ids = [row[0] for row in episode_rows]

    media_ids: List[int] = []
    if episode_ids:
        placeholders = ",".join("?" for _ in episode_ids)
        media_rows = cursor.execute(
            f"SELECT id FROM media WHERE episode_id IN ({placeholders})",
            tuple(episode_ids),
        ).fetchall()
        media_ids = [row[0] for row in media_rows]

    counts = {
        "programmes": cursor.execute(
            "SELECT COUNT(*) FROM programmes WHERE id = ?",
            (programme_id,),
        ).fetchone()[0],
        "programme_config": cursor.execute(
            "SELECT COUNT(*) FROM programme_config WHERE programme_id = ?",
            (programme_id,),
        ).fetchone()[0],
        "archive_stats": cursor.execute(
            "SELECT COUNT(*) FROM archive_stats WHERE programme_id = ?",
            (programme_id,),
        ).fetchone()[0],
        "episodes": len(episode_ids),
        "media": len(media_ids),
    }

    return DeletionPlan(
        programme_id=programme_id,
        episode_ids=episode_ids,
        media_ids=media_ids,
        counts=counts,
    )


def _delete_database_rows(conn: sqlite3.Connection, plan: DeletionPlan) -> None:
    with conn:
        if plan.episode_ids:
            placeholders = ",".join("?" for _ in plan.episode_ids)
            conn.execute(
                f"DELETE FROM media WHERE episode_id IN ({placeholders})",
                tuple(plan.episode_ids),
            )

        conn.execute(
            "DELETE FROM episodes WHERE programme_id = ?",
            (plan.programme_id,),
        )
        conn.execute(
            "DELETE FROM archive_stats WHERE programme_id = ?",
            (plan.programme_id,),
        )
        conn.execute(
            "DELETE FROM programme_config WHERE programme_id = ?",
            (plan.programme_id,),
        )
        conn.execute(
            "DELETE FROM programmes WHERE id = ?",
            (plan.programme_id,),
        )


def _delete_archive_files(
    archive_root: str,
    programme_id: int,
    media_ids: Iterable[int],
) -> List[str]:
    removed: List[str] = []

    image_path = os.path.join(archive_root, "images", f"{programme_id}.0.jpg")
    if os.path.exists(image_path):
        os.remove(image_path)
        removed.append(image_path)

    media_dir = os.path.join(archive_root, "media")
    for media_id in media_ids:
        media_path = os.path.join(media_dir, f"{media_id}.mp4")
        if os.path.exists(media_path):
            os.remove(media_path)
            removed.append(media_path)

    return removed


def _print_plan(plan: DeletionPlan) -> None:
    print(f"Deletion plan for programme_id={plan.programme_id}")
    print(f"  programmes: {plan.counts['programmes']}")
    print(f"  programme_config: {plan.counts['programme_config']}")
    print(f"  archive_stats: {plan.counts['archive_stats']}")
    print(f"  episodes: {plan.counts['episodes']}")
    print(f"  media: {plan.counts['media']}")



def _parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="delete_programme",
        description="Delete a programme and related rows from the ohdieux SQLite database.",
    )
    parser.add_argument(
        "programme_id",
        type=int,
        help="Programme ID to delete.",
    )
    parser.add_argument(
        "--db",
        default="data/ohdieux.db",
        help="Path to SQLite database file. Default: data/ohdieux.db",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply deletion. Without this flag, the script only prints a dry-run plan.",
    )
    parser.add_argument(
        "--delete-archive-files",
        action="store_true",
        help="Also remove archive files under --archive-root for this programme/media.",
    )
    parser.add_argument(
        "--archive-root",
        default="data/archive",
        help="Archive root folder containing images/ and media/. Default: data/archive",
    )
    return parser.parse_args(argv)



def main(argv: List[str]) -> int:
    args = _parse_args(argv)

    if not os.path.exists(args.db):
        print(f"Database file not found: {args.db}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(args.db)
    try:
        plan = _build_plan(conn, args.programme_id)
        _print_plan(plan)

        if not args.apply:
            print("Dry-run only. Re-run with --apply to execute deletion.")
            return 0

        _delete_database_rows(conn, plan)
        print("Database deletion completed.")

        if args.delete_archive_files:
            removed = _delete_archive_files(
                archive_root=args.archive_root,
                programme_id=plan.programme_id,
                media_ids=plan.media_ids,
            )
            print(f"Archive cleanup removed {len(removed)} file(s).")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
