from __future__ import annotations


class JournalSupport:
    def __init__(self, dbfs):
        self.dbfs = dbfs

    def append_journal_event(self, cur, action, path=None, file_id=None, directory_id=None):
        current_uid, _ = self.dbfs.current_uid_gid()
        cur.execute(
            """
            INSERT INTO journal (id_user, id_directory, id_file, action, date_time)
            VALUES (%s, %s, %s, %s, NOW())
            """,
            (current_uid, directory_id, file_id, action if path is None else f"{action}:{path}"),
        )
