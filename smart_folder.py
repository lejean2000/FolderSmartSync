"""
Folder synchronization class.
"""
from __future__ import annotations
import os
import shutil
import sqlite3
import glob
import hashlib


class SmartFolder:

    MODE_SMART_MIRROR = 0
    MODE_MOVE = 1
    SQLITE_DB_PATH = 'file.info.db'

    def __init__(self, folder_location: str):
        if not os.path.isdir(folder_location):
            raise ValueError("Path does not exist: " + folder_location)

        self.sqlite_db = SmartFolder.SQLITE_DB_PATH
        self.location = folder_location
        self.sqlite_conn = sqlite3.connect(self.sqlite_db)
        self.sqlite_conn.row_factory = sqlite3.Row
        self._configure_sqlite()
        self.debug = True

    def set_debug_mode(self, mode: bool):
        """When Debug mode is set to True
        no file operations will be attempted.
        This is the default mode.

        Parameters
        ----------
        mode : bool
        """
        self.debug = mode

    def get_db_table_name(self) -> str:
        """The table name which will hold the folder information in sqlite

        Returns
        -------
        str
        """
        hsh = hashlib.new('md5')
        hsh.update(self.location.encode())
        return 'f' + hsh.hexdigest()

    def _configure_sqlite(self):
        cur = self.sqlite_conn.cursor()
        table_name = self.get_db_table_name()

        # get the count of tables with the name
        cur.execute('''
        SELECT count(name) FROM sqlite_master 
        WHERE type='table' AND name=? 
        ''', [table_name])

        # if the count is 1, then table exists
        if cur.fetchone()[0] == 1:
            print('Table exists.')
            self.sqlite_conn.execute("DROP TABLE " + table_name)

        self.sqlite_conn.execute('''
        CREATE TABLE IF NOT EXISTS ''' + table_name + ''' (
        path text not null,
        name text not null,
        size integer not null,
        mtime real
        )
        ''')

        u_index_name = 'idx_u_'+table_name
        query = 'CREATE UNIQUE INDEX ' + u_index_name
        query += ' ON ' + table_name + '(path,name)'
        self.sqlite_conn.execute(query)

        index_name = 'idx'+table_name
        query = 'CREATE INDEX ' + index_name
        query += ' ON ' + table_name + '(size,mtime)'
        self.sqlite_conn.execute(query)

        self.sqlite_conn.execute("PRAGMA auto_vacuum = 1")
        self.sqlite_conn.execute("PRAGMA case_sensitive_like = false")
        self.sqlite_conn.execute("PRAGMA encoding = 'UTF-8'")

        # commit the changes to db
        self.sqlite_conn.commit()

    def _get_renames(self, target: SmartFolder):
        """Get all files in target that need to be renamed
        """
        src_table = self.get_db_table_name()
        tgt_table = target.get_db_table_name()

        print("src_table=" + src_table)
        print("tgt_table=" + tgt_table)

        qry = '''
        select 'rename' as operation, 
        src.path as source_path, src.name as source_name, 
        tgt.path as target_path, tgt.name as target_name
        from '''+src_table+''' src, '''+tgt_table+''' tgt
        where src.size=tgt.size and src.mtime=tgt.mtime and src.path<>tgt.path
        '''
        cur = self.sqlite_conn.cursor()
        cur.execute(qry)
        for row in cur:
            if row['source_path'] == '.':
                target_path_new = os.path.join(target.location, row['source_name'])
            else:
                target_path_new = os.path.join(target.location, row['source_path'], row['source_name'])

            if row['target_path'] == '.':
                target_path_old = os.path.join(target.location, row['target_name'])
            else:
                target_path_old = os.path.join(target.location, row['target_path'], row['target_name'])

            print("Rename " + target_path_old)
            print("Into: " + target_path_new)
            if not self.debug:
                os.makedirs(os.path.dirname(target_path_new), exist_ok=True)
                shutil.move(target_path_old, target_path_new)

        cur.close()

    def _get_copies(self, target: SmartFolder):
        """Get all files in target that need to be copied
        """
        src_table = self.get_db_table_name()
        tgt_table = target.get_db_table_name()
        qry = '''
        select 'copy' as operation, 
        src.path as source_path, 
        src.name as source_name
        from '''+src_table+''' src
        where (src.size, src.mtime)
        not in (select size, mtime from '''+tgt_table+''')
        '''
        cur = self.sqlite_conn.cursor()
        cur.execute(qry)
        for row in cur:
            if row['source_path'] == '.':
                target_path = os.path.join(target.location, row['source_name'])
                source_path = os.path.join(self.location, row['source_name'])
            else:
                target_path = os.path.join(target.location, row['source_path'], row['source_name'])
                source_path = os.path.join(self.location, row['source_path'], row['source_name'])

            print("Copy " + source_path)
            print("Into: " + target_path)
            if not self.debug:
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                shutil.copy2(source_path, target_path)

        cur.close()

    def _get_deletes_target(self, target: SmartFolder):
        """Get all files in target that need to be deleted
        because they are missing in the source
        """
        src_table = self.get_db_table_name()
        tgt_table = target.get_db_table_name()

        qry = '''
        select 'delete' as operation, tgt.path as target_path, tgt.name as target_name
        from '''+tgt_table+''' tgt
        where (tgt.size, tgt.mtime)
        not in (select size, mtime from '''+src_table+''')
        '''
        cur = self.sqlite_conn.cursor()
        cur.execute(qry)
        for row in cur:
            if row['target_path'] == '.':
                target_path = os.path.join(self.location, row['target_path'])
            else:
                target_path = os.path.join(self.location, row['target_path'], row['target_name'])

            print("DEL " + target_path)
            if not self.debug:
                os.remove(target_path)

        cur.close()

    def _get_moves(self, target: SmartFolder):
        """This is for MODE_MOVE only. Gets all files and where they need to be moved.
        Already existing files are just deleted.

        Parameters
        ----------
        target : SmartFolder
        """
        src_table = self.get_db_table_name()
        tgt_table = target.get_db_table_name()
        qry = '''
        select 'move' as operation, 
        src.path as source_path, src.name as source_name
        from '''+src_table+''' src
        where (src.size, src.mtime)
        not in (select size, mtime from '''+tgt_table+''')
        '''
        cur = self.sqlite_conn.cursor()
        cur.execute(qry)
        for row in cur:
            if row['source_path'] == '.':
                target_path = os.path.join(target.location, row['source_name'])
                source_path = os.path.join(self.location, row['source_name'])
            else:
                target_path = os.path.join(target.location, row['source_path'], row['source_name'])
                source_path = os.path.join(self.location, row['source_path'], row['source_name'])

            print("Move " + source_path)
            print("Into: " + target_path)
            if not self.debug:
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                shutil.move(source_path, target_path)

        cur.close()

        qry = '''
        select 'del' as operation, 
        src.path as source_path, src.name as source_name
        from '''+src_table+''' src
        where (src.size, src.mtime)
        in (select size, mtime from '''+tgt_table+''')
        '''
        cur = self.sqlite_conn.cursor()
        cur.execute(qry)
        for row in cur:
            if row['source_path'] == '.':
                source_path = os.path.join(self.location, row['source_name'])
            else:
                source_path = os.path.join(self.location, row['source_path'], row['source_name'])

            print("DEL " + source_path)
            if not self.debug:
                os.remove(source_path)

        cur.close()

    def sync_to(self, target: SmartFolder, sync_mode: int):
        """
        :param target: The target SmartFolder
        :param sync_mode: Only SmartFolder.MODE_SMART_MIRROR and SmartFolder.MODE_MOVE
        :return: None
        """
        self.populate_db()
        target.populate_db()

        if sync_mode == self.MODE_SMART_MIRROR:
            self._get_renames(target)
            self._get_copies(target)
            self._get_deletes_target(target)

        if sync_mode == self.MODE_MOVE:
            self._get_moves(target)

    def populate_db(self):
        """Read the files and populate metadata into sqlite.
        """

        # Clean up database
        table_name = self.get_db_table_name()
        self.sqlite_conn.execute("DELETE FROM " + table_name)
        self.sqlite_conn.commit()

        files_array = []
        for filename in glob.iglob(os.path.join(self.location, '**', '*'), recursive=True):
            if os.path.isfile(filename):
                basename_file = os.path.basename(filename)
                basedir_file = os.path.relpath(os.path.dirname(filename), self.location)
                size = os.path.getsize(filename)
                mtime = os.path.getmtime(filename)
                files_array.append((basedir_file, basename_file, size, mtime))

        query = "INSERT INTO " + table_name
        query += "(path, name, size, mtime) VALUES(?, ?, ?, ?)"

        cur = self.sqlite_conn.cursor()
        cur.executemany(query, files_array)
        self.sqlite_conn.commit()

    def __del__(self):
        self.sqlite_conn.close()
