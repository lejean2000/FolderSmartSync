###### Description

Smart Folder is a folder synchronization code which supports a synchronization mode which detects moved/renamed files in a folder tree and instead of synchronizing via delete from target + copy from source only moves the files on the target to match the source.

The MOVE mode just moves all files from a source tree to the target tree but without deleting the folders from the source.

###### Use Cases

An example use case for this is when you have a GB-sized collection of documents classified in dozens of folders and decide to reclassify them. 

Another use case is for collections of photos and videos where videos are renamed and the whole collection has to be re-synced to an external drive.

###### Use
    from smart_folder import SmartFolder
    
    # configure path to use for sqlite database
    SmartFolder.SQLITE_DB_PATH = 'file.info.db'

    source = SmartFolder("X:\Videos\")
    target = SmartFolder("Z:\Videos\")

    # debug mode only lists the actions without performing them
    # it must be disabled manually like this:
    # source.set_debug_mode(False)

    source.sync_to(target, SmartFolder.MODE_SMART_MIRROR)
    
    # or source.sync_to(target, SmartFolder.MODE_MOVE)
