### MD structure:

```
{
    file: {
        filepath__branch            # PK
        filepath
        branch
        timestamp_added,
        fs_timestamp_created        # as reported by os at that time
        filename,
        status: enum[active, untracked, removed]
        history: {
            filepath_branch         # FK
            file_filepath           #
            file_branch             #
            timestamp_created,      # when this entry was added
            fs_size,                # size as reported by os
            fs_date_modified,       # date modified as reported by os
            fs_inode,               # inode as reported by os
            n_total_lines,          # number of lines
            n_changed_lines,        # number of changed lines since the last refresh
            running_changed_lines,  # sum of all previous changed lines - offloads analytics
            file_hash,              # SHA256
        },
    }
}
```

-   md init: initialize Metadata folder

    -   holds a sqlite database called .metadata.db tracking the above mentioned structure
        -   sqlalchemy is used for db operations on the sqlite file
    -   creates an empty `delete` root folder to hold deleted files
    -   creates an empty `hashes` folder to hold copy of tracked files from previous refresh
        -   this one is used to hold copies of all of the tracked files
        -   each time the `hashes` is run, this gets replaced by current files
        -   used to compute differences between refreshes
    -   when initializing metadata, check whether any parent down to fs root is initialized as MD controlled
        folder, if yes, stop the process, we don't want to have multiple MD controlled folders at the same path
    -   check if this folder is controlled by git, if so, inform user about this fact as this can lead to
        inconistencies if metadata is initialized outside of the 'main' branch

-   always execute these commands as if they were run from the MD `root` folder

-   md touch:
    -   creates new file and adds the initial record to history
    -   creates a copy of that file in the refresh folder
    -   does nothing if the file already exists in MD
-   md add:
    -   adds file to metadata if it is not already there
    -   use os info to populate date_modified, and date_created
    -   creates a copy of that file in the refresh folder
    -   does nothing if the file is alrady tracked and in active state
    -   if the file is in untracked state, change it to tracked state and create a copy in `hashes` folder
-   md rm:
    -   creates a minimal subfoler structure in the `delete` folder
    -   creates a copy of the file and stores it in the corresponding place in the `delete` folder
    -   the name of the copied file is `<original_filename>__rm__timestamp__uuid.<original_ext>`
    -   remove file from fs and set status to `removed` in metadata
    -   remove the file from the `hashes` folder
-   md untrack:
    -   set status of a file to `untracked` but keeps it in file system
    -   remove the copy from the `hashes` folder
-   md mv:
    -   moves the file in the filesystem and updates the "key" value in the metadata
    -   if the destination file already exists
        -   copy of that file is created in the `delete` folder
        -   the name of the exising dest file is `<original_filename>__mv__timestamp__uuid.<original_ext>`
        -   metadata of the exising dest file is updated with the corresponding filepath
        -   the existing dest file is removed from the `hashes` folder
        -   metadata of the source file is updated with the corresponding filepath
        -   the copy of the source file is created in the `hashes` folder
-   md refresh:

    -   add new history entry for each active file, comuting all required values

-   md list: list tracked files

-   Synchronization - TBD
-   github integration - TBD

-   TODO:

    -   block multiple executions of file scanner at the same time
    -   parallelize the execution of file scanner, multiple source dirs can be scanned at the same time
    -   add docker build for MD
    -   crete build scripts
    -   add logging to MD
    -   test file stats are computed correctly
    -   test adding records to md database

-   DONE:
    -   test case which covers that `md init` creates target dir if it doesn't exist
