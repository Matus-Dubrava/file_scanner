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
    -   creates an empty `hashes` folder to hold copy of tracked files from previous refresh
        -   this one is used to hold copies of all of the tracked files
        -   each time the `hashes` is run, this gets replaced by current files
        -   used to compute differences between refreshes
    -   when initializing metadata, check whether any parent down to fs root is initialized as MD controlled
        folder, if yes, stop the process, we don't want to have multiple MD controlled folders at the same path
    -   check if this folder is controlled by git, if so, inform user about this fact as this can lead to
        inconistencies if metadata is initialized outside of the 'main' branch

-   md touch:
    -   purpose of this command is to collect current file stats and store them in database
    -   it automatically creates a tracked file if one doesn't exists yet or starts tracking an existing file
    -   if file doesn't exist yet:
        -   create new file
        -   add record to `file` table
        -   add record to `history` table
        -   creates empty `hash` file
    -   if file exists:
        -   update record in `file` table if necessary
        -   add record to `history` table
        -   write line hashes to `hash` file (create if it doesn't exist yet/overwrite its contents otherwise)
-   md add:
    -   adds file to metadata if it is not already there
    -   use os info to populate date_modified, and date_created
    -   creates a copy of that file in the refresh folder
    -   does nothing if the file is alrady tracked and in active state
    -   if the file is in untracked state, change it to tracked state and create a copy in `hashes` folder
-   md rm:
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
    -   --active same as `md list`
    -   --all/-a same as md list
    -   --removed list only removed files
    -   --untracked list only untracked files
-   md show <filename> show file history

-   Synchronization - TBD
-   github integration - TBD

-   TODO:

    -   block multiple executions of file scanner at the same time
    -   parallelize the execution of file scanner, multiple source dirs can be scanned at the same time
    -   add docker build for MD
    -   crete build scripts
    -   add logging to MD
    -   add support for custom metadata associated with the file in form of json document
        stored in the metadata database
        -   add option to list the metadata as well as fetch specific one
    -   test file stats are computed correctly
    -   test adding records to md database
    -   ensure that md init/add/mv cannot be applied on files under .md directory itself
    -   add support for .md sub-repositories
        -   files will be tracked in the closest .md repository on the path to the root
    -   add `commit` argument to functions that perform database operations so that they can optionally
        changes
    -   refactor paths to use .joinpath instead of `/`
    -   refactor `subprocess.run` commands into `subprocess.check_output` in test cases where all we care
        about is that the operation was successful
    -   add `md --purge` to purge all removed records
    -   add new table tracking summary statistics such as no. tracked files, no. lines, no. updated lines
        -   also include total no. files
        -   add command that can collect this data
    -   refactor Union types to use `|` instead
    -   add script for running test coverage
    -   refactor logic that check for initilized `md` repository into decorator and make `md_root`
        available via `md_manager` attrubute

-   DONE:
    -   test case which covers that `md init` creates target dir if it doesn't exist
    -   add version tracking into app & into store it in db, create new table during,
        initliazation with app-specific data
        -   add version file, tracking version, commit_id & build_type
