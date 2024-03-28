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
    -   --active same as `mdm list`
    -   --all/-a same as md list
    -   --removed list only removed files
    -   --untracked list only untracked files
-   md show <filename> show file history

-   Synchronization - TBD
-   github integration - TBD

-   TODO:

    -   block multiple executions of file scanner at the same time
    -   parallelize the execution of file scanner, multiple source dirs can be scanned at the same time
    -   add docker build for `mdm`
    -   crete build scripts
    -   add logging to `mdm`
    -   add support for custom metadata associated with the file in form of json document
        stored in the metadata database
        -   add option to list the metadata as well as fetch specific one
    -   test file stats are computed correctly
    -   test adding records to `mdm` database
    -   currently `mdm` and `file scanner` are different tools called separatelly, eventually `scanner` should be part of `mdm` avaialable via `scan` subcommand

    -   add support for `mdm` sub-repositories

        -   this will require new table `repository`
            -   it can also hold the version info that is currently stored in `version_info` table
        -   files will be tracked in the closest `mdm` repository on the path to the root
        -   if file is currently tracked in parent `mdm` database and new `mdm` subrepository is created
            -   mark the file as `SUBREPOSITORY_TRACKED`
                -   this only applies to files that would end up under the new subrepository, files that are outside of the new subrepository are unaffected
            -   copy `ACTIVE` and `UNTRACKED` file records from current `mdm` repository to subrepository
                -   files's metadata, hash file and history are moved to new subrepsitory
                -   all following operations on the file will be tracked by the nearest subrepostiory
                -   each `mdm` repository stores pointer to nearest parent `mdm` repositories in form of a filepath and uuid
            -   there should be an option that allows users to remove `mdm` subrepository which will synchronize the subrepository's records into parent `mdm` repository if parent repository still exits, if it doesn't exists then the data is simply removed, this requires confirmation in form of `-f/--force` flag
            -   add field to `history` table that records current repository's id to track movement of records between repositories
            -   add better error handling

    -   refactor paths to use `joinpath` instead of `/`
    -   add new table tracking summary statistics such as no. tracked files, no. lines, no. updated lines
        -   also include total no. files
        -   add command that can collect this data
    -   refactor Union types to use `|` instead
    -   add script for running test coverage
    -   add option to search based on custom attributes and values via `mdm list` once the custom file attributes
        are implemented
    -   add `commit` argument to functions that perform database operations so that they can optionally
        commit changes
    -   rename `mdm` to `mdm` (MetadataManager)
    -   add support for both global (repository-level) and file-level custom metadata and
        option to filter out records based on them
    -   add CHANGELOG.md file
    -   add `--debug` flag to control printing of trackeback to stderr
    -   check how exceptions are passed throughout the code and make necessary changes so that traceback is not lost
    -   implement `mdm refresh` which will create new `history` record for each `ACTIVE` record in `file` table
    -   add support for `rm --recursive/-r <directory>`
        -   this will remove all files tracked by `mdm`
        -   if the directory is empty after files were removed, remove the directory as
    -   update `rm` to not delete files that are not tracked by `mdm`
    -   add `--repository-path` option to `rm` so that it can be run from outside of `mdm` repository
    -   implement 2 Phase Commit when synchronizing parent `mdm` database with child `mdm` database
    -   add Mixins to the Sqlalchemy models implementing `__repr__` method
    -   add better "rollback" when synchronizing `mdm`s, preserve source `hash` files until transactions are done
    -   add test cases that cover `mdm init --load-from-parent-repository` flag
    -   refactor common hardcoded exit codes >=100 (magic ints) into constants
    -   refactor catching `SystemExit` to `CalledProcessError` in test cases
    -   look for options how to parallelize the test cases that run many subprocesses, ex `test_touch/test_source_repository`
    -   add `--name` option to `mdm init` that can be used to provide a repository name which will be stored in `repository` table
    -   add `mdm get global <key>` and `mdm set global <key> <value>` to get and set repository level metadata
    -   fix bug in `mdm init` when `--recreate` is used in a directory which is not `mdm` root
    -   change `repository_filepath` to `repository_path` in `RepositoryORM` model definition

    -   rename project to `file tracker`
    -   add functionality to package the `ft` repository including active/untracked file into a zip file, preserving the internal structure for backup purposes
        -   it should support encrypting the package with AES256 encryption
        -   it should support HTTP upload, zipped file should be splitted into chunks which can be uploaded in parallel
        -   set up docker HTTP server that can receive and reconstruct the package
        -   `ft` should have its own separate database that would track all all of the repositories and can package all of them automatically and perform backup

-   DONE:
    -   test case which covers that `mdm init` creates target dir if it doesn't exist
    -   add version tracking into app & into store it in db, create new table during,
        initliazation with app-specific data
        -   add version file, tracking version, commit_id & build_type
    -   refactor `intialize_working_dir` fixture into marker
    -   refactor logic that check for initilized `mdm` repository into decorator and make `mdm_root`
        available via `mdm_manager` attrubute
    -   refactor `subprocess.run` commands into `subprocess.check_output` in test cases where all we care
        about is that the operation was successful
    -   implement `mdm rm <FILE>`
        -   add `mdm rm --purge` to purge all removed records
    -   refactor `mdm rm --purge` into its own subcommand `mdm purge`
    -   refactor decorators to better handle arguments passthrough
    -   handle scenario when `mdm init` is issued from the directory where `mdm` has already been initalized
        -   do nothing by default
        -   use `--recreate` to remove current `mdm` repository and create a new one
    -   block `touch` command if it is not clear which `mdm` the file belongs to in case there are subrepositories and the cwd's repository doesn't match file's nearest subrepository
    -   BUG: figure out why when executing `mdm init --load-from-parent-repository`, the file records are not removed from the parent respository
    -   make `mdm list` available via `mdm ls`
    -   add `--all`, `--active`, `--removed`, `--untracked`, `--subrepository-tracked` flags to `mdm ls` to filter out based on file status, show only `active` files by default
    -   implement `mdm ls json-dump` which will dump the result into a json file provided to this option
    -   add `--force/-f` flag to `mdm ls json-dump` to create parent directories if they don't exist
    -   add test case for `mdm ls json-dump` testing `--debug` flag
    -   fix typo in `RepositoryORM` name
