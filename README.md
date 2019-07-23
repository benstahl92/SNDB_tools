Upload of a set of tools originally written by I. Shivvers (see below, and ). Currently maintained by B. Stahl and T. Brink. 

A private ``credentials.py`` file is required.

The original README follows below...

This is a repository of all of the tools we in the Filippenko Group
 use to manage the SNDB.

-I.Shivvers, 2015


TO USE THIS LIBRARY, ADD THE FOLLOWING TO YOUR .bashrc:

    # using the anaconda python install
    export PATH="/home/anaconda/bin:$PATH"
    # including the SNDB python tools in our path
    export PYTHONPATH="$PYTHONPATH:/home/ishivvers/Documents/SNDB_tools"

TO IMPORT SPECTRA FROM A FOLDER:

    import add2db
    add2db.import_spec_from_folder( <path to folder> )

TO MOVE FILES FROM THE CURRENT FOLDER AND THEN IMPORT THEM:

    import add2db
    add2db.move_files()
