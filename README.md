# StackExchange-CLI-Local

A CLI tool to query and display StackExchange Q&amp;A via a local SQLite database from XML dumps.

This tooling is very much in an 'alpha' state.  There are bad practices, poor design, and bugs abounding.  Caveat emptor, and my apologies.

## Files:

README.md - This file
se.pl     - The actual CLI utility
se_sqlite_import.py - a Python script from StackExchange, modified to do what's required.

## TODO:

   * Code and algorithmic cleanup,
   * Output simplification and cleanup,
   * Replacement of bad practices with good,
   * Restructure of database import to exclude less important information, or
   * incorporate unused database information into output for clarity

