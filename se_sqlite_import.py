#!/usr/bin/python3
###############################################################################
#
# Stack Exchange XML Dump to SQLite Importer
#
# Does what the name suggests, includng setting up virtual tables, triggers and
# indices to help speed along recall.
#
# Modified from:
#
#      https://meta.stackexchange.com/questions/28103/python-script-to-import-create-sqlite3-database-from-so-data-dump
#
#################################################################[ AJC 2019 ]##

# Required Python 3 modules
import sqlite3
import os
import xml.etree.cElementTree as etree
import logging

# This is the anatomy of our tables and the data dump.
ANATHOMY = {
  'badges': {
    'Id': 'INTEGER',
    'UserId': 'INTEGER',
    'Class': 'INTEGER',
    'Name': 'TEXT',
    'Date': 'DATETIME',
    'TagBased': 'BOOLEAN',
  },
  'comments': {
    'Id': 'INTEGER',
    'PostId': 'INTEGER',
    'Score': 'INTEGER',
    'Text': 'TEXT',
    'CreationDate': 'DATETIME',
    'UserId': 'INTEGER',
    'UserDisplayName': 'TEXT'
  },
  'posts': {
      'Id': 'INTEGER',
      'PostTypeId': 'INTEGER',  # 1: Question, 2: Answer
      'ParentId': 'INTEGER',  # (only present if PostTypeId is 2)
      'AcceptedAnswerId': 'INTEGER',  # (only present if PostTypeId is 1)
      'CreationDate': 'DATETIME',
      'Score': 'INTEGER',
      'ViewCount': 'INTEGER',
      'Body': 'TEXT',
      'OwnerUserId': 'INTEGER',  # (present only if user has not been deleted)
      'OwnerDisplayName': 'TEXT',
      'LastEditorUserId': 'INTEGER',
      'LastEditorDisplayName': 'TEXT',  # ="Rich B"
      'LastEditDate': 'DATETIME',  #="2009-03-05T22:28:34.823"
      'LastActivityDate': 'DATETIME',  #="2009-03-11T12:51:01.480"
      'CommunityOwnedDate': 'DATETIME',  #(present only if post is community wikied)
      'Title': 'TEXT',
      'Tags': 'TEXT',
      'AnswerCount': 'INTEGER',
      'CommentCount': 'INTEGER',
      'FavoriteCount': 'INTEGER',
      'ClosedDate': 'DATETIME'
  },
  'votes': {
      'Id': 'INTEGER',
      'PostId': 'INTEGER',
      'UserId': 'INTEGER',
      'VoteTypeId': 'INTEGER',
      # -   1: AcceptedByOriginator
      # -   2: UpMod
      # -   3: DownMod
      # -   4: Offensive
      # -   5: Favorite
      # -   6: Close
      # -   7: Reopen
      # -   8: BountyStart
      # -   9: BountyClose
      # -  10: Deletion
      # -  11: Undeletion
      # -  12: Spam
      # -  13: InformModerator
      'CreationDate': 'DATETIME',
      'BountyAmount': 'INTEGER'
  },

# This was redacted from my final database due to size constraints.

#  'posthistory': {
#      'Id': 'INTEGER',
#      'PostHistoryTypeId': 'INTEGER',
#      'PostId': 'INTEGER',
#      'RevisionGUID': 'TEXT',
#      'CreationDate': 'DATETIME',
#      'UserId': 'INTEGER',
#      'UserDisplayName': 'TEXT',
#      'Comment': 'TEXT',
#      'Text': 'TEXT'
#  },

  'postlinks': {
      'Id': 'INTEGER',
      'CreationDate': 'DATETIME',
      'PostId': 'INTEGER',
      'RelatedPostId': 'INTEGER',
      'PostLinkTypeId': 'INTEGER',
      'LinkTypeId': 'INTEGER'
  },
  'users': {
      'Id': 'INTEGER',
      'Reputation': 'INTEGER',
      'CreationDate': 'DATETIME',
      'DisplayName': 'TEXT',
      'LastAccessDate': 'DATETIME',
      'WebsiteUrl': 'TEXT',
      'Location': 'TEXT',
      'Age': 'INTEGER',
      'AboutMe': 'TEXT',
      'Views': 'INTEGER',
      'UpVotes': 'INTEGER',
      'DownVotes': 'INTEGER',
      'AccountId': 'INTEGER',
      'ProfileImageUrl': 'TEXT'
  },
  'tags': {
      'Id': 'INTEGER',
      'TagName': 'TEXT',
      'Count': 'INTEGER',
      'ExcerptPostId': 'INTEGER',
      'WikiPostId': 'INTEGER'
  }
}


def dump_files(file_names, anathomy,
  dump_path='.',
    dump_database_name='so-dump.db',
    create_query='CREATE TABLE IF NOT EXISTS {table} ({fields})',
    insert_query='INSERT INTO {table} ({columns}) VALUES ({values})',
    log_filename='so-parser.log'):

  logging.basicConfig(filename=os.path.join(dump_path, log_filename), level=logging.INFO)
  db = sqlite3.connect(os.path.join(dump_path, dump_database_name))
  for file in file_names:
      print("Opening {0}.xml".format(file))
      with open(os.path.join(dump_path, file + '.xml')) as xml_file:
          tree = etree.iterparse(xml_file)
          table_name = file.lower()

          # The Great Experiment

          sql_create = create_query.format(
              table=table_name,
              fields=", ".join(['{0} {1}'.format(name, type) for name, type in anathomy[table_name].items()]))
          print('Creating table {0}'.format(table_name))

          try:
              logging.info(sql_create)
              db.execute(sql_create)
          except Exception as e:
              logging.warning(e)

          # This is the only time we really need the virtual table and triggers.
          if file == "posts":
             
              sql_virtual1 = "CREATE VIRTUAL TABLE posts_search USING fts5(Body, Title, tokenize=porter);"
              sql_virtual2 = "CREATE TRIGGER after_posts_insert AFTER INSERT ON posts WHEN NEW.PostTypeId=1 BEGIN INSERT INTO posts_search (rowid,Body,Title) VALUES (new.Id,new.Body,new.Title); END;"
              sql_virtual3 = "CREATE TRIGGER after_posts_delete AFTER DELETE ON posts BEGIN DELETE FROM posts_search WHERE rowid = old.Id; END;"
              sql_virtual4 = "CREATE TRIGGER after_posts_update_body UPDATE OF Body ON posts BEGIN UPDATE posts_search SET Body = new.Body where rowid = old.Id; END;"
              sql_virtual5 = "CREATE TRIGGER after_posts_update_title UPDATE OF Title ON posts BEGIN UPDATE posts_search SET Title = new.Title where rowid = old.Id; END;"

              try:
                  logging.info(sql_virtual1)
                  db.execute(sql_virtual1)
                  logging.info(sql_virtual2)
                  db.execute(sql_virtual2)
                  logging.info(sql_virtual3)
                  db.execute(sql_virtual3)
                  logging.info(sql_virtual4)
                  db.execute(sql_virtual4)
                  logging.info(sql_virtual5)
                  db.execute(sql_virtual5)

              except Exception as e:
                  logging.warning(e)
                  print(e)
                  exit(-1)


          count = 0
          for events, row in tree:
              try:
                  if row.attrib.values():
                      logging.debug(row.attrib.keys())
                      query = insert_query.format(
                          table=table_name,
                          columns=', '.join(row.attrib.keys()),
                          values=('?, ' * len(row.attrib.keys()))[:-2])
                      vals = []
                      for key, val in row.attrib.items():
                          if anathomy[table_name][key] == 'INTEGER':
                              vals.append(int(val))
                          elif anathomy[table_name][key] == 'BOOLEAN':
                              vals.append(1 if val=="TRUE" else 0)
                          else:
                              vals.append(val)
                      db.execute(query, vals)

                      count += 1
                      if (count % 1000 == 0):
                          print("{}".format(count))

              except Exception as e:
                  logging.warning(e)
                  print("x", end="")
              finally:
                  row.clear()
          print("\n")
          db.commit()
          del (tree)

  sql_virtual1 = "CREATE INDEX post_ids ON posts (Id);"
  sql_virtual2 = "CREATE INDEX comments_postid ON comments (PostId);"
  sql_virtual3 = "CREATE INDEX parent_ids on posts (ParentId) WHERE ParentId IS NOT NULL;"

  try:
      print("Creating indices...")
      logging.info(sql_virtual1)
      db.execute(sql_virtual1)
      logging.info(sql_virtual2)
      db.execute(sql_virtual2)
      logging.info(sql_virtual3)
      db.execute(sql_virtual3)

  except Exception as e:
      logging.warning(e)
      print(e)
      exit(-1)
  

if __name__ == '__main__':
  dump_files(ANATHOMY.keys(), ANATHOMY)

