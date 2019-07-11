#!/usr/bin/perl -w
#=[ Alpha ]====================================================================
#
# se.pl - Query an SQLite3 database imported from a StackExchange XML dump and
#         display the questions and answers in a human-readable format.
#
# This code should be considered in an 'alpha' state and is missing features,
# intelligent architecture and design, and probable bug fixes.  My apologies.
#
#===============================================================[ AJC 2019 ]===

#======================================
# Mandatory Modules
#====================================== 
use HTML::FormatText::WithLinks;
use List::MoreUtils qw(uniq);
use List::Flatten;
use Getopt::Long;
use Data::Dumper;
use DBD::SQLite;
use Text::Wrap;
use warnings;
use strict;
use Encode;
use POSIX;
use DBI;

#====================================== 
# Constants and Global Variables
#======================================
$Text::Wrap::unexpand = 0;
my $dbpath = "/home/josefcub/scripts/se-db/";
my $dbname = "stackoverflow";
my $dbext = ".db";

#====================================== 
# Initialization Code
#====================================== 

# 
# Process the command line appropriately.
#
my $allflag;
my $bodysearch;
my $help;
my $listflag;
my $nomungeflag;
my $questionsearch;
my $titlesearch;

GetOptions(

  'all|a'        => \$allflag,
  'body|b=s'     => \$bodysearch,
  'database|d=s' => \$dbname,
  'help|h'       => \$help,
  'list|l'       => \$listflag,
  'nomunge|n'    => \$nomungeflag,
  'question|q=s' => \$questionsearch,
  'title|s=s'    => \$titlesearch,
  
) || die printUsage();

printUsage() if defined $help;

# Get the database set up and connected.
my $dbh = DBI->connect(
	"dbi:SQLite:$dbpath$dbname$dbext",
	"",
	"",
	{ RaiseError => 1 },
) or die $DBI::errstr;

#====================================== 
# Useful Subroutines
#====================================== 

#####
#
# printUsage - Prints out a useful help message.
#
#  COMMAND-LINE ARGUMENTS
#    -h            prints out a usage summary
#
#####
sub printUsage {

  my $name = $0;

   print << "ENDUSAGE";

Usage: $name [options]

  --all		-a 		Print out all available results
  --body 	-b <string>	Searches the bodies of questions
  --database	-d <dbname>     Searches a specific database
  --list	-l 		Lists available SE databases
  --help	-h		Print this message
  --nomunge	-n		Doesn't alter or remove HTML
  --question	-q <ID>		return a question and answers
  --title	-t <string>	Searches only the titles of questions

ENDUSAGE

  exit(0)
}

######
#
# subhtml - Do bad practices to HTML.
#
#           This is neither the smart way, 
#           the Perl way, nor the right way
#           to handle this.
#
######
sub subhtml {

  my $data = shift;

  if (not $nomungeflag and $data) {

    # Handle specific tags
    $data =~ s/<LI>/\n\ \ \*\ /gsi;
    $data =~ s/<STRONG>/\*/gsi;
    $data =~ s/<\/STRONG>/\*/gsi;

    $data =~ s/<BLOCKQUOTE>/---------------------\n/gsi;
    $data =~ s/<\/BLOCKQUOTE>/---------------------\n/gsi;

    $data =~ s/<PRE><CODE>/---------------------\n/gsi;
    $data =~ s/<\/CODE><\/PRE>/---------------------\n/gsi;

    # Handle the rest of the HTML tags
    $data =~ s/<[^>]*>//gs;

    # Handle HTML entities
    $data =~ s/\&nbsp;//gsi;
    $data =~ s/\&lt;/</gsi;
    $data =~ s/\&gt;/>/gsi;

  }

  return($data);

}

######
#
# searchtitles - Search titles in database for specified string
#
######
sub searchtitles {

  my $search = shift;

  # Fetch our search results from the database
  my $stmt = $dbh->prepare("SELECT rowid,Title,Body from posts_search WHERE Title MATCH '$search';");
  $stmt->execute() or die $DBI::errstr;

  my @results = $stmt->fetchall_arrayref();
  my $retcount = $stmt->rows();

  $stmt->finish();

  # Search results empty
  if ($retcount == 0) {
    print "There were no results found for '$search'.\n";
    exit(-1);
  }

  # Search results header
  $Text::Wrap::columns = 71;
  print ("Number\t| Article Title\n");
  print "="x80 . "\n";

  # The results themselves.
  if ($allflag) {

    for (my $counter = 0;$counter < $retcount;$counter++) {

      if (not defined $results[0][$counter][0]) { next }

      # make sure my body is ready
      my @body = split("\n", $results[0][$counter][2]);

      push @body, [$1] while $body[0] =~ s/<[^>]*>//gs; 
      my $body = $body[0];

      # Pretty formatting.
      my $numbertabs = "\t\t";
      if ($results[0][$counter][0] > 9999999) {
        $numbertabs = "\t";
      }
      print $results[0][$counter][0] . "$numbertabs| " . wrap("","\t\t| ",$results[0][$counter][1]) . "\n";
      print "\t\t" . "-"x64 . "\n";
      print "\t\t| " . wrap("","\t\t| ", $body) . "\n";
      if ($counter < $retcount - 1) { print "-"x80 . "\n" }
    }

  } else {

    for (my $counter = 0;$counter < 10;$counter++) {
      
      # Pretty formatting.
      my $numbertabs = "\t\t";
      if (defined $results[0][$counter][0] and $results[0][$counter][0] > 9999999) {
        $numbertabs = "\t";
      }
      
      if (not defined $results[0][$counter][0]) { next }
      print $results[0][$counter][0] . "$numbertabs| " . wrap("","\t\t| ",$results[0][$counter][1]) . "\n";
      if ($counter < 9 and $counter < $retcount - 1) { print "-"x80 . "\n" }
    }

  }

  # Finish out the footer
  print "="x80 . "\n";
  if ($retcount < 10 or $allflag) {
    if ($retcount > 1) {
      print "\nAll $retcount results shown for '$search'.\n";
    } else {
      print "One result shown for '$search'.\n";
    }
  } else {
    print "\nShowing 10 results of $retcount maximum for '$search'.\n";
  }

}
  
######
#
# searchbodies - Search titles in database for specified string
#
######
sub searchbodies {

  my $search = shift;

  # Fetch our search results from the database
  my $stmt = $dbh->prepare("SELECT rowid,Title,Body from posts_search WHERE Body MATCH '$search';");
  $stmt->execute() or die $DBI::errstr;

  my @results = $stmt->fetchall_arrayref();
  my $retcount = $stmt->rows();

  $stmt->finish();
  $dbh->disconnect();

  # Search results empty
  if ($retcount == 0) {
    print "There were no results found for '$search'.\n";
    exit(-1);
  }

  # Search results header
  $Text::Wrap::columns = 71;
  print ("Number\t| Article Title and Match\n");
  print "="x80 . "\n";

  # The results themselves.
  if ($allflag) {

    for (my $counter = 0;$counter < $retcount;$counter++) {

      # make sure my body is redy
      my @body = split("\n", $results[0][$counter][2]);
      # munge search term for grep
      my @searchterms = join("|", split(" ", $search));

      # Pull out the first matching line and remove any HTML.
      my @foo = grep(/@searchterms/i, @body);
      push @foo, [$1] while $foo[0] =~ s/<[^>]*>//gs; 
      my $foo = $foo[0];

      # Pretty formatting.
      my $numbertabs = "\t\t";
      if ($results[0][$counter][0] > 9999999) {
        $numbertabs = "\t";
      }

      if (not defined $results[0][$counter][0]) { next }
      print $results[0][$counter][0] . "$numbertabs| " . wrap("","\t\t| ",$results[0][$counter][1]) . "\n";
      print "\t\t" . "-"x64 . "\n";
      print "\t\t| " . wrap("","\t\t| ", $foo) . "\n";
      if ($counter < $retcount - 1) { print "-"x80 . "\n" }
    }

  } else {

    for (my $counter = 0;$counter < 10;$counter++) {

      if (not defined $results[0][$counter][0]) { next }

      # make sure my body is redy
      my @body = split("\n", $results[0][$counter][2]);
      # munge search term for grep
      my @searchterms = join("|", split(" ", $search));

      # Pull out the first matching line and remove any HTML.
      my @foo = grep(/@searchterms/i, @body);
      if ($foo[0]) {
        push @foo, [$1] while $foo[0] =~ s/<[^>]*>//gs;
      } else {
        $foo[0] = "";
      }
      my $foo = $foo[0];

      # Pretty formatting.
      my $numbertabs = "\t\t";
      if ($results[0][$counter][0] > 9999999) {
        $numbertabs = "\t";
      }

      # Display foo with the article title for context
      print $results[0][$counter][0] . "$numbertabs| " . wrap("","\t\t| ",$results[0][$counter][1]) . "\n";
      print "\t\t" . "-"x64 . "\n";
      print "\t\t| " . wrap("","\t\t| ", $foo) . "\n";
      if ($counter < 9 and $counter < $retcount - 1) { print "-"x80 . "\n" }

    }

  }

  # Finish out the footer
  print "="x80 . "\n";
  if ($retcount < 10 or $allflag) {
    if ($retcount > 1) {
      print "\nAll $retcount results shown for '$search'.\n";
    } else {
      print "One result shown for '$search'.\n";
    }
  } else {
    print "\nShowing 10 results of $retcount maximum for '$search'.\n";
  }

}

#####
#
# showquestion - Shows the question referenced by number.
#
#####
sub showquestion {

  my $question = shift;

  $Text::Wrap::columns = 91;

  # Fetch the post itself, and any comments
  my $stmt = $dbh->prepare("SELECT posts.Title, posts.Body, comments.Text, posts.ParentId FROM posts LEFT JOIN comments ON posts.Id = comments.PostId WHERE posts.Id='$question';");
  $stmt->execute() or die $DBI::errstr;

  my @post = flat $stmt->fetchall_arrayref();
  my $postcount = $stmt->rows();

  # Stop here if it doesn't exist.
  if ($postcount < 1) { 
    print "\nPost '$question' not found.\n";
    exit(-1);
  }

  # If this is a reply, bring up the parent instead.
  if ($post[0][3]) { print "Redirecting from reply #$question to parent question #$post[0][3].\n"; 
      showquestion($post[0][3]); 
      exit(0);
  }

  # Pretty formatting begins.
  print "="x100 . "\n";

  # Make the correct URL
  my $url = "";
  if ($dbname eq "stackoverflow" or $dbname eq "superuser" or $dbname eq "serverfault") {
    $url = "https://$dbname.com/questions/$question";
  } else {
    $url = "https://$dbname.stackexchange.com/questions/$question";
  }
  print "URL\t| $url\n";
  print "-"x100 . "\n";
  
  my $f = HTML::FormatText::WithLinks->new(
    before_link => "",
    after_link => " [%n]"
  );

  # Fix unicode and filter out garbage.
  my $qtext = Encode::decode("utf8", $post[0][1]);
  $qtext =~ tr/\x00-\x7f//cd;

  # Parse the text
  my $setext = $f->parse($qtext);

  # Since you can pull a reply, but not a comment, by number, we need to be
  # ready and handle this edge case.  Also, replies don't have title text, so
  if ( not $post[0][3]) {
    print "Title\t| " . wrap("","\t| ", subhtml($post[0][0])) . "\n";
  }
  print "-"x100 . "\n";
  print "Body\t| " . wrap("","\t| ", $setext);
  print "-"x100 . "\n";

  $Text::Wrap::columns = 83;

  # Comments are broken out, if they exist.
  for (my $counter = 0;$counter <= $#post;$counter++) {
    my $comment = $post[$counter];
    if (@$comment[2]) {
    
      # Label the block
      if ($counter == 0) {
        print "Comment(s)\t| " . wrap("","\t\t| ", subhtml(@$comment[2])) . "\n";
      } else {
        print "\t\t| " . wrap("","\t\t| ", subhtml(@$comment[2])) . "\n";
      }
      print "\t\t|". "-"x83 . "\n";
      
    }
  }

  # Now for the replies and any existing comments.
  $stmt = $dbh->prepare("SELECT posts.Body, comments.Text FROM posts LEFT JOIN comments ON posts.Id = comments.PostId WHERE posts.parentId = '$question' AND posts.PostTypeID = 2 ORDER BY posts.Score DESC;");
  $stmt->execute() or die $DBI::errstr;

  my @reply = flat $stmt->fetchall_arrayref();
  my $replycount = $stmt->rows();
  my $oldreply = "";

  foreach my $comment (@reply) {
#  for (my $counter = 0;$counter <= $#post;$counter++) {
#    my $comment = $post[$counter];

    # Reply, and not comment
    if (@$comment[0] ne $oldreply) {

      # Fix unicode and filter out garbage.
      my $qtext = Encode::decode("utf8", @$comment[0]);
      $qtext =~ tr/\x00-\x7f//cd;

      # Parse the text
      my $setext = $f->parse($qtext);

      $Text::Wrap::columns = 91;
      print "-"x100 . "\n";
      print "Reply\t| " . wrap("","\t| ", $setext);
      print "-"x100 . "\n";

      if (@$comment[1]) {
        $Text::Wrap::columns = 83;
        print "Comment(s)\t| " . wrap("","\t\t| ", subhtml(@$comment[1])) . "\n";
        print "\t\t|". "-"x83 . "\n";
      }

      $oldreply = @$comment[0];
    } else {

      # The rest of the comments
      $Text::Wrap::columns = 83;

      print "\t\t| " . wrap("","\t\t| ", subhtml(@$comment[1])) . "\n";
      print "\t\t|". "-"x83 . "\n";
      
    }
  }
      print "="x100 . "\n";
}

#====================================== 
# Main Execution Block
#====================================== 
if ($listflag) {
  opendir my($dh), $dbpath or die "Couldn't open dir '$dbpath': $!";
  my @files = readdir $dh;

  if ($#files == 0) {
    print "No databases available in '$dbpath'.  Please configure this script and try again.\n";
    exit(-1);
  }
  print "Available databases\n";
  print "-------------------\n";  
  for (my $counter = 0;$counter <= $#files;$counter++) {
    $files[$counter] =~ s/.db//g;
    if ($files[$counter] ne ".." and $files[$counter] ne ".") {
      print "    $files[$counter]\n";
    }
  }
  print "-------------------\n";  
  print "Default database is:\n";
  print "    $dbname\n";
  exit(0);
}

if ($titlesearch) {
  searchtitles($titlesearch);
  $dbh->disconnect();
  exit(0);
}

if ($bodysearch) {
  searchbodies($bodysearch);
  exit(0);
}

if ($questionsearch) {
  showquestion($questionsearch);
  $dbh->disconnect();
  exit(0);
}

# If we somehow fall through here...
printUsage();
