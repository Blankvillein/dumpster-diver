""" Functions to crawl and process a
Wikimedia stub-meta-history XML dump"""

from base64 import b64encode, b64decode
from collections import defaultdict
from datetime import datetime
from hashlib import sha1
from os import mkdir
from os.path import exists, join


class Revision:
    """Holds data from a <revision> tag"""

    def __init__(self, crawler):
        self.revid = None
        self.pagename = None
        self.month = None
        self.user = None
        self.sha1 = None
        self.timestamp = None
        self.parent = crawler
        self.linecount = 0

    def log(self, message):
        """ Write an error message to log,
        or to console if not attached to Crawler."""
        if self.parent is None:
            print(message)
        else:
            self.parent.log(message)

    def add_id(self, revid):
        """Add ID to rev.
        Complain if it already has one."""
        if self.revid is None:
            self.revid = revid
        else:
            message = "Double revids at {}: {} {}"
            message = message.format(self.linecount,
                                     self.revid, revid)
            self.log(message)

    def add_month(self, timestamp):
        """Add month to rev from timestamp.
        Complain if it already has one."""
        month = timestamp[:7]
        if self.month is None:
            self.month = month
        else:
            message = "Double timestamps at line {} " \
                      "for revision {}: {}"
            message = message.format(self.linecount,
                                     self.revid, timestamp)
            self.log(message)


class Page:
    """Holds data from a <page> tag"""

    def __init__(self, crawler=None):
        self.name = None
        self.page_id = None
        self.namespace = None
        self.is_redirect = False
        self.parent = crawler
        self.user_months = defaultdict(int)
        self.user_ids = set()
        self.hashes = set()
        self.users = {}

    def log(self, message):
        """ Write an error message to log,
        or to console if not attached to Crawler."""
        if self.parent is None:
            print(message)
        else:
            message += " at line " + str(self.parent.linecount)
            self.parent.log(message)

    def to_csv(self):
        """Generate single CSV line for page in format:
        (page_id, namespace, encoded_page_name, is_redirect)"""
        if not self.name:
            message = "Error! Page ID {} closed without name"
            message = message.format(self.page_id)
            self.log(message)
            return
        name = self.name.encode("utf-8")
        base64_name = b64encode(name).decode("utf-8")
        is_redirect = str(int(self.is_redirect))
        pieces = (self.page_id, self.namespace,
                  base64_name, is_redirect)
        csv = ",".join(pieces)
        return csv

    def from_csv(self, csv_line):
        """Restore page ID and name from CSV"""
        csv_line = csv_line.strip()
        pieces = csv_line.split(",")
        page_id, namespace, encoded_name, is_redirect = pieces
        name = b64decode(encoded_name).decode("utf-8")
        self.name = name
        self.add_id(page_id)
        self.namespace = namespace
        self.is_redirect = bool(int(is_redirect))

    def is_revert(self, revision):
        """Determine if page is an exact hash
        duplicate of a previous revision of
        the same page. Not yet used."""
        if not revision.sha1:
            message = "Missing SHA1 for revision {}"
            message = message.format(revision.revid)
            self.log(message)
        if revision.sha1 in self.hashes:
            return True
        else:
            return False

    def add_id(self, page_id):
        """Add ID to page, and complain if page
        already has one."""
        if self.page_id is None:
            self.page_id = page_id
        else:
            message = "Double page_ids: {} and {}"
            message = message.format(self.page_id, page_id)
            self.log(message)

    def add_user(self, user, revision):
        """Add user to user_ids of page, and
        increment relevant user_months value"""
        user_id = user.user_id
        if user_id not in self.user_ids:
            self.user_ids.add(user_id)
            self.users[user_id] = user
        month = revision.month
        key = (user_id, month)
        self.user_months[key] += 1

    def get_user_page_months(self):
        """Output a list of CSV lines in format:
        user_id,page_id,namespace,is_redirect,month,count
        """
        if not self.user_months:
            m = "Page ID {} has no revisions"
            message = m.format(self.page_id)
            self.log(message)
        user_months_by_year = defaultdict(list)
        for user_id, month in self.user_months.keys():
            count = self.user_months[(user_id, month)]
            year = ""
            if self.parent:
                if self.parent.split_by_year:
                    year = month[:4]
            pieces = (user_id, self.page_id, self.namespace,
                      str(int(self.is_redirect)), month, str(count))
            new_line = ",".join(pieces)
            user_months_by_year[year].append(new_line)
        return user_months_by_year


class User:
    """Holds data from a <contributor> tag"""

    def __init__(self, crawler=None):
        self.user_id = None
        self.name = None
        self.parent = crawler
        self.is_new = False
        self.ip = None

    def log(self, message):
        """ Write an error message to log,
        or to console if not attached to Crawler."""
        if self.parent is None:
            print(message)
        else:
            linecount = self.parent.linecount
            message += " at line " + str(linecount)
            self.parent.log(message)

    def to_csv(self):
        """Generate CSV line for user with fields
        (user_id,encoded_name) using base-64 encoding.
        For IP users, user_id is a sequential identifier
        and encoded_name is the existing SHA1 hash of the
        IP address."""
        if not self.name:
            message = "User ID {} has no name"
            message = message.format(self.user_id)
            self.log(message)
            return
        if self.ip:
            encoded_name = self.name
        else:
            encoded_name = b64encode(self.name.encode("utf-8"))
            encoded_name = encoded_name.decode("utf-8")
        pieces = (self.user_id, encoded_name)
        csv = ",".join(pieces)
        return csv

    def from_csv(self, csv):
        """Add values from user CSV line as
        generated by self.to_csv()."""
        user_id, encoded_name = csv.split(",")
        self.user_id = user_id
        if user_id.startswith("IP:"):
            self.name = encoded_name
        else:
            self.name = b64decode(encoded_name).decode("utf-8")

    def add_name(self, name):
        """Add name to user.
        Complain if user already has one."""
        if self.name is None:
            self.name = name
        else:
            message = "Warning: two user names at {}: {}; {}"
            message = message.format(self.parent.linecount,
                                     self.name, name)
            self.log(message)

    def add_id(self, user_id):
        """Add user ID to user. If user
        already has ID, log warning."""
        if self.user_id is None:
            self.user_id = user_id
        else:
            message = "Double user_ids at {}: {} {}"
            message = message.format(self.parent.linecount, self.user_id, user_id)
            self.log(message)


class Crawler:
    """Crawls through XML dump while writing
    extracted user, page, and user-page-month
    data to respective output files."""

    def __init__(self,
                 filepath="stub.xml",
                 output_directory=".",
                 log_to_console=True,
                 mainspace_only=False,
                 split_by_year=False,
                 overwrite=False):
        self.filepath = filepath
        self.log_to_console = log_to_console
        self.split_by_year = split_by_year
        self.mainspace_only = mainspace_only
        self.overwrite = overwrite
        self.handle = None
        self.user_ids = set()
        self.ips = set()
        self.ip_count = 0
        self.ip2id = {}
        self.loghandle = None
        self.maxlines = None
        self.linecount = 0
        self.revcount = 0
        self.deleted_user_edits = 0
        self.current_page = None
        self.current_revision = None
        self.current_user = None
        self.output_headers = {"users_output": ("user_id", "user_name"),
                               "user_page_months_output": ("user_id", "page_id",
                                                           "page_namespace", "page_is_redirect",
                                                           "user_page_month", "user_page_month_edits"),
                               "pages_output": ("page_id", "page_namespace",
                                                "page_name_base64", "page_is_redirect")
                               }
        self.output_directory = output_directory
        self.active_outputs = {}

    def activate_outputs(self, year=None):
        """Open files to be used for CSV output.
        Iff starting fresh, write header line."""
        message = "Activating outputs"
        if year is not None:
            message += " for year " + str(year)
        self.log(message)
        if self.overwrite is False:
            mode = "a"
        else:
            mode = "w"
        if not exists(self.output_directory):
            mkdir(self.output_directory)
        for name in self.output_headers.keys():
            filename = get_output_filename(name, year)
            if filename in self.active_outputs.keys():
                continue
            filepath = join(self.output_directory, filename)
            overwrite = self.overwrite
            if not overwrite:
                if not exists(filepath):
                    overwrite = True
            handle = open(filepath, mode, encoding="utf-8")
            self.active_outputs[filename] = handle
            if overwrite is True:
                fields = self.output_headers[name]
                first_line = ",".join(fields) + "\n"
                handle.write(first_line)

    def close_outputs(self):
        """Close the CSV output files."""
        for name, output in self.active_outputs.items():
            try:
                output.close()
            except Exception as e:
                m = "Error in closing output {}: {}"
                message = m.format(name, str(e))
                self.log(message)
            self.active_outputs[name] = None

    def log(self, message):
        """Write message to log, and optionally
        also to console."""
        if not message:
            message = "Error! Blank message at line" + \
                      str(self.linecount)
        if self.log_to_console:
            print(message)
        if self.loghandle is not None:
            self.loghandle.write(message + "\n")

    def get_oneline_tag(self, text):
        """Get content of a tag that is expected
        to all be in a single line."""
        try:
            content = text.split(">", 1)[1]
        except IndexError:
            self.log("Problem with one-line tag at line {}: {}"
                     .format(self.linecount, text))
            return ""
        tag_pieces = content.split("<", 1)
        if len(tag_pieces) != 2:
            self.log("Problem with one-line tag at line {}: {}"
                     .format(self.linecount, text))
            return ""
        tag_content = tag_pieces[0]
        tag_content = tag_content.strip()
        return tag_content

    def reset_page(self):
        """Write data and clean up after </page>"""
        if self.current_page is not None:
            if self.current_user is not None:
                m = "Warning: page ended without closing user "\
                    "at {}, page id {}, user id {}"
                message = m.format(self.linecount,
                                   self.current_page.page_id,
                                   self.current_user.user_id)
                self.log(message)
            if self.current_revision is not None:
                message = "Warning: page ended without closing " \
                          "revision at {}, page id {}, revision id {}"
                self.log(message.format(self.linecount,
                                        self.current_page.page_id,
                                        self.current_revision.revid))
            self.write_current_page_months()
            self.write_current_page()
        self.current_page = None
        self.current_revision = None
        self.current_user = None

    def reset_revision(self):
        """Store user and rev data and
        clean up after </revision>"""
        if self.current_page is not None:
            if self.current_revision is not None:
                if self.current_user is None:
                    m = "Warning: revision {} has no user at {}"
                    message = m.format(self.current_revision.revid,
                                       self.linecount)
                    self.log(message)
                else:
                    self.add_user()
        self.revcount += 1
        if not self.revcount % 5000000:
            now = datetime.today().isoformat()
            message = "Reached revision {} at line {} at {}"
            message = message.format(self.revcount, self.linecount, now)
            self.log(message)
        self.current_revision = None
        self.current_user = None

    def get_id_and_name_for_ip(self, ip):
        """For new unique IP address, return a sequential
        ID and an SHA1 hash to use as a user name."""
        user_id = "IP:{}".format(self.ip_count)
        self.ip2id[ip] = user_id
        hashed_ip = sha1(ip.encode("utf-8"))
        user_name = hashed_ip.hexdigest()
        return user_id, user_name

    def add_user(self):
        """Add user info to self and current_page.
        If user already there, do nothing."""
        this_user = self.current_user
        if this_user.ip:
            if this_user.is_new:
                self.ips.add(this_user.ip)
                self.ip_count += 1
        this_id = this_user.user_id
        if this_id not in self.user_ids:
            self.user_ids.add(this_id)
            self.write_current_user()
        this_rev = self.current_revision
        self.current_page.add_user(this_user, this_rev)
        self.current_user = None

    def get_output(self, output_name, year=None):
        """Given an output name such as 'users_output',
        return the correct open output file"""
        output_filename = get_output_filename(output_name, year)
        active_outputs = self.active_outputs.keys()
        if output_filename not in active_outputs:
            self.activate_outputs(year=year)
        output = self.active_outputs[output_filename]
        return output

    def write_output_line(self, output_name, line,
                          year=None):
        """Write provided line to specified output."""
        output_file = self.get_output(output_name, year=year)
        if not line.endswith("\n"):
            line += "\n"
        output_file.write(line)

    def write_current_user(self):
        """Write user info to users_output as CSV
        in format: (user_id, encoded_name)"""
        csv = self.current_user.to_csv()
        if csv:
            self.write_output_line("users_output", csv)
        else:
            message = "Warning: blank user CSV at line: " + \
                      str(self.linecount)
            self.log(message)

    def write_current_page_months(self):
        """Write CSV of all user_page_months for
        current_page to respective output file or
        annual output file."""
        lines_by_year = self.current_page.get_user_page_months()
        if not lines_by_year:
            m = "Warning: No user_page_months for page ID {} at line {}"
            message = m.format(self.current_page.page_id, self.linecount)
            self.log(message)
            return
        for year, csv_lines in lines_by_year.items():
            if len(set([x.count(",") for x in csv_lines])) != 1:
                # there should be no extra or missing commas
                message = "Problem with user-page-months CSV at {}:\n{}" \
                    .format(self.linecount, csv_lines)
                self.log(message)
                return
            csv = "\n".join(csv_lines)
            output = "user_page_months_output"
            self.write_output_line(output, csv, year=year)

    def write_current_page(self):
        """Write CSV of current_page info to respective
        output file."""
        page_csv = self.current_page.to_csv()
        if page_csv is None:
            message = "Warning: blank page CSV at line {}" \
                .format(self.linecount)
            self.log(message)
        else:
            self.write_output_line("pages_output", page_csv)

    def process_line(self, line):
        """Process a single line from
        stub-meta-history dump."""
        line = line.lstrip()
        if not line:
            return
        if line[0] != "<":
            return
        if line[1] == "/":
            if line.startswith("</revision>"):
                self.reset_revision()
            if line.startswith("</page>"):
                self.reset_page()
        else:
            tagname = line[1:line.find(">")]
            if tagname == "page":
                self.current_page = Page(crawler=self)
            elif self.current_page is not None:
                if tagname == "id":
                    this_id = self.get_oneline_tag(line)
                    if self.current_user:
                        self.current_user.add_id(this_id)
                    elif self.current_revision:
                        self.current_revision.add_id(this_id)
                    elif self.current_page:
                        self.current_page.add_id(this_id)
                elif tagname == "revision":
                    self.current_revision = Revision(crawler=self)
                elif tagname == "contributor":
                    self.current_user = User(crawler=self)
                elif tagname == "username":
                    username = self.get_oneline_tag(line)
                    self.current_user.add_name(username)
                elif tagname == "ip":
                    ip = self.get_oneline_tag(line)
                    self.current_user.ip = ip
                    if ip in self.ips:
                        user_id = self.ip2id[ip]
                    else:
                        user_id, username = self.get_id_and_name_for_ip(ip)
                        self.current_user.add_name(username)
                        self.current_user.is_new = True
                    self.current_user.user_id = user_id
                elif tagname == "timestamp":
                    timestamp = self.get_oneline_tag(line)
                    self.current_revision.add_month(timestamp)
                elif tagname == "sha1":
                    hashed = self.get_oneline_tag(line)
                    self.current_revision.sha1 = hashed
                elif tagname == "ns":
                    namespace = self.get_oneline_tag(line)
                    self.current_page.namespace = namespace
                    if namespace != "0":
                        if self.mainspace_only:
                            self.current_page = None
                elif tagname == "title":
                    title = self.get_oneline_tag(line)
                    if title != "":
                        self.current_page.name = title
                elif tagname[:8] == "redirect":
                    self.current_page.is_redirect = True
                elif tagname[:19] == "contributor deleted":
                    self.create_deleted_user()
                    self.deleted_user_edits += 1

    def create_deleted_user(self):
        """Create dummy user as current_user
        for revisions with attribution removed."""
        user = User()
        user.user_id = "0"
        user.name = "[Attribution Removed]"
        self.current_user = user

    def process_file(self):
        """iterate through the open file at self.handle"""
        if not hasattr(self.handle, "closed"):
            raise IOError
        if self.handle.closed:
            raise IOError
        with self.handle:
            for line in self.handle:
                self.process_line(line)
                self.linecount += 1
                if self.maxlines is not None:
                    if self.linecount > self.maxlines:
                        break

    def end_crawl(self):
        """Close all open files at end of crawl."""
        now = datetime.today().isoformat()
        message = "Ended run on line {} at {}" \
            .format(self.linecount, now)
        self.log(message)
        self.close_outputs()
        self.loghandle.close()

    def crawl(self,
              maxlines=None,
              logpath="log.txt",
              mainspace_only=None):
        """Crawl over stub-meta-history dump until
        reaching either maxlines or end of file."""
        self.handle = open(self.filepath, encoding="utf-8")
        if maxlines is not None:
            self.maxlines = maxlines
        if mainspace_only is not None:
            self.mainspace_only = mainspace_only
        if logpath is not None:
            handle = open(logpath, "a", encoding="utf-8")
            self.loghandle = handle
            now = datetime.today().isoformat()
            message = "Started run at " + now
            self.log(message)
        self.linecount = 0
        if not self.split_by_year:
            self.activate_outputs()
        try:
            self.process_file()
        except KeyboardInterrupt:
            now = datetime.today().isoformat()
            m = "Terminating on keyboard interrupt "\
                "at line {} at {}"
            message = m.format(self.linecount, now)
            self.log(message)
        self.end_crawl()


def get_output_filename(output_name, year=None):
    # Return CSV filename
    filename = ""
    if year is not None:
        if output_name == "user_page_months_output":
            filename = str(year) + "-"
    filename += output_name + ".csv"
    return filename


def main(args):
    """Generate a Crawler with specified args
    and crawl provided dump, outputting data to
    current working directory."""
    from argparse import ArgumentParser
    description = "Crawl a stub-meta-history dump and "\
                  "output to current directory"
    parser = ArgumentParser(description=description)
    parser.add_argument("filepath", )
    parser.add_argument("-f", "--filepath",
                        dest="filepath",
                        help="Path to XML dump")
    parser.add_argument("-n", "--no_console",
                        default=True,
                        dest="log_to_console",
                        action="store_false",
                        help="Whether to log to console")
    parser.add_argument("-m", "--mainspace_only",
                        default=False,
                        dest="mainspace_only",
                        action="store_true",
                        help="Whether to process mainspace edits only")
    parser.add_argument("--maxlines",
                        default=None,
                        dest="maxlines",
                        help="Maximum number of lines to read from dump")
    parser.add_argument("--no-overwrite",
                        default=True,
                        action="store_false",
                        dest="overwrite",
                        help="Append to existing CSV files rather than overwriting")
    parser.parse_args(args)
    crawler = Crawler(filepath=parser.filepath,
                      log_to_console=parser.log_to_console,
                      mainspace_only=parser.mainspace_only,
                      overwrite=parser.overwrite)
    crawler.crawl(maxlines=parser.maxlines)


if __name__ == "__main__":
    from sys import argv
    main(argv[1:])
