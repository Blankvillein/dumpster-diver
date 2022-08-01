"""Functions to selectively pick up
data from CSVs generated by crawl.py"""

from base64 import b64encode
from collections import defaultdict, namedtuple


class UserPageMonthLine:
    """Holds data from a single user-page-month CSV line.
    All attribute values are strings."""

    def __init__(self):
        self.attrs = ["user_id", "page_id", "namespace",
                      "page_is_redirect", "month",
                      "month_edits"]
        for attrname in self.attrs:
            setattr(self, attrname, None)
        self.text = ""

    def from_csv(self, csv_line):
        """Fill in values from provided CSV line"""
        csv = csv_line.strip()
        self.text = csv
        if not csv.startswith("IP:"):
            if not csv[0].isnumeric():
                return
        pieces = csv.split(",")
        paired = zip(self.attrs, pieces)
        for attrname, val in paired:
            setattr(self, attrname, val)


def get_bot_ids(botpath, userpath):
    """Given a file with bot usernames each on one
    line, and another file with mappings between
    user_IDs and usernames, return set of bots."""
    botfile = open(botpath, "rb")
    bot_names = set()
    for line in botfile:
        line = line.rstrip()
        if not line:
            continue
        line = line.replace(b"_", b" ")
        encoded_name = b64encode(line)
        bot_names.add(encoded_name)
    bot_ids = set()
    user_file = open(userpath, "rb")
    for line in user_file:
        if line.startswith(b"IP:"):
            continue
        line = line.rstrip()
        user_id, user_name = line.split(b",")
        if user_name in bot_names:
            decoded_id = user_id.decode("utf-8")
            bot_ids.add(decoded_id)
            bot_names.remove(user_name)
    return bot_ids


class Picker:
    """Sifts through CSV files to pick out
    desired data."""

    def __init__(self,
                 filepaths=None,
                 namespaces=None,
                 bots=None):
        if filepaths is None:
            self.filepaths = []
        elif type(filepaths) is str:
            raise ValueError
        else:
            self.filepaths = filepaths
        self.namespaces = namespaces
        if bots is None:
            self.bots = set()
        else:
            self.bots = set(bots)
        self.inc = 0
        self.user_ids = set()
        self.ips = set()
        self.page_ids = set()
        self.redirect_ids = set()
        self.num_user_upm = 0
        self.num_ip_upm = 0
        self.num_user_edits = 0
        self.num_ip_edits = 0
        self.output = {}
        self.by_month = False
        self.months = None

    def get_basic_counts(self,
                         filepaths=None,
                         maxlines=None,
                         by_month=False):
        """Given filepaths to user-page-month CSV
        files, return a {filepath, <stats>} dict.
        If namespaces are provided, collect only for those
        namespaces. If bots are provided, exclude bots.
        Bots must be set of bot IDs (not usernames)."""
        self.output = {}
        self.by_month = by_month
        if self.by_month:
            self.months = defaultdict(Picker)
        if filepaths is None:
            filepaths = self.filepaths
        for path in filepaths:
            print("Processing {}".format(path))
            handle = open(path, encoding="utf-8")
            result = self.process_file(handle, maxlines=maxlines)
            if self.by_month:  # avoid dict of dicts
                result = [x for x in self.months.items()]
            self.output[path] = result
        return self.output

    def process_file(self, open_file, maxlines=None):
        """Process provided open_file up to maxlines,
        and return resulting stats as namedtuple."""
        self.inc = -1
        for line in open_file:
            self.inc += 1
            if not line:
                continue
            if maxlines is not None:
                if self.inc > maxlines:
                    break
            self.process_line(line)
        if self.by_month:
            result = {}
            for month, picker in self.months.items():
                monthly_result = picker.get_results()
                result[month] = monthly_result
        else:
            result = self.get_results()
        return result

    def get_results(self):
        Results = namedtuple("Results",
                             ["num_users", "num_ips",
                              "num_pages", "num_redirects",
                              "num_user_upm", "num_ip_upm",
                              "num_user_edits", "num_ip_edits"])
        result = Results(num_users=len(self.user_ids),
                         num_ips=len(self.ips),
                         num_pages=len(self.page_ids),
                         num_redirects=len(self.redirect_ids),
                         num_user_upm=self.num_user_upm,
                         num_ip_upm=self.num_ip_upm,
                         num_user_edits=self.num_user_edits,
                         num_ip_edits=self.num_ip_edits)
        return result

    def process_ip(self, lineobj):
        self.num_ip_upm += 1
        self.num_ip_edits += int(lineobj.month_edits)
        self.ips.add(lineobj.user_id)

    def process_user(self, lineobj):
        self.num_user_upm += 1
        self.num_user_edits += int(lineobj.month_edits)
        user = lineobj.user_id
        self.user_ids.add(user)

    def line_is_ok(self, lineobj):
        if self.namespaces:
            if lineobj.namespace not in self.namespaces:
                return False
        if lineobj.user_id in self.bots:
            return False
        if lineobj.namespace is None:
            return False
        if lineobj.month_edits is None:
            print("Bad line at {}! {}".format(self.inc, lineobj))
            return False
        return True

    def process_line(self, line):
        """Process line into object and send for
        further processing."""
        lineobj = UserPageMonthLine()
        lineobj.from_csv(line)
        self.process_lineobj(lineobj)

    def process_lineobj(self, lineobj):
        """Increment relevant stats for line"""
        if self.by_month:
            picker = self.months[lineobj.month]
            picker.process_lineobj(lineobj)
        if not self.line_is_ok(lineobj):
            return
        if lineobj.user_id.startswith("IP:"):
            self.process_ip(lineobj)
        else:  # non-bot registered user
            self.process_user(lineobj)
        page = lineobj.page_id
        if lineobj.page_is_redirect == "0":
            self.page_ids.add(page)
        elif lineobj.page_is_redirect == "1":
            self.redirect_ids.add(page)
        else:
            m = "Bad value for is_redirect:" + lineobj.text
            print(m)


def fields2line(fields):
    """Create CSV line of 'fields', ending
    with newline."""
    fields = [x.strip().replace(",", "\\,")
              for x in fields]
    line = ",".join(fields)
    line += "\n"
    return line


def stats2csv(stats, unit_name="year"):
    """Given a {(unit, <data>)} dict, where
    data is a namedtuple and unit is a year or month,
    convert to CSV"""
    csv = ""
    headers = [unit_name]
    data = list(stats.values())[0]
    headers.extend(list(data._fields))
    first_line = fields2line(headers)
    csv += first_line
    for year, data in stats.items():
        values = [year] + list(data)
        values = [str(x) for x in values]
        line = fields2line(values)
        csv += line
    return csv


class BasicStats:
    """Holder for basic stats from
    digesting user-page-months CSV"""

    def __init__(self):
        self.peak_user_page_months = {}
        self.current_peaks = defaultdict(int)
        self.total_revisions = 0
        self.page_ids = set()
        self.user_ids = set()
        self.collect_pages = False
        self.mainspace_only = False
        self.mainspace_user_months = set()
        self.mainspace_page_months = set()
        self.months_by_namespace = defaultdict(int)
        self.edits_by_namespace = defaultdict(int)

    def process_line(self, lineobj):
        """Given a UserPageMonthLine object,
        process it for global stats."""
        if lineobj.user_id is None:
            print("Skipping line object with no user ID")
            return
        self.months_by_namespace[lineobj.namespace] += 1
        count = int(lineobj.month_edits)
        self.total_revisions += count
        if self.collect_pages is True:
            if lineobj.namespace == "0" or \
               self.mainspace_only is False:
                self.page_ids.add(lineobj.page_id)
                self.user_ids.add(lineobj.user_id)
        user_month = (lineobj.user_id, lineobj.month)
        page_month = (lineobj.page_id, lineobj.month)
        self.edits_by_namespace[lineobj.namespace] += count
        if count > self.current_peaks[lineobj.namespace]:
            self.peak_user_page_months[lineobj.namespace] = \
                (lineobj.user_id, lineobj.page_id, count)
            self.current_peaks[lineobj.namespace] = count
        if lineobj.namespace == "0":
            if self.collect_pages:
                self.mainspace_user_months.add(user_month)
                self.mainspace_page_months.add(page_month)

    def load_stats(self,
                   months_filepath="user_page_months_output.csv",
                   limit=None):
        """Load dict of users from CSV files in format:
        {(user_id, User)}."""
        with open(months_filepath, encoding="utf-8") as upm_file:
            linecount = 0
            for line in upm_file:
                linecount += 1
                if limit is not None:
                    if linecount > limit:
                        print("Ending at line {}".format(linecount))
                        break
                lineobj = UserPageMonthLine()
                lineobj.from_csv(line)
                if linecount == 1 and lineobj.user_id is None:
                    # skip header
                    continue
                self.process_line(lineobj)


def load_all_upms(filepath):
    """Given a filepath to a user-page-month CSV,
    return a set of all user-page-months and complain
    if any dups are found."""
    all_upms = set()
    with open(filepath) as lines:
        for line in lines:
            lineobj = UserPageMonthLine()
            lineobj.from_csv(line)
            if lineobj.user_id is None:
                continue
            upm = (lineobj.user_id, lineobj.page_id, lineobj.month)
            if upm in all_upms:
                print("Found duplicate:", str(upm))
            all_upms.add(upm)
    return all_upms