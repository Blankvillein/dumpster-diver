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


class BandInfo:
    """Stores data for a user or article band,
    consisting of "name", "members", and
    "edit_count". This can be used to store
    information about, for example, the activity
    of a user band defined in one year for a
    different year."""

    def __init__(self, name=""):
        self.name = name
        self.members = set()
        self.edit_count = 0

    def tuplify(self):
        BandData = namedtuple("BandData",
                              ["name", "member_count", "edit_count"])
        output = BandData(self.name, len(self.members),
                          self.edit_count)
        return output


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
        # whether to omit redirects from
        # page edit count:
        self.skip_redirects = True
        # bands by log 10 less 1,
        # aka number of digits:
        self.bands = [1, 2, 3, 4, None]
        self.by_month = False
        self.by_user_band = False
        self.reset_counts()

    def reset_counts(self):
        """Set all counts to default values"""
        self.inc = 0
        self.user_ids = set()
        self.ips = set()
        self.page_ids = set()
        self.redirect_ids = set()
        self.num_user_upm = 0
        self.num_ip_upm = 0
        self.num_user_edits = 0
        self.num_ip_edits = 0
        self.basic_counts = {}
        self.months = None
        self.user_edits = None
        self.page_edits = None
        self.banded_users = defaultdict(int)

    def get_edit_band(self, editcount, base=10):
        """Given an edit count, return the corresponding
        edit band. Each band consists of numbers less than
        the given power of the base number (which in the
        default base-10 case corresponds to the number of
        digits.) The maximum edit band is None, whether
        specified or not.
        """
        if editcount < 1:
            print("Warning! Bad edit count {}"
                  .format(editcount))
        for band in self.bands:
            if band is None:
                return None
            elif editcount < base ** band:
                return band
        print("Warning! No band specified for edit count\
               {}".format(editcount))
        return None

    def get_monthly_edits_by_band(self,
                                  filepath=None,
                                  banded_users=None):
        """Given a {(user_id, band)} dict,
        calculate monthly users and edits
        from provided file."""
        if banded_users is None:
            if self.banded_users is None:
                raise ValueError
            banded_users = self.banded_users
        user_ids = set(banded_users.keys())
        banded_data = defaultdict(BandInfo)
        for line in open(filepath):
            lineobj = self.process_line(line)
            if not self.line_is_ok(lineobj):
                continue
            if lineobj.user_id in user_ids:
                month = lineobj.month
                band = banded_users[lineobj.user_id]
                band_data = banded_data[(month, band)]
                edits = int(lineobj.month_edits)
                band_data.edit_count += edits
                band_data.members.add(lineobj.user_id)
        final_data = {}  # use a standard dict to return data
        for band_label, band_data in banded_data.items():
            band_data.name = band_label
            final_data[band_label] = band_data.tuplify()[1:]
        return final_data

    def get_user_edits(self,
                       filepaths=None):
        """Given a list of filepaths, return a
        {userid : editcount} dict."""
        self.user_edits = defaultdict(int)
        if filepaths is None:
            filepaths = self.filepaths
        for path in filepaths:
            print("Processing {}".format(path))
            handle = open(path, encoding="utf-8")
            result = self.process_file(handle)
            self.basic_counts[path] = result
        return self.user_edits

    def get_page_edits(self,
                       filepaths=None):
        """Given a list of filepaths, return a
        {pageid : editcount} dict."""
        self.page_edits = defaultdict(int)
        if filepaths is None:
            filepaths = self.filepaths
        for path in filepaths:
            print("Processing {}".format(path))
            handle = open(path, encoding="utf-8")
            result = self.process_file(handle)
            self.basic_counts[path] = result
        return self.page_edits

    def get_pages_for_month(self, filepath, month):
        """Given a single filepath, return a set of
        all page IDs edited in a given month"""
        page_ids = set()
        page_file = open(filepath)
        with page_file:
            for line in page_file:
                lineobj = self.process_line(line)
                if not self.line_is_ok(lineobj):
                    continue
                if lineobj.month == month:
                    page_ids.add(lineobj.page_id)
        return page_ids

    def get_users(self, filepath):
        """For a given user-page-month CSV file,
        return set of registered user IDs present
        in the file."""
        if not self.bots:
            print("Warning! Getting users without excluding bots.")
        users = set()
        user_file = open(filepath)
        with user_file:
            for line in user_file:
                lineobj = self.process_line(line)
                user_id = lineobj.user_id
                namespace = lineobj.namespace
                if namespace not in self.namespaces:
                    continue
                if user_id is None:
                    continue
                if self.bots is not None:
                    if lineobj.user_id in self.bots:
                        continue
                if user_id.startswith("IP:"):
                    continue
                users.add(user_id)
        return users

    def get_basic_counts(self,
                         filepaths=None,
                         maxlines=None,
                         by_month=False):
        """Given filepaths to user-page-month CSV
        files, return a {filepath : stats} dict.
        If namespaces provided, collect only for those
        namespaces. If bots provided, exclude bots.
        Bots must be set of bot IDs (not usernames)."""
        self.basic_counts = {}
        self.by_month = by_month
        if self.by_month:
            self.months = defaultdict(Picker)
        if filepaths is None:
            filepaths = self.filepaths
        for path in filepaths:
            print("Processing {}".format(path))
            handle = open(path, encoding="utf-8")
            result = self.process_file(handle,
                                       maxlines=maxlines)
            if self.by_month:  # avoid dict of dicts
                result = [(x[0], x[1].get_results()) for x in
                          self.months.items()]
            self.basic_counts[path] = result
        return self.basic_counts

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
            lineobj = self.process_line(line)
            self.process_lineobj(lineobj)
        if self.by_month:
            result = {}
            for month, picker in self.months.items():
                picker.bots = self.bots
                monthly_result = picker.get_results()
                result[month] = monthly_result
        else:
            result = self.get_results()
        return result

    def get_results(self):
        """Generate a Results namedtuple
        from the Picker's basic stats."""
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
        """Increment counts for IP and add IP to self.ips"""
        self.num_ip_upm += 1
        self.num_ip_edits += int(lineobj.month_edits)
        self.ips.add(lineobj.user_id)

    def process_user(self, lineobj):
        """Increment counts for user and add user to
        self.user_ids. If user_edits is set, update."""
        self.num_user_upm += 1
        editcount = int(lineobj.month_edits)
        self.num_user_edits += editcount
        user = lineobj.user_id
        self.user_ids.add(user)
        if self.user_edits is not None:
            self.user_edits[user] += editcount

    def process_page(self, lineobj):
        """Increment counts for page and add page to
        self.page_ids."""
        page = lineobj.page_id
        is_redirect = bool(int(lineobj.page_is_redirect))
        if is_redirect is False:
            self.page_ids.add(page)
        else:
            self.redirect_ids.add(page)
        if self.page_edits is not None:
            if self.skip_redirects is True and is_redirect:
                return
            else:
                editcount = int(lineobj.month_edits)
                self.page_edits[page] += editcount

    def line_is_ok(self, lineobj):
        """Return True if line is OK to process,
        otherwise False."""
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

    @staticmethod
    def process_line(line):
        """Process line into object and send for
        further processing."""
        lineobj = UserPageMonthLine()
        lineobj.from_csv(line)
        return lineobj

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
        self.process_page(lineobj)


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


def fields2line(fields):
    """Create CSV line from 'fields', ending
    with newline."""
    fields = [x.strip().replace(",", "\\,")
              for x in fields]
    line = ",".join(fields)
    line += "\n"
    return line


def get_upm_files(directory):
    """Given a directory, return a sorted
    list of paths to CSV files with
    "user_page_month" in filename."""
    from os import listdir
    from os.path import join
    upm_files = []
    for filename in listdir(directory):
        if not filename.endswith(".csv"):
            continue
        if not "user_page_month" in filename:
            continue
        filepath = join(directory, filename)
        upm_files.append(filepath)
    upm_files.sort()
    return upm_files


def stats2csv(stats, unit_name="year"):
    """Given a {unit : data} dict, where data is a
    namedtuple and unit is a year or month, return
    CSV text."""
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


def load_all_upms(filepath):
    """Given a filepath to a user-page-month CSV,
    return a set of all user-page-months and complain
    if any dups are found."""
    all_upms = set()
    upm_file = open(filepath)
    with upm_file:
        for line in upm_file:
            lineobj = UserPageMonthLine()
            lineobj.from_csv(line)
            if lineobj.user_id is None:
                continue
            upm = (lineobj.user_id, lineobj.page_id, lineobj.month)
            if upm in all_upms:
                print("Found duplicate:", str(upm))
            all_upms.add(upm)
    return all_upms


def file2year(filename):
    """Return year if present in filename,
    otherwise return filename."""
    from re import search
    year_finder = search("[12][09]\\d\\d", filename)
    label = filename
    if year_finder:
        label = year_finder.group(0)
    return label


def get_user_ages(users, this_year, user_years):
    """Given a set of users from this_year and a
    {year:set([user IDs])} dict, return a defaultdict
    of the age distribution of this_year's users."""
    this_year = int(this_year)
    user_ages = dict()
    first_years = reversed(sorted(user_years.keys()))
    for y in first_years:
        first_year = int(y)
        age = this_year - first_year
        first_year_users = user_years[y].intersection(users)
        user_ages[age] = len(first_year_users)
    return user_ages


def get_user_ages_by_year(directory, bots=None):
    """Get the age distribution of editors editing
    in mainspace during each year."""
    from os import listdir
    from os.path import join
    upm_files = [join(directory, x) for x in
                 listdir(directory) if "user_page_month" in x]
    user_years = dict()
    user_ages = dict()
    existing_users = set()
    if bots is None:
        bots = set()
    for u in upm_files:
        year = file2year(u)
        if not year.isnumeric():
            print("Bad file name: {}".format(u))
            continue
        picker = Picker(filepaths=[u], namespaces=["0"], bots=bots)
        year_users = picker.get_users(filepath=u)
        new_users = year_users - existing_users
        old_users = existing_users.intersection(year_users)
        print(year, len(year_users), len(new_users), len(old_users))
        year_ages = get_user_ages(old_users, year, user_years)
        year_ages[0] = len(new_users)
        year_ages_listed = sorted(year_ages.items())
        user_years[year] = new_users
#        print([(x,len(user_years[x])) for x in user_years.keys()])
        existing_users |= new_users
        user_ages[year] = year_ages_listed
        print(year, str(year_ages_listed))
    return user_ages


def get_weighted_age_by_year(directory,
                             bots=None,
                             namespaces=None):
    """Get the mean age of editors editing during
    each year, weighted by edits (i.e. over
    all non-bot edits made, the mean editor age).
    Defaults to mainspace."""
    upm_files = get_upm_files(directory)
    if namespaces is None:
        namespaces = ["0"]
    user_years = dict()
    existing_users = set()
    averages = []
    if bots is None:
        print("Warning! Proceeding without bot exclusion.")
        bots = set()
    for u in upm_files:
        year = file2year(u)
        if not year.isnumeric():
            print("Bad file name: {}".format(u))
            continue
        picker = Picker(namespaces=namespaces, bots=bots)
        user_edits = picker.get_user_edits(filepaths=[u])
        year_users = set(user_edits.keys())
        new_users = year_users - existing_users
        old_users = existing_users.intersection(year_users)
        print(year, len(year_users), len(new_users), len(old_users))
        total_edits = sum(user_edits.values())
#       mean calculated as: (sum of (edits * editor_age))/num_edits
        weighted_edits = 0
        for old_year, users in user_years.items():
            age = int(year) - int(old_year)
            users_of_age = users.intersection(year_users)
            edits_by_age = sum([user_edits[x] for x in users_of_age])
            weighted_edits += edits_by_age * age
            print(year, age, edits_by_age, weighted_edits)
        mean_age = weighted_edits / total_edits
        print(mean_age)
        averages.append((year, mean_age))
        user_years[year] = new_users
        existing_users |= new_users
    return averages


def get_year_band_totals(directory,
                         bots=None,
                         namespaces=None,
                         page_edits=False):
    """Get yearly totals of edits and users or pages,
    by user/page edit band for that year. If page_edits
    is set, append a list of cumulative pagecounts by year
    to the results list."""
    from re import search
    if bots is None:
        print("Warning! Proceeding without bot file.")
        bots = set()
    paths = get_upm_files(directory)
    output = list()
    all_pages = set()
    page_counts = []
    if namespaces is None:  # default to mainspace
        namespaces = ["0"]
    for p in paths:
        print(p)
        picker = Picker([p], namespaces, bots)
        if page_edits is True:
            picker.skip_redirects = True
            member_edits = picker.get_page_edits()
        else:
            member_edits = picker.get_user_edits()
        starter = [(1, 0), (2, 0), (3, 0), (4, 0), (None, 0)]
        band_edits = dict(starter)
        band_members = dict(starter)
        for member, edits in member_edits.items():
            band = picker.get_edit_band(edits)
            band_edits[band] += edits
            band_members[band] += 1
        year_finder = search("\\d{4}", p)
        label = p
        if year_finder:
            label = year_finder.group(0)
        if page_edits is True:
            page_count_before = len(all_pages)
            all_pages |= picker.page_ids
            page_count_after = len(all_pages)
            new_pages = page_count_after - page_count_before
            if new_pages < 0:
                print("Error! Files out of order.")
            page_counts.append((label, page_count_after))
        data = (label, list(band_edits.items()), list(band_members.items()))
        output.append(data)
    if page_edits is True:
        output.append(page_counts)
    return output


def get_cross_bands(filepath,
                    namespaces1=None,
                    namespaces2=None,
                    bots=None):
    """Get yearly totals of edits and users, by user/page
    edit band, sorting users from namespaces2 into bands
    based on edit count in namespaces1. By default, compare
    mainspace to Project and Project_talk."""
    if namespaces1 == namespaces2:
        raise ValueError
    if namespaces1 is None:
        namespaces1 = ["0"]
    if namespaces2 is None:
        namespaces2 = ["4", "5"]
    if bots is None:
        print("Warning! Proceeding without bot file.")
        bots = set()
    picker = Picker([filepath], namespaces1, bots)
    user_edits = picker.get_user_edits()
    banded_users = dict()
    print(sum(user_edits.values()), picker.num_user_edits)  # should be equal
    starter = [(1, 0), (2, 0), (3, 0), (4, 0), (None, 0)]
    band_edits = dict(starter)
    band_users = dict(starter)
    for user, edits in user_edits.items():
        band = picker.get_edit_band(edits)
        band_edits[band] += edits
        band_users[band] += 1
        banded_users[user] = band
#    data1 = (list(band_edits.items()), list(band_users.items()))
    picker2 = Picker([filepath], namespaces2, bots)
    user_edits2 = picker2.get_user_edits()
    band_edits2 = dict(starter)
    band_users2 = dict(starter)
    for user, edits in user_edits2.items():
        try:
            band = banded_users[user]
        except KeyError:
            band = 0
            if 0 not in band_edits2.keys():
                band_edits2[0] = 0
                band_users2[0] = 0
        band_edits2[band] += edits
        band_users2[band] += 1
    data2 = (list(band_edits2.items()), list(band_users2.items()))
    return data2


def get_banded_ages(directory, bots=None):
    """Return the age distribution of editors editing
    in mainspace in each edit band during each year,
    as a {(year,age,band):(users,edits)} dict."""
    upm_paths = get_upm_files(directory)
    banded_ages = dict()
    user_years = dict()
    users2years = dict()
    existing_users = set()
    if bots is None:
        print("Warning! Proceeding without bot exclusion.")
        bots = set()
    for path in upm_paths:
        print(path)
        year = file2year(path)
        if not year.isnumeric():
            print("Bad file name: {}".format(path))
            continue
        picker = Picker(filepaths=[path], namespaces=["0"], bots=bots)
        picker.skip_redirects = False
        user_edits = picker.get_user_edits()
        band_edits = defaultdict(int)
        band_users = defaultdict(int)
        year_users = set(user_edits.keys())
        new_users = year_users - existing_users
        old_users = existing_users.intersection(year_users)
        outstr = "{}: {} users, {} new + {} old"
        outstr = outstr.format(year, len(year_users),
                               len(new_users), len(old_users))
        print(outstr)
        for n in new_users:
            users2years[n] = int(year)
        for user, editcount in user_edits.items():
            user_age = int(year) - users2years[user]
            if user_age < 0:
                print("Warning! Files out of order", path, user)
            band = picker.get_edit_band(editcount)
            key = (year, user_age, band)
            band_edits[key] += editcount
            band_users[key] += 1
        for key, editcount in band_edits.items():
            user_count = band_users[key]
            banded_ages[key] = (user_count, editcount)
        user_years[year] = new_users
        existing_users |= new_users
    return banded_ages
    
