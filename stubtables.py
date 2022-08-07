"""Functions to process and present data that
load.py loads from CSV files crawl.py creates."""

from collections import defaultdict, namedtuple
from datetime import date, timedelta


def make_user_table_by_year(yearbands,
                            title="EN Wikipedians by year who made "
                                  "''n'' edits in their first year"):
    """Generate wikitable of user-year data.

    Parameters
    ----------
    yearbands: dict
        Dict of ((year, bandlabel),count) pairs
    title : str
        Desired title of table

    Returns
    -------
    str
    Returns wikitable with user-bands as columns
    and years as rows.
    If neither 'users' nor 'yearbands' is provided,
    a blank string is returned.
    """
    bands = sorted(yearbands.keys())
    table = "{|class=wikitable"
    table += """
|+{}
|Year
|1-9 edits
|10-99 edits
|100-999 edits
|1,000-9,999 edits
|10,000+ edits
""".format(title)
    years = sorted(set([x[0] for x in bands]))
    for y in years:
        bands_in_year = [x for x in bands if x[0] == y]
        row = "|-\n|" + y
        for b in bands_in_year:
            count = yearbands[b]
            row += "\n|{}".format(count)
        row += "\n"
        table += row
    table += "|}"
    return table


def make_page_table_by_year(yearbands,
                            title="Pages by year"):
    """Generate wikitable of user-year data.

    Parameters
    ----------
    yearbands: dict
        Dict of ((year, bandlabel),count) pairs
    title : str
        Desired title of table

    Returns
    -------
    str
    Returns wikitable with user-bands as columns
    and years as rows.
    """
    bands = sorted(yearbands.keys())
    table = "{|class=wikitable"
    table += """
|+{}
|Year
|1-9 edits
|10-99 edits
|100-999 edits
|1,000-9,999 edits
|10,000-99,999 edits
|100,000+ edits
""".format(title)
    years = sorted(set([x[0] for x in bands]))
    for y in years:
        bbb = [x for x in bands if x[0] == y]
        row = "|-\n|" + y
        for b in bbb:
            total = str(yearbands[b])
            row += "\n|" + total
        row += "\n"
        table += row
        table += "|}"
    return table


def get_y2_users_by_y1_edits(users, stop=None, calendar_year="2021"):
    """Sorts users into bands based on edit count
    their first 12 months of editing, and returns
    the resulting banded tallies of users and
    edits made in calendar_year (default 2021).
    Users are not counted if they made their
    first edit during or after calendar_year.

    Parameters
    ----------
    users : dict
        Dict of (id,User) pairs
    stop: NoneType, int
        If specified, number of users to stop at
    calendar_year: str
        Four-digit string of calendar year to be studied.

    Returns
    -------
    namedtuple
    Returns a named tuple with attribs 'banded_users'
    and 'banded_edits', each containing a dict with
    the format: {(year, bandlabel):count}.
    """
    if type(calendar_year) != str or len(calendar_year) != 4:
        raise ValueError
    userbands = defaultdict(int)
    editbands = defaultdict(int)
    user_inc = -1
    for u in users.values():
        user_inc += 1
        if stop is not None:
            if user_inc >= stop:
                break
        months = sorted(u.months.keys())
        first_month = months[0]
        first_year, mm = first_month.split("-", 1)
        if len(first_year) != 4 or len(mm) != 2:
            raise ValueError
        if first_year >= calendar_year:
            continue
        nominal_start = date(int(first_year), int(mm), 1)
        twelve_later = nominal_start + timedelta(365)
        thirteenth_month = twelve_later.isoformat()[:7]
        this_month = first_month
        month_inc = -1
        edits_in_first_year = 0
        num_months = len(months)
        while this_month < thirteenth_month and \
                month_inc + 1 < num_months:
            month_inc += 1
            this_month = months[month_inc]
            edits_in_first_year += u.months[this_month]
        y1_band = get_banded_count(edits_in_first_year)
        y2_total = 0
        for m in months:
            year = m[:4]
            if year > calendar_year:
                break
            elif year == calendar_year:
                monthly_edits = u.months[m]
                y2_total += monthly_edits
        y2_band = get_banded_count(y2_total)
        pair_band = (y2_band, y1_band)
        userbands[pair_band] += 1
        editbands[pair_band] += y2_total
    Results = namedtuple("Results",
                         ["banded_users", "banded_edits"])
    output = Results(banded_users=userbands,
                     banded_edits=editbands)
    return output


def get_banded_count(count):
    """
    Returns a band based on a number (typically a
    number of edits)
        Parameters
        ----------
        count: int

        Returns
        ---------
        int
    """
    if count == 0:
        return 0
    if count < 10:
        band = 10
    elif count < 100:
        band = 100
    elif count < 1000:
        band = 1000
    elif count < 10000:
        band = 10000
    else:
        band = 100000
    return band


def make_combo_table(userbands, editbands,
                     title="Users and edits by number of "
                           "edits in first year",
                     x_band_labels=None,
                     y_band_labels=None,
                     x_band_suffix="",
                     y_band_suffix=""):
    """Generate wikitable of combined user and
    edit data with arbitrary headings.

    Parameters
    ----------
    userbands: list
        List of ((label1, label2),count) pairs
    editbands: list
        List of ((label1, label2),count) pairs.
        Must have same keys as userbands.
    title : str
        Desired title of table
    x_band_labels : list or NoneType
        Labels of columns
    y_band_labels : list or NoneType
        Labels of rows
    x_band_suffix : str
        String appended to all x_band_labels
    y_band_suffix : str
        String appended to all y_band_labels

    Returns
    -------
    str
    Returns wikitable of provided values
    with user and edit values in each cell
    """
    userbands = dict(userbands)
    editbands = dict(editbands)
    if userbands.keys() != editbands.keys():
        return ValueError
    if not x_band_labels:
        xxx = set([x[0] for x in userbands.keys()])
        x_band_labels = sorted(xxx)
    if not y_band_labels:
        yyy = set([x[1] for x in userbands.keys()])
        y_band_labels = sorted(yyy)
    if x_band_suffix:
        x_band_labels = [str(x) + x_band_suffix
                         for x in x_band_labels]
    if y_band_suffix:
        y_band_labels = [str(x) + y_band_suffix
                         for x in y_band_labels]
    table = "{|class=wikitable"
    table += """
|+{}
|
""".format(title)
    for label in x_band_labels:
        table += "!{}\n".format(label)
    table += "|-\n"
    for y_label in y_band_labels:
        table += "!{}\n".format(y_label)
        for x_label in x_band_labels:
            try:
                cell_user_count = userbands[(x_label, y_label)]
                cell_edit_count = editbands[(x_label, y_label)]
            except KeyError:
                cell_user_count = 0
                cell_edit_count = 0
            cell_content = str(cell_user_count) + \
                " users{{br}}making{{br}}" + str(cell_edit_count) + \
                " edits"
            cell = "|{}\n".format(cell_content)
            table += cell
        table += "|-\n"
    table += "|}"
    return table


def tuples2table(data, title=""):
    """Given {(row_label, <stats>)} dict of
    namedtuples, return wikitable with field
    names as columns.


    Parameters
    ----------
    data: dict
        Dict of (label, namedtuple) pairs
    title: str
        Desired title of table

    Returns
    ----------
    str
    Returns wikitable of provided stats
    """
    headers = list(data.values())[0]._fields
    first_row_content = [""] + list(headers)
    table = "{|class=wikitable"
    table += """
|+{}
|-
""".format(title)
    for label in first_row_content:
        table += "!{}\n".format(label)
    table += "|-\n"
    labeled_stats = sorted(data.items())
    for label, stats in labeled_stats:
        table += "!{}\n".format(label)
        for s in stats:
            table += "|{}\n".format(s)
        table += "|-\n"
    table += "|}"
    return table


def double_tuples_to_table(tuple_list,
                           title="",
                           members_name="users"):
    """Given a list of tuples in format:
    ((y_label, x_label),(member_count, edit_count)),
    where y_label is typically a month or year,
    generate wikitable with member and edit counts,
    percentages and percentage changes in each cell."""
    y_labels = [str(x[0][0]) for x in tuple_list]
    y_labels = sorted(set(y_labels))
    x_labels = [str(x[0][1]) for x in tuple_list]
    x_labels = sorted(set(x_labels))
    tuple_list.sort(key=lambda x: str(x))
    headers = x_labels
    first_row_content = [""] + headers
    table = "{|class=wikitable"
    table += """
|+{}
|-
""".format(title)
    for label in first_row_content:
        table += "!{}\n".format(label)
    y_done = set()
    edit_totals = dict([(x, sum([y[1][1] for y in
                                 tuple_list if y[0][0] == x]))
                        for x in y_labels])
    prev_edits = {}
    for labels, vals in tuple_list:
        members, edits = vals
        y_label, x_label = labels
        if y_label not in y_done:
            table += "|-\n!{}\n".format(y_label)
        y_done.add(y_label)
        cell = "|{} {}\n{} edits\n"
        cell_content = cell.format(members, members_name,
                                   edits)
        total = edit_totals[y_label]
        percent = round(100 * edits / total, 1)
        cell_content += "{}%\n".format(percent)
        if x_label in prev_edits.keys():
            prev = prev_edits[x_label]
            change = round(100 * ((edits - prev) / prev), 2)
            if change > 0:
                change_sign = "+"
            else:
                change_sign = ""
            cell_content += "({}{}% change in edits)\n" \
                .format(change_sign, change)
        prev_edits[x_label] = edits
        cell_content = cell_content.strip()
        cell_content = cell_content.replace("\n", "{{br}}\n")
        cell_content += "\n"
        table += cell_content
    table += "|-\n|}"
    return table


def get_annual_bands(output):
    """Given a list of outputs in format
    [('year',[(band, user_count)...], [(band, edit_count)...]]
    return wikitable of banded amounts and percentages.
    """
    table = """{|class=wikitable
|+Number of registered users who made ''n'' extant mainspace edits in each calendar year
|Year
|1-9 edits
|10-99 edits
|100-999 edits
|1,000-9,999 edits
|10,000+ edits
|Percent of all{{br}}registered {{br}}users who edited
|Percent of all{{br}}edits by {{br}}registered users
|-
"""
    from re import sub
    for year, banded_edits, banded_users in output:
        band2edits = dict(banded_edits)
        table += "|{}\n".format(year)
        edit_percents = {}
        user_percents = {}
        total_edits = sum(band2edits.values())
        total_users = sum([x[1] for x in banded_users])
        banded_users.sort(key=lambda x: str(x[0]))
        for band, users in banded_users:
            edits = band2edits[band]
            text = "{:,}".format(users) + " users {{br}}"
            text += "making {{br}}" + "{:,}".format(edits)
            text += " edits"
            rounder = 1
            edit_percent = round(100 * edits / total_edits, rounder)
            if band is None:  # final and smallest band of users
                rounder = 2
            user_percent = round(100 * users / total_users, rounder)
            if user_percent == 0.0 and users > 0:
                user_percent = "{:.7f}".format(100 * users / total_users)
                user_percent = sub("(0\\.0+[1-9]{1,2}).*", "\\1", user_percent)
            edit_percents[band] = edit_percent
            user_percents[band] = user_percent
            table += "|{}\n".format(text)
        user_max = max([x for x in user_percents.values() if type(x) is float])
        user_values = [user_max] + list(user_percents.values())
        user_chart = """../Dumpster chart
 | data_max   = {}
 | table_width = 15
 | data3  = {}
 | data4  = {}
 | data5  = {}
 | data6  = {}
 | data7  = {}"""
        if len(user_percents) > 5:
            user_chart += "\n | data8  = {}"
        user_chart = "{{" + user_chart.format(*user_values) + "}}"
        table += "|{}\n".format(user_chart)
        edit_max = max(edit_percents.values())
        edit_values = [edit_max] + list(edit_percents.values())
        edit_chart = """../Dumpster chart
 | data_max   = {}
 | table_width = 15
 | data3  = {}
 | data4  = {}
 | data5  = {}
 | data6  = {}
 | data7  = {}"""
        edit_chart = "{{" + edit_chart.format(*edit_values) + "}}"
        table += "|{}\n".format(edit_chart)
        table += "|-\n"
    table += "|}"
    return table


def tabulate_years(uuu):
    """Given a dict of user age distributions by year,
    prepare a table with year of first edit in X and
    years since first edit in Y."""
    table = """{|class=wikitable
|+Number of registered users who made first extant mainspace edit in X who were still editing after Y years.
"""
    years = sorted(uuu.keys())
    for num in range(len(years)):
        table += "! scope= col | {}\n".format(num)
    table += "|-\n"
    for year, ages in uuu.items():
        ages.sort()
        table += "! scope = row | {}\n".format(year)
        for age, count in ages:
            table += "|{}\n".format(count)
        table += "|-\n"
    table += "|}"
    return table
 
