"""Functions to process and present data that
load.py loads from CSV files crawl.py creates."""

from collections import defaultdict, namedtuple
from datetime import date, timedelta


def make_user_table_by_year(yearbands, title="EN Wikipedians by year "
                            "who made ''n'' edits in their first year"):
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
    if yearbands is None:
        return ""
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


def make_page_table_by_year(yearbands, title="Pages by year"):
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
    Returns a band based on a number (of edits)
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
                " users{{br}}making{{br}}" + str(cell_edit_count) +\
                " edits"
            cell = "|{}\n".format(cell_content)
            table += cell
        table += "|-\n"
    table += "|}"
    return table