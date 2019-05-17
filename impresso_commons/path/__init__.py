import re
from impresso_commons.path.path_fs import IssueDir
from datetime import date


def parse_canonical_filename(filename):
    """Parse a canonical page names into its components.

    :param filename: the filename to parse
    :type filename: string
    :return: a tuple

    >>> filename = "GDL-1950-01-02-a-i0002"
    >>> parse_canonical_filename(filename)
    >>> ('GDL', ('1950', '01', '02'), 'a', 'i', 2, '')
    """
    regex = re.compile(
        (
            r'^(?P<np>[A-Za-z0-9]+)-(?P<year>\d{4})'
            r'-(?P<month>\d{2})-(?P<day>\d{2})'
            r'-(?P<ed>[a-z])-(?P<type>[p|i])(?P<pgnb>\d{4})(?P<ext>.*)?$'
        )
    )
    result = re.match(regex, filename)
    newspaper_id = result.group("np")
    date = (result.group("year"), result.group("month"), result.group("day"))
    page_number = int(result.group("pgnb"))
    edition = result.group("ed")
    filetype = result.group("type")
    extension = result.group("ext")
    return (
        newspaper_id,
        date,
        edition,
        filetype,
        page_number,
        extension
    )


def id2IssueDir(id, path):
    """
    TODO: documentation
    """
    newspaper, year, month, day, edition = id.split("-")
    year = int(year)
    month = int(month)
    day = int(day)
    return IssueDir(newspaper, date(year, month, day), edition, path)
