"""Code for parsing impresso's canonical directory structures."""

import os
import logging
from datetime import date, datetime
from smart_open import s3_iter_bucket
from collections import namedtuple
import re
import json

logger = logging.getLogger(__name__)

# a simple data structure to represent input directories
# a `Document.zip` file is expected to be found in `IssueDir.path`
IssueDir = namedtuple(
    "IssueDir", [
        'journal',
        'date',
        'edition',
        'path'
    ]
)

ContentItem = namedtuple(
    "Item", [
        'journal',
        'date',
        'edition',
        'number',
        'path',
        'type'
    ]
)


KNOWN_JOURNALS = [
    "BDC",
    "CDV",
    "DLE",
    "EDA",
    "EXP",
    "IMP",
    "GDL",
    "JDF",
    "JDV",
    "LBP",
    "LCE",
    "LCG",
    "LCR",
    "LCS",
    "LES",
    "LNF",
    "LSE",
    "LSR",
    "LTF",
    "LVE",
    "EVT",
    "JDG",
    "LNQ",
    "NZZ",
    "FedGazDe",
    "FedGazFr",
    "FedGazIt",
    "arbeitgeber",
    "handelsztg",
    "actionfem",
    "armeteufel",
    "avenirgdl",
    "buergerbeamten",
    "courriergdl",
    "deletz1893",
    "demitock",
    "diekwochen",
    "dunioun",
    "gazgrdlux",
    "indeplux",
    "kommmit",
    "landwortbild",
    "lunion",
    "luxembourg1935",
    "luxland",
    "luxwort",
    "luxzeit1844",
    "luxzeit1858",
    "obermosel",
    "onsjongen",
    "schmiede",
    "tageblatt",
    "volkfreu1869",
    "waechtersauer",
    "waeschfra",
    "BLB",
    "BNN",
    "DFS",
    "DVF",
    "EZR",
    "FZG",
    "HRV",
    "LAB",
    "LLE",
    "MGS",
    "NTS",
    "NZG",
    "SGZ",
    "SRT",
    "WHD",
    "ZBT",
    "CON", "DTT",
    "FCT", "GAV",
    "GAZ", "LLS",
    "OIZ", "SAX",
    "SDT", "SMZ",
    "VDR", "VHT"

]


def pair_issue(issue_list1, issue_list2):
    """ Associates pairs of issues originating from original and canonical repositories.

    :param issue_list1: list of IssueDir
    :type issue_list1: array
    :param issue_list2: list of IssueDir
    :type issue_list2: array
    :return: list containing tuples of issue pairs [(issue1, issue2), (...)]
    :rtype: list
    """
    dict1 = {}
    pairs = []
    for i in issue_list1:
        s_i = "-".join([i[0], str(i[1]), i[2]])
        dict1[s_i] = i

    for j in issue_list2:
        s_j = "-".join([j[0], str(j[1]), j[2]])
        if s_j in dict1:
            pairs.append((dict1[s_j], j))
    return pairs


def canonical_path(dir, name=None, extension=None, path_type="file"):
    """Create a canonical dir/file path from an `IssueDir` object.

    :param dir: an object representing a newspaper issue
    :type dir: `IssueDir`
    :param name: the file name (used only if path_type=='file')
    :type name: string
    :param extension: the file extension (used only if path_type=='file')
    :type extension: string
    :param path_type: type of path to build ('dir' | 'file')
    :type path_type: string
    :rtype: string
    """
    sep = "-" if path_type == "file" else "/"
    extension = extension if extension is not None else ""
    if path_type == "file":
        return "{}{}".format(
            sep.join(
                [
                    dir.journal,
                    str(dir.date.year),
                    str(dir.date.month).zfill(2),
                    str(dir.date.day).zfill(2),
                    dir.edition,
                    name
                ]
            ),
            extension)
    else:
        return sep.join(
            [
                dir.journal,
                str(dir.date.year),
                str(dir.date.month).zfill(2),
                str(dir.date.day).zfill(2),
                dir.edition
            ]
        )


def _apply_datefilter(filter_dict, issues, year_only):
    filtered_issues = []

    for newspaper, dates in filter_dict.items():
        # date filter is a range
        if isinstance(dates, str):
            start, end = dates.split("-")
            start = datetime.strptime(start, "%Y/%m/%d").date()
            end = datetime.strptime(end, "%Y/%m/%d").date()

            if year_only:
                filtered_issues += [
                    i
                    for i in issues
                    if i.journal == newspaper and start.year <= i.date.year <= end.year
                ]
            else:
                filtered_issues += [
                    i
                    for i in issues
                    if i.journal == newspaper and start <= i.date <= end
                ]

        # date filter is not a range
        elif isinstance(dates, list):
            if not dates:
                filtered_issues += [
                    i
                    for i in issues
                    if i.journal == newspaper
                ]
            else:
                filter_date = [
                    datetime.strptime(d, "%Y/%m/%d").date().year if year_only else datetime.strptime(d, "%Y/%m/%d").date()
                    for d in dates
                ]

                if year_only:
                    filtered_issues += [
                        i
                        for i in issues
                        if i.journal == newspaper and i.date.year in filter_date
                    ]
                else:
                    filtered_issues += [
                        i
                        for i in issues
                        if i.journal == newspaper and i.date in filter_date
                    ]

    return filtered_issues


def select_issues(config_dict, inp_dir):
    """ Reads a configuration file and select newspapers/issues to consider
    See config.example.md for explanations.

    Usage example:
        if config_file and os.path.isfile(config_file):
            with open(config_file, 'r') as f:
                config = json.load(f)
                issues = select_issues(config, inp_dir)
            else:
                issues = detect_issues(inp_dir)

    :param config_dict: dict of newspaper filter parameters
    :type config_dict: dict
    :param inp_dir: base dit where to get the issues from
    :type inp_dir: str

    """
    # read filters from json configuration (see config.example.json)
    try:
        filter_dict = config_dict.get("newspapers")
        exclude_list = config_dict["exclude_newspapers"]
        year_flag = config_dict["year_only"]
    except KeyError:
        logger.critical(f"The key [newspapers|exclude_newspapers|year_only] is missing in the config file.")
        return
    exclude_flag = False if not exclude_list else True
    logger.debug(f"got filter_dict: {filter_dict}, "
                 f"\nexclude_list: {exclude_list}, "
                 f"\nyear_flag: {year_flag}"
                 f"\nexclude_flag: {exclude_flag}")

    # detect issues to be imported
    if not filter_dict and not exclude_list:  # todo: remove this case? should be detect issue
        logger.debug("No positive nor negative filter definition, all issues in {inp_dir} will be considered.")
        issues = detect_issues(inp_dir)
        return issues
    else:
        filter_newspapers = set(filter_dict.keys()) if not exclude_list else set(exclude_list)
        logger.debug(f"got filter_newspapers: {filter_newspapers}, with exclude flag: {exclude_flag}")
        issues = detect_issues(inp_dir, journal_filter=filter_newspapers, exclude=exclude_flag)

        # apply date filter if not exclusion mode
        filtered_issues = _apply_datefilter(filter_dict, issues, year_only=year_flag) if not exclude_flag else issues
        return filtered_issues


def detect_issues(base_dir, journal_filter=None, exclude=False):
    """Parse a directory structure and detect newspaper issues to be imported.

    NB: invalid directories are skipped, and a warning message is logged.

    :param base_dir: the root of the directory structure
    :type base_dir: basestring
    :param journal_filter: list of newspaper to filter (positive or negative)
    :type journal_filter: set
    :param exclude: whether journal_filter is positive or negative
    :type exclude: boolean
    :rtype: list of `IssueDir` instances
    """
    detected_issues = []
    dir_path, dirs, files = next(os.walk(base_dir))
    # workaround to deal with journal-level folders like: 01_GDL, 02_GDL
    if journal_filter is None:
        journal_dirs = [d for d in dirs if d.split("_")[-1] in KNOWN_JOURNALS]
    else:
        if not exclude:
            filtrd_journals = list(
                set(KNOWN_JOURNALS).intersection(journal_filter)
            )
        else:
            filtrd_journals = list(
                set(KNOWN_JOURNALS).difference(journal_filter)
            )
        journal_dirs = [d for d in dirs if d.split("_")[-1] in filtrd_journals]

    for journal in journal_dirs:
        journal_path = os.path.join(base_dir, journal)
        journal = journal.split("_")[-1] if "_" in journal else journal
        dir_path, year_dirs, files = next(os.walk(journal_path))
        # year_dirs = [d for d in dirs if len(d) == 4]

        for year in year_dirs:
            year_path = os.path.join(journal_path, year)
            dir_path, month_dirs, files = next(os.walk(year_path))

            for month in month_dirs:
                month_path = os.path.join(year_path, month)
                dir_path, day_dirs, files = next(os.walk(month_path))

                for day in day_dirs:
                    day_path = os.path.join(month_path, day)
                    # concerning `edition="a"`: for now, no cases of newspapers
                    # published more than once a day in Olive format (but it
                    # may come later on)
                    try:
                        detected_issue = IssueDir(
                            journal,
                            date(int(year), int(month), int(day)),
                            'a',
                            day_path
                        )
                        logger.debug("Found an issue: {}".format(
                            str(detected_issue))
                        )
                        detected_issues.append(detected_issue)
                    except ValueError:
                        logger.warning(
                            "Path {} is not a valid issue directory".format(
                                day_path
                            )
                        )
    return detected_issues


def detect_canonical_issues(base_dir, newspapers):
    """Parse a directory structure and detect newspaper issues to be imported.

    NB: invalid directories are skipped, and a warning message is logged.

    :param base_dir: the root of the directory structure
    :type base_dir: IssueDir
    :param newspapers: the list of newspapers to consider (acronym blank separated)
    :type newspapers: str
    :return: list of `IssueDir` instances
    :rtype: list
    """
    detected_issues = []
    dir_path, dirs, files = next(os.walk(base_dir))

    # workaround to deal with journal-level folders like: 01_GDL, 02_GDL
    # journal_dirs = [d for d in dirs if d.split("_")[-1] == newspapers]
    journal_dirs = [d for d in dirs if d in [d.split("_")[-1] for d in newspapers]]

    for journal in journal_dirs:
        journal_path = os.path.join(base_dir, journal)
        journal = journal.split("_")[-1] if "_" in journal else journal
        dir_path, year_dirs, files = next(os.walk(journal_path))
        # year_dirs = [d for d in dirs if len(d) == 4]

        for year in year_dirs:
            year_path = os.path.join(journal_path, year)
            dir_path, month_dirs, files = next(os.walk(year_path))

            for month in month_dirs:
                month_path = os.path.join(year_path, month)
                dir_path, day_dirs, files = next(os.walk(month_path))

                for day in day_dirs:
                    day_path = os.path.join(month_path, day)
                    dir_path, edition_dirs, files = next(os.walk(day_path))

                    for edition in edition_dirs:
                        edition_path = os.path.join(day_path, edition)
                        try:
                            detected_issue = IssueDir(
                                journal,
                                date(int(year), int(month), int(day)),
                                edition,
                                edition_path
                            )
                            logger.debug("Found an issue: {}".format(
                                str(detected_issue))
                            )
                            detected_issues.append(detected_issue)
                        except ValueError:
                            logger.warning(
                                "Path {} is not a valid issue directory".format(
                                    edition_path
                                )
                            )
    return detected_issues


def detect_journal_issues(base_dir, newspapers):
    """Parse a directory structure and detect newspaper issues to be imported.

    :param base_dir: the root of the directory structure
    :type base_dir: IssueDir
    :param newspapers: the list of newspapers to consider (acronym blank separated)
    :type newspapers: str
    :return: list of `IssueDir` instances
    :rtype: list
    """
    #newspapers = [journal.split("_")[-1] if "_" in journal else journal for journal in newspapers]

    detected_issues = []
    dir_path, dirs, files = next(os.walk(base_dir))
    journal_dirs = [d for d in dirs if d == newspapers]

    for journal in journal_dirs:
        journal_path = os.path.join(base_dir, journal)
        journal = journal.split("_")[-1] if "_" in journal else journal
        dir_path, year_dirs, files = next(os.walk(journal_path))

        for year in year_dirs:
            if "_" not in year:
                year_path = os.path.join(journal_path, year)
                dir_path, month_dirs, files = next(os.walk(year_path))

                for month in month_dirs:
                    if "_" not in month:
                        month_path = os.path.join(year_path, month)
                        dir_path, day_dirs, files = next(os.walk(month_path))

                        for day in day_dirs:
                            if "_" not in day:
                                day_path = os.path.join(month_path, day)
                                # concerning `edition="a"`: for now, no cases of newspapers
                                # published more than once a day in Olive format (but it
                                # may come later on)
                                detected_issue = IssueDir(
                                    journal,
                                    date(int(year), int(month), int(day)),
                                    'a',
                                    day_path
                                )
                                logger.debug("Found an issue: {}".format(
                                    str(detected_issue))
                                )
                                detected_issues.append(detected_issue)
    return detected_issues


def check_filenaming(file_basename):
    """ Checks whether a filename complies with our naming convention (GDL-1900-01-10-a-p0001)

    :param file_basename: page file (txt or image)
    :type file_basename: str
    """
    page_pattern = re.compile(r"^[A-Z]+-\d{4}-\d{2}-\d{2}-[a-z]-p\d{4}$")
    return page_pattern.match(file_basename)


def get_issueshortpath(issuedir):
    """ Returns short version of issue dir path"""

    path = issuedir.path
    return path[path.index(issuedir.journal):]
