class RebuiltDocument(object):
    """docstring for RebuiltDocument."""

    """
    properties:
    - lang, type, title, text
    """

    def __init__(self):
        super(RebuiltDocument, self).__init__()
        self._title = None
        self._fulltext = None
        self._id = None
        self._text_offsets = {}

    def __repr__(self):
        return "<{} id={}>".format(
            self.__class__.__name__,
            self.id
        )

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    @property
    def lines(self):
        return self._text_offsets["line"]

    @lines.setter
    def lines(self, value):
        self._text_offsets["line"] = value

    @property
    def paragraphs(self):
        return self._text_offsets["paragraphs"]

    @paragraphs.setter
    def paragraphs(self, value):
        self._text_offsets["paragraphs"] = value

    @property
    def fulltext(self):
        return self._fulltext

    @fulltext.setter
    def fulltext(self, value):
        self._fulltext = value

    @property
    def title(self):
        return self._title

    @title.setter
    def title(self, value):
        self._title = value

    @staticmethod
    def from_json(path=None, data=None):
        """Loads an instance of `RebuiltDocument` from a JSON file."""
        assert data is not None or path is not None

        if data is not None:
            doc = RebuiltDocument()
            doc.title = data['t'] if 't' in data else None
            doc.fulltext = data['ft']
            doc.id = data['id']
            doc.lines = data['lb'] if 'lb' in data else None
            doc.paragraphs = data['pb'] if 'pb' in data else None
            return doc
        elif path is not None:
            return

    def to_json(path=None):
        pass
