

class Message(dict):

    @property
    def id(self):
        return self.get('id')

    @id.setter
    def id(self, value):
        self['id'] = value
