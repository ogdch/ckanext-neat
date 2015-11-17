import logging
import sys
from pprint import pprint
import ckan.lib.cli

class NeatCommand(ckan.lib.cli.CkanCommand):
    '''Command to import NEAT data

    Usage::

            paster --plugin="ckanext-neat" neat show /tmp/neat-files -c <path to config file>
            paster --plugin="ckanext-neat" neat import /tmp/neat-files -c <path to config file>

    '''
    summary = __doc__.split('\n')[0]
    usage = __doc__

    def command(self):
        options = {
            'show': self.showCmd,
            'import': self.importCmd,
            'help': self.helpCmd,
        }

        try:
            cmd = self.args[0]
            options[cmd](*self.args[1:])
        except KeyError:
            self.helpCmd()

    def helpCmd(self):
        print self.__doc__

    def showCmd(self, path=None):
        if (path is None):
            print "Argument 'path' must be set"
            self.helpCmd()
            sys.exit(1)
        print "SHOW"
    
    def importCmd(self, path=None):
        if (path is None):
            print "Argument 'path' must be set"
            self.helpCmd()
            sys.exit(1)
        print "IMPORT"
