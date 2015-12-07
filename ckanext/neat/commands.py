import logging
import sys
import os
import shutil
import traceback
from lxml import etree
from pprint import pprint
import ckan.lib.cli
from ckan.lib.munge import munge_name, munge_tag
import ckanapi
from ckanapi.errors import CKANAPIError
from ckanext.fulltext.parser.tikaparser import Tika_Wrapper_Singleton


namespaces = {
    'dc': 'http://purl.org/dc/elements/1.1/',
}

class MyLocalCKAN(ckanapi.LocalCKAN):
    def call_action(self, action, data_dict=None, context=None, apikey=None,
            files=None):
        """
        :param action: the action name, e.g. 'package_create'
        :param data_dict: the dict to pass to the action, defaults to {}
        :param context: an override for the context to use for this action,
                        remember to include a 'user' when necessary
        :param apikey: not supported
        :param files: not supported
        """
        if not data_dict:
            data_dict = []
        if context is None:
            context = self.context
        if apikey:
            # FIXME: allow use of apikey to set a user in context?
            raise CKANAPIError("LocalCKAN.call_action does not support "
                "use of apikey parameter, use context['user'] instead")
        if files:
            return self._handle_files(action, data_dict, context, files)

        # copy dicts because actions may modify the dicts they are passed
        return self._get_action(action)(dict(context), dict(data_dict))

    def _handle_files(self, action, data_dict, context, files):
        if action not in ['resource_create', 'resource_update']:
            raise CKANAPIError("LocalCKAN.call_action only supports file uploads for resources.")

        new_data_dict = dict(data_dict)
        if action == 'resource_create':
            if 'url' not in new_data_dict or new_data_dict['url']:
                new_data_dict['url'] = '/tmp-file' # url needs to be set, otherwise there is a ValidationError
            resource = self._get_action(action)(dict(context), new_data_dict)
        else:
            resource = new_data_dict

        from ckan.lib.uploader import ResourceUpload
        resource_upload = ResourceUpload({'id': resource['id']})

        # get first upload, ignore key
        source_file = files.values()[0]
        if not resource_upload.storage_path:
            raise CKANAPIError("No storage configured, unable to upload files")

        directory = resource_upload.get_directory(resource['id'])
        filepath = resource_upload.get_path(resource['id'])
        try:
            os.makedirs(directory)
        except OSError, e:
            ## errno 17 is file already exists
            if e.errno != 17:
                raise

        with open(filepath, 'wb+') as dest:
            shutil.copyfileobj(source_file, dest)

        resource['url'] = ('/dataset/%s/resource/%s/download/%s' 
                           % (resource['package_id'], resource['id'], os.path.basename(source_file.name)))
        resource['url_type'] = 'upload'
        self._get_action('resource_update')(dict(context), resource)
        source_file.close()
        return resource


class NeatCommand(ckan.lib.cli.CkanCommand):
    '''Command to import NEAT data

    Usage::

            paster --plugin="ckanext-neat" neat show /tmp/neat-files -c <path to config file>
            paster --plugin="ckanext-neat" neat import /tmp/neat-files -c <path to config file>

    '''
    summary = __doc__.split('\n')[0]
    usage = __doc__

    def command(self):
        self._load_config()
        self.tika_parser = Tika_Wrapper_Singleton()

        options = {
            'show': self.showCmd,
            'import': self.importCmd,
            'help': self.helpCmd,
        }

        try:
            cmd = self.args[0]
            options[cmd](*self.args[1:])
        except (IndexError, KeyError):
            self.helpCmd()

    def helpCmd(self):
        print self.__doc__

    def _ckan_connect(self):
        return MyLocalCKAN(context={'user': 'admin'})
        # return ckanapi.RemoteCKAN('http://neat.lo',
        #                     apikey='df3163fc-da37-4c8a-a8b7-f1c22bbeda58')

    def showCmd(self, path=None):
        if (path is None):
            print "Argument 'path' must be set"
            self.helpCmd()
            sys.exit(1)
        for root, dirs, files in os.walk(path):
            for dir_name in dirs:
                print "Package Name: %s" % dir_name
                dir_path = os.path.join(path, dir_name)
                for file_name in os.listdir(dir_path):
                    if (os.path.isfile(os.path.join(dir_path, file_name)) and
                        file_name != 'Thumbs.db'):
                        print "Ressource: %s" % file_name
            break
    
    def importCmd(self, path=None):
        self.ckan = self._ckan_connect()
        
        if (path is None):
            print "Argument 'path' must be set"
            self.helpCmd()
            sys.exit(1)
        
        for root, dirs, files in os.walk(path):
            for dir_name in dirs:
                try:
                    dir_path = os.path.join(root, dir_name)
                    print "dir_path: %s" % dir_path
                    for file_name in os.listdir(dir_path):
                        file_path = os.path.join(dir_path, file_name)
                        if not file_path.endswith('.pdf') or not os.path.isfile(file_path):
                            continue

                        base_name = file_name.split('.')[0]
                        meta_xml_path = os.path.join(dir_path, base_name + '.xml')

                        metadata = self._parse_metadata(meta_xml_path)

                        # read fulltext with tika
                        metadata['full_text_search'] = self.tika_parser.parse_with_tika(file_path)
                        print "FULLTEXT: %s" % metadata['full_text_search']

                        # add tags to structure
                        tags = [
                            metadata.get('source', '').replace('#', ' ').replace('-', ' '),
                            metadata.get('contributor'),
                            metadata.get('creator'),
                            metadata.get('publisher'),
                            metadata.get('pdf_image_color_mode'),
                            metadata.get('pdf_image_color_space'),
                            metadata.get('pdf_image_format'),
                            metadata.get('pdf_image_resolution'),
                        ]
                        tags = [munge_tag(tag) for tag in tags if tag and tag is not None]
                        metadata['tags'] = [{'name': tag} for tag in set(tags)]
                        
                        pkg = self._create_or_update_package(base_name, metadata)
                        self._attach_file(pkg['id'], file_name, file_name, file_path, metadata, 'PDF')
                        self._attach_file(pkg['id'], base_name + '.xml', 'Metadata XML', meta_xml_path, format='XML')
                except Exception, e:
                    traceback.print_exc()
            

    def _create_or_update_package(self, base_name, metadata):
        pkg_name = munge_name(base_name.replace('#', '_'))
        extras_list = self._generate_extras(metadata)
        pkg_dict = {
            'name':  pkg_name,
            'title': metadata.get('title', base_name),
            'notes': metadata.get('doc_excerpt', None),
            'tags': metadata.get('tags', None),
            'maintainer': metadata.get('publisher', None),
            'author': metadata.get('creator', None),
            'extras': extras_list,
        }
        pprint(pkg_dict)
        try:
            print "pkg_name: %s" % pkg_name
            pkg = self.ckan.action.package_show(id=pkg_name)
            pkg.update(pkg_dict)
            self.ckan.call_action('package_update', pkg)
        except ckanapi.NotFound:
            pkg = self.ckan.call_action('package_create', pkg_dict)

        return pkg

    def _generate_extras(self, data_dict):
        core_fields = [
            'name',
            'title',
            'tags',
            'maintainer',
            'author',
            'notes',
        ]
        extras_list = []
        for key, value in data_dict.iteritems():
            if key not in core_fields:
                extras_list.append({'key': key, 'value': value})
        return extras_list

    def _attach_file(self, pkg_id, name, title, file_path, metadata={}, format=None):
        if not os.path.isfile(file_path):
            return

        resource_dict = {
            'package_id': pkg_id,
            'name': name,
            'title': name,
            'format': format,
        }

        self.ckan.call_action(
            'resource_create', 
            resource_dict,
            files={'upload': open(file_path)}
        )

    def _parse_metadata(self, xml_path):
        if not os.path.isfile(xml_path):
            return {}

        print "Metadata xml: %s" % xml_path
        try:
            meta_xml = etree.parse(xml_path)
        except etree.XMLSyntaxError, e:
            raise MetadataFormatError('Could not parse XML: %r' % e)

        mapping = {
            'creator': './/dc:creator',
            'contributor': './/dc:contributor',
            'publisher': './/dc:publisher',
            'source': './/dc:source',
            'language': './/dc:language',
            'doc_number': './/docNumber',
            'doc_excerpt': './/docExcerpt',
            'scan_date': './/scanParameters/scanDate',
            'scan_format': './/scanParameters/scanFormat',
            'scan_resolution': './/scanParameters/scanResolution',
            'scan_compression': './/scanParameters/scanCompression',
            'scan_color_mode': './/scanParameters/scanColorMode',
            'scan_color_space': './/scanParameters/scanColorSpace',
            'scan_note': './/scanParameters/scanNote',
            'pdf_date': './/pdfParameters/pdfDate',
            'pdf_image_format': './/pdfParameters/pdfImageFormat',
            'pdf_image_resolution': './/pdfParameters/pdfImageResolution',
            'pdf_image_quality': './/pdfParameters/pdfImageQuality',
            'pdf_image_color_mode': './/pdfParameters/pdfImageColorMode',
            'pdf_image_color_space': './/pdfParameters/pdfImageColorSpace',
            'pdf_nof_pages': './/pdfParameters/pdfNofPages',
            'pdf_file_size': './/pdfParameters/pdfFileSize',
            'pdf_md5_checksum': './/pdfParameters/pdfMd5CheckSum',
            'pdf_pdfa_validation': './/pdfParameters/pdfPDFAValidation',
            'referenced_file': './/referencedFile',
        }
        metadata = {}
        for key, xpath in mapping.iteritems():
            try:
                metadata[key] = meta_xml.xpath(xpath, namespaces=namespaces)[0].text
            except IndexError:
                metadata[key] = None

        return metadata

class MetadataFormatError(Exception):
    pass
