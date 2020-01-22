import os

from lxml import etree as et


def replace_chars(doc, chars_to_replace):
    variable_names = doc.xpath('//variable')
    for variable in variable_names:
        for k, v in chars_to_replace.items():
            variable.attrib['label'] = variable.attrib['label'].replace(k, v)
            variable.attrib['qLabel'] = variable.attrib['qLabel'].replace(k, v)

    table_names = doc.xpath('//table')
    for table in table_names:
        for k, v in chars_to_replace.items():
            table.attrib['title'] = table.attrib['title'].replace(k, v)
            table.attrib['titleWrapped'] = table.attrib['titleWrapped'].replace(k, v)


def main():
    project_list = {'PC2018.xml', 'PC2017.xml'}

    path = r'C:\Projects\CEDMetadata'
    chars_to_replace = {'Ã³': 'ó', 'Ã¡': 'á', 'Ã±': 'ñ', 'Ãº': 'ú', 'Ã©': 'é'}

    for project in project_list:
        file = os.path.join(path, project)
        parser = et.XMLParser(strip_cdata=False)
        doc = et.parse(file, parser=parser)

        replace_chars(doc, chars_to_replace)

        doc.write(file, pretty_print=True)


if __name__ == '__main__':
    main()
