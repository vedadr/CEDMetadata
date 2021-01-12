import os
from pathlib import Path
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
    # set this to true if you want to fix characters in all projects
    fix_all_projects = False

    path = Path(r'C:\Projects\Website-ASP.NET\pub\ReportData\Metadata')  # r'C:\Projects\CEDMetadata')

    if fix_all_projects:
        project_list = [project.name for project in path.iterdir() if project.name.endswith('.xml')]
    else:
        # in case you don't want all projects to be checked, you can create a list of selected projects for fixing
        project_list = {'PC2011.xml','CA2009.xml', 'PC2018.xml',
                        'NOMEN2011.xml', 'ELEC2011.xml',
                        'SSOC2011.xml', 'TCN2011.xml',
                        'nac2011.xml', 'renta2016.xml',
                        'EVR2011.xml', 'DEF2011.xml',
                        'PERE2011.xml', 'SHP2011.xml',
                        'VEH2014.xml', 'mat2011.xml',
                        'CON2011.xml', 'CATAS2012.xml','SSPEN2018.xml'}

    chars_to_replace = {'Ã³': 'ó', 'Ã¡': 'á', 'Ã±': 'ñ', 'Ãº': 'ú', 'Ã©': 'é'}

    for project in project_list:
        file = path / project
        parser = et.XMLParser(strip_cdata=False)
        doc = et.parse(str(file), parser=parser)

        replace_chars(doc, chars_to_replace)

        doc.write(str(file), pretty_print=True)


if __name__ == '__main__':
    main()