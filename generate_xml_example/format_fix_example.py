"""
This is used to fix variable formatting between two ACS projects.
It can be useful when working on new ACS project to make sure its
variables has the same properties as the old one.

"""
from lxml import etree as et
from os.path import join


def main(template_file, file_to_fix):
    """
    Function to run them all
    :param template_file: file to use as a template
    :param file_to_fix: file to be checked and fixed
    :return:
    """
    # what do you want to fix? Note that index is matching indexes in form_template variable.
    # fix_scope = {
    #     'indent': 0,
    #     'dataType': 1,
    #     'dataTypeLength': 2,
    #     'formatting': 3,
    #     'aggMethod': 4,
    #     'AggregationStr': 5,
    #     'customFormatStr': 6,
    #     'suppType': 7,
    #     'SuppField': 8,
    #     'suppFlags': 9,
    #     'BubbleSizeHint': 10
    # }

    fix_scope = {
        'aggMethod': 4,
        'AggregationStr': 5,
        'BubbleSizeHint': 10,
        'FR': 11,
        'PN': 12
    }

    parser = et.XMLParser(strip_cdata=False)
    doc = et.parse(template_file)
    original_variables = doc.xpath('//SurveyDataset[@abbreviation="ORG"]//variable')
    form_template = {v.attrib['name'][-3:]: [v.attrib['indent'],
                                        v.attrib['dataType'],
                                        v.attrib['dataTypeLength'],
                                        v.attrib['formatting'],
                                        v.attrib['aggMethod'],
                                        v.attrib['AggregationStr'],
                                        v.attrib['customFormatStr'],
                                        v.attrib['suppType'],
                                        v.attrib['SuppField'],
                                        v.attrib['suppFlags'],
                                        v.attrib['BubbleSizeHint'],
                                        v.attrib['FR'],
                                        v.attrib['PN'],
                                        ] for v in original_variables}

    doc_to_fix = et.parse(file_to_fix, parser=parser)
    variables = doc_to_fix.xpath('//SurveyDataset[@abbreviation="ORG"]//variable')
    for var in variables:
        try:
            var_values = form_template[var.attrib['name'][-3:]]
        except KeyError:
            continue

        for fix, index in fix_scope.items():
            var.attrib[fix] = var_values[index]
    print('Writing doc ...')
    doc_to_fix.write(file_to_fix, pretty_print=True)


if __name__ == '__main__':

    projects = {'PC2003.xml', 'PC2004.xml', 'PC2005.xml', 'PC2006.xml', 'PC2006.xml',
                'PC2007.xml', 'PC2008.xml', 'PC2009.xml', 'PC2010.xml', 'PC2011.xml',
                'PC2012.xml', 'PC2013.xml', 'PC2014.xml', 'PC2015.xml', 'PC2016.xml',
                'PC2017.xml'}

    project_template = 'PC2018.xml'

    working_dir = r'C:\Projects\Website-ASP.NET\pub\ReportData\Metadata'

    for project in projects:
        print(f'Editing {project}')

        main(join(working_dir, project_template), join(working_dir, project))

        print('Done!')
