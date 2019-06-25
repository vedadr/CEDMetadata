"""
This is used to fix variable formatting between two ACS projects.
It can be useful when working on new ACS project to make sure its
variables has the same properties as the old one.

"""
from lxml import etree as et


def main(template_file, file_to_fix):
    """
    Function to run them all
    :param template_file: file to use as a template
    :param file_to_fix: file to be checked and fixed
    :return:
    """
    # what do you want to fix? Note that index is matching indexes in form_template variable.
    fix_scope = {
        'indent': 0,
        'dataType': 1,
        'dataTypeLength': 2,
        'formatting': 3,
        'aggMethod': 4,
        'AggregationStr': 5,
        'customFormatStr': 6,
        'suppType': 7,
        'SuppField': 8,
        'suppFlags': 9,
        'BubbleSizeHint': 10
    }

    parser = et.XMLParser(strip_cdata=False)
    doc = et.parse(template_file)
    original_variables = doc.xpath('//SurveyDataset[@abbreviation="SE"]//variable')
    form_template = {v.attrib['name']: [v.attrib['indent'],
                                        v.attrib['dataType'],
                                        v.attrib['dataTypeLength'],
                                        v.attrib['formatting'],
                                        v.attrib['aggMethod'],
                                        v.attrib['AggregationStr'],
                                        v.attrib['customFormatStr'],
                                        v.attrib['suppType'],
                                        v.attrib['SuppField'],
                                        v.attrib['suppFlags'],
                                        v.attrib['BubbleSizeHint']] for v in original_variables}
    doc_to_fix = et.parse(file_to_fix, parser=parser)
    variables = doc_to_fix.xpath('//SurveyDataset[@abbreviation="SE"]//variable')
    for var in variables:
        try:
            var_values = form_template[var.attrib['name']]
        except KeyError:
            continue

        for fix, index in fix_scope.items():
            var.attrib[fix] = var_values[index]
    print('Writing doc ...')
    doc_to_fix.write(file_to_fix, pretty_print=True)


if __name__ == '__main__':

    main(r'CED2011.xml',
         r'CED2012.xml')
