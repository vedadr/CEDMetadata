"""
This is used to fix variable formatting between two ACS projects.
It can be useful when working on new ACS project to make sure its
variables has the same properties as the old one.

"""
from lxml import etree as et
from os.path import join


def fix_title(doc, doc_to_fix):
    fix_scope_variables = {
        "Visible": 0,
        "GeoTypeTreeNodeExpanded": 1,
        "GeoCorrespondenceTreeNodeExpanded": 2,
        "DisplayName": 3,
        "Categories": 4
    }

    original_survey = doc.xpath('/survey')

    survey_to_fix = doc_to_fix.xpath('/survey')

    original_survey_year = original_survey[0].attrib['year']

    # this assumes that survey naming is following convention prefix+survey year e.g. EVR2011,PC2018 etc.
    new_survey_year = survey_to_fix[0].attrib['name'][-4:]

    for fix, index in fix_scope_variables.items():
        if fix == 'DisplayName':
            if original_survey_year in original_survey[0].attrib[fix]:
                survey_to_fix[0].attrib[fix] = original_survey[0].attrib[fix].replace(original_survey_year, new_survey_year)
            else:
                survey_to_fix[0].attrib[fix] = original_survey[0].attrib[fix] + ' ' + new_survey_year

    return doc_to_fix


def fix_tables(doc, doc_to_fix):
    fix_scope_tables = {
        "VariablesAreExclusive": 0,
        "notes": 1,
        "PrivateNotes": 2,
        "TableMapInfo": 3,
        "DollarYear": 4,
        "PercentBaseMin": 5,
        "title": 6,
        "titleWrapped": 7,
        "titleShort": 8,
        "universe": 9,
        "Visible": 10,
        "VisibleInMaps": 11,
        "TreeNodeCollapsed": 12,
        "DocSectionLinks": 13,
        "DataCategories": 14,
        "ProductTags": 15,
        "FilterRuleName": 16,
        "CategoryPriorityOrder": 17,
        "PaletteType": 18,
        "PaletteInverse": 19,
        "PaletteName": 20,
        "ShowOnFirstPageOfCategoryListing": 21,
        "DbTableSuffix": 22,
        "source": 23,
        "DefaultColumnCaption": 24,
        "samplingInfo": 25,
    }

    original_tables = doc.xpath('//SurveyDataset[@abbreviation="ORG"]//table')

    form_template = {t.attrib['name'][-3:]: [t.attrib['VariablesAreExclusive'],
                                             t.attrib['notes'],
                                             t.attrib['PrivateNotes'],
                                             t.attrib['TableMapInfo'],
                                             t.attrib['DollarYear'],
                                             t.attrib['PercentBaseMin'],
                                             t.attrib['title'],
                                             t.attrib['titleWrapped'],
                                             t.attrib['titleShort'],
                                             t.attrib['universe'],
                                             t.attrib['Visible'],
                                             t.attrib['VisibleInMaps'],
                                             t.attrib['TreeNodeCollapsed'],
                                             t.attrib['DocSectionLinks'],
                                             t.attrib['DataCategories'],
                                             t.attrib['ProductTags'],
                                             t.attrib['FilterRuleName'],
                                             t.attrib['CategoryPriorityOrder'],
                                             t.attrib['PaletteType'],
                                             t.attrib['PaletteInverse'],
                                             t.attrib['PaletteName'],
                                             t.attrib['ShowOnFirstPageOfCategoryListing'],
                                             t.attrib['DbTableSuffix'],
                                             t.attrib['uniqueTableId'],
                                             t.attrib['source'],
                                             t.attrib['DefaultColumnCaption'],
                                             t.attrib['samplingInfo'],
                                             ] for t in original_tables}

    tables = doc_to_fix.xpath('//SurveyDataset[@abbreviation="ORG"]//table')

    for table in tables:
        try:
            table_values = form_template[table.attrib['name'][-3:]]
        except KeyError:
            continue

        for fix, index in fix_scope_tables.items():
            table.attrib[fix] = table_values[index]
    return doc_to_fix


def fix_variables(doc, doc_to_fix):
    fix_scope_variables = {
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
        'BubbleSizeHint': 10,
        'FR': 11,  # filter rule
        'PN': 12,  # palette
        'PT': 13,  #
        'label': 14,
        'qLabel': 15,
    }

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
                                             v.attrib['PT'],
                                             v.attrib['label'],
                                             v.attrib['qLabel'],
                                             ] for v in original_variables}

    variables = doc_to_fix.xpath('//SurveyDataset[@abbreviation="ORG"]//variable')

    for var in variables:
        try:
            var_values = form_template[var.attrib['name'][-3:]]
        except KeyError:
            continue

        for fix, index in fix_scope_variables.items():
            var.attrib[fix] = var_values[index]

    return doc_to_fix


def main(template_file, file_to_fix):
    """
    Function to run them all
    :param template_file: file to use as a template
    :param file_to_fix: file to be checked and fixed
    :return:
    """

    parser = et.XMLParser(strip_cdata=False)
    doc = et.parse(template_file)
    doc_to_fix = et.parse(file_to_fix, parser=parser)

    doc_to_fix = fix_title(doc, doc_to_fix)
    doc_to_fix = fix_tables(doc, doc_to_fix)
    doc_to_fix = fix_variables(doc, doc_to_fix)

    print('Writing doc ...')
    doc_to_fix.write(file_to_fix, pretty_print=True)


if __name__ == '__main__':

    projects = {'RENTA2015.xml','RENTA2017.xml'}

    project_template = 'RENTA2016.xml'

    working_dir = r'C:\Users\jgarcia\Documents\CEDMetadata'

    for project in projects:
        print(f'Editing {project}')

        main(join(working_dir, project_template), join(working_dir, project))

        print('Done!')
