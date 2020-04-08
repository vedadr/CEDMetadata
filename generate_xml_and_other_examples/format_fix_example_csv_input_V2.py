"""
This is used to fix variable formatting between two ACS projects.
It can be useful when working on new ACS project to make sure its
variables has the same properties as the old one.

"""
import json

from lxml import etree as et
from os.path import join
import pandas as pd


def main(settings, file_to_fix):
    """
    Function to run them all
       scope = {'PT':'Sequential',
        'PI':'false',
        'PN':'blues',
        'BracketSourceVarGUID':'',
        'BracketFromVal':'0',
        'BracketToVal':'0',
        'BracketType':'None',
        'FirstInBracketSet':'false',
        'notes':'',
        'PrivateNotes':'',
        'name':'T002_001',
        'label':'PoblaciónTotal',
        'qLabel':'PoblaciónTotal',
        'indent':'0',
        'dataType':'4',
        'dataTypeLength':'0',
        'formatting':'9',
        'customFormatStr':'',
        'suppType':'0',
        'SuppField':'',
        'suppFlags':'',
        'aggMethod':'1',
        'DocLinksAsString':'',
        'FR':'',
        'AggregationStr':'Add',
        'BubbleSizeHint':'2'}

    :param settings:
    :param file_to_fix: file to be checked and fixed
    :return:
    """

    parser = et.XMLParser(strip_cdata=False)
    doc_to_fix = et.parse(file_to_fix, parser=parser)

    survey_id = doc_to_fix.xpath('/survey')[0].attrib['name']
    settings = settings[settings['survey_id'] == survey_id]

    table_settings = dict(zip(settings['table_old_name'], [settings['table_new_name'], dict(zip(settings['old_category_name'], settings['new_category_name']))]))

    for element in doc_to_fix.iter():
        # set survey name
        if element.tag == 'survey':
            element.attrib['DisplayName'] = settings.project_name.iloc[0]
        # set table properties
        elif element.tag == 'table':
            try:
                old_label = element.attrib['title']
                element.attrib['title'] = table_settings[old_label][0]
                element.attrib['titleWrapped'] = element.attrib['title']
            except KeyError:
                pass

            try:
                if table_settings[element.attrib['title']][1]['new_category_name'].isnull():
                    element.attrib['DataCategories'] = table_settings[element.attrib['title']][1][element.attrib['DataCategories']]
                else:
                    element.attrib['DataCategories'] = table_settings[element.attrib['title']][1][element.attrib['DataCategories']]
            except KeyError:
                pass

        # set variable properties
        elif element.tag == 'variable':

            print(element)

    print('Writing doc ...')
    doc_to_fix.write(file_to_fix, pretty_print=True)


def get_palettes():
    with open(r'C:\Projects\dingo-maps\maps\color-palettes\ced-color-palettes.json', 'r') as file:
        palette_definitions = json.load(file)
    return {tag['title']: tag['id'] for tag in palette_definitions}


if __name__ == '__main__':

    projects = {'VEH2014', 'EVR2011', 'SHP2011'}

    # 'CA2009.xml', 'PC2018.xml',
    # 'NOMEN2011.xml', 'ELEC2011.xml',
    # 'SSOC2011.xml', 'TCN2011.xml',
    # 'nac2011.xml', 'renta2016.xml','DEF2011.xml',
    # 'PERE2011.xml',
    # 'mat2011.xml',
    # 'CON2011.xml', 'CATAS2012.xml'}

    working_dir = r'C:\Projects\Website-ASP.NET\pub\ReportData\Metadata'

    palettes = get_palettes()

    input_file_variables = pd.read_csv('settings.csv', encoding='latin-1', dtype={'survey_id': str,
                                                                                   'project_name': str,
                                                                                   'file_name': str,
                                                                                   'table_old_name': str,
                                                                                   'table_new_name': str,
                                                                                   'variable_id': str,
                                                                                   'variable_old_name': str,
                                                                                   'variable_new_name': str,
                                                                                   'indent': str,
                                                                                   'color_palette': str,
                                                                                   'cutpoints': str,
                                                                                   'categories': str,
                                                                                   'type_of_map': str,
                                                                                   'old_category_name': str,
                                                                                   'new_category_name': str,
                                                                                   'aggregation_method': str,
                                                                                   'table_maps_notvisible': str,
                                                                                   'popup_variable': str,
                                                                                   'default_map': str})

    map_type_mapping = {'bubbles': 'Add', 'Shaded': 'Rate|||100000',
                        'Bubbles': 'Add', 'shaded': 'Rate|||100000'}

    # column_naformat_fix_example_csv_input.pyme_mapping = {'variable': 'name',
    #                        'name_variable': 'label',
    #                        'short_name': 'qlabel',
    #                        'Color palette': 'PN',
    #                        'Cutpoints': 'FR',
    #                        'Type of map': 'AggregationStr'}

    # input_file_variables.rename(column_name_mapping, axis=1, inplace=True)
    # input_file_variables.AggregationStr.replace(map_type_mapping, inplace=True)
    # input_file_variables.PN = input_file_variables.PN.str.title().replace(palettes)

    if projects is None:
        projects = input_file_variables.Project.drop_duplicates()

    for project in projects:
        print(f'Editing {project}')

        main(input_file_variables, join(working_dir, project + '.xml'))

        print('Done!')
