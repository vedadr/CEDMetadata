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
    :param settings:
    :param template_file: file to use as a template
    :param file_to_fix: file to be checked and fixed
    :return:
    """
    # what do you want to fix? Note that index is matching indexes in form_template variable.
    # fix_scope = {
    #     'aggMethod': 'agg_method',
    #     'AggregationStr': 'agg_string',
    #     'BubbleSizeHint': 'bubble_size',
    #     'FR': 11,
    #     'PN': 12
    #     }

    """
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
    """
    #
    # fix_scope = {
    #     'label':'PoblaciónTotal',
    #     'qLabel':'PoblaciónTotal',
    #     'PN':'blues',
    #     'FR':'',
    #     'AggregationStr':'Add',
    # }

    parser = et.XMLParser(strip_cdata=False)
    doc_to_fix = et.parse(file_to_fix, parser=parser)
    variables = doc_to_fix.xpath('//SurveyDataset[@abbreviation="ORG"]//variable')
    for var in variables:

        variable_name_from_full = var.attrib['name'][[i for i, c in enumerate(var.attrib['name']) if c == '_'][-2] + 1:]

        var_values = settings[settings['name'].str.lower() == variable_name_from_full.lower()]

        if len(var_values) == 0:
            var_values = settings[settings['name'].str.lower() == var.attrib['name'].lower()]

        for attribute in settings.columns:
            if attribute == 'name':  # we don't want to change the name
                continue
            try:
                var.attrib[attribute]
            except KeyError:
                continue

            if not pd.isna(var_values[attribute].item()):  # change values that actually exists in the sheet
                var.attrib[attribute] = var_values[attribute].item()

    print('Writing doc ...')
    doc_to_fix.write(file_to_fix, pretty_print=True)


def get_palettes():
    with open(r'C:\Projects\dingo-maps\maps\color-palettes\ced-color-palettes.json', 'r') as file:
        palette_definitions = json.load(file)
    return {tag['title']: tag['id'] for tag in palette_definitions}


if __name__ == '__main__':

    # projects = {'CA2009.xml', 'PC2018.xml',
    #             'NOMEN2011.xml', 'ELEC2011.xml',
    #             'SSOC2011.xml', 'TCN2011.xml',
    #             'nac2011.xml', 'renta2016.xml',
    #             'EVR2011.xml', 'DEF2011.xml',
    #             'PERE2011.xml', 'SHP2011.xml',
    #             'VEH2014.xml', 'mat2011.xml',
    #             'CON2011.xml', 'CATAS2012.xml'}

    working_dir = r'C:\Projects\Website-ASP.NET\pub\ReportData\Metadata'

    palettes = get_palettes()
    input_file_variables = pd.read_csv('settings.csv', encoding='latin-1')
    map_type_mapping = {'bubbles': 'Add', 'Shaded': 'Rate|||100000',
                        'Bubbles': 'Add', 'shaded': 'Rate|||100000'}

    column_name_mapping = {'variable': 'name',
                           'name_variable': 'label',
                           'short_name': 'qlabel',
                           'Color palette': 'PN',
                           'Cutpoints': 'FR',
                           'Type of map': 'AggregationStr'}

    input_file_variables.rename(column_name_mapping, axis=1, inplace=True)
    input_file_variables.AggregationStr.replace(map_type_mapping, inplace=True)
    input_file_variables.PN = input_file_variables.PN.str.title().replace(palettes)

    projects = input_file_variables.Project.drop_duplicates()
    for project in projects:
        print(f'Editing {project}')

        main(input_file_variables, join(working_dir, project + '.xml'))

        print('Done!')
